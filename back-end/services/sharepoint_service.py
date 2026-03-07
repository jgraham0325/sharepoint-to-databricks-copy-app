from typing import List, Optional

from common.connectors.microsoft_graph import graph_get, graph_get_all_pages
from models.sharepoint import Site, Drive, DriveItem
from models.transfer import FileTransferItem


async def search_sites(token: str, query: str) -> List[Site]:
    """Search SharePoint sites by keyword."""
    from common.logger import get_logger
    import httpx

    logger = get_logger(__name__)

    # Microsoft Graph API /sites endpoint behavior:
    # - /sites (no params) often returns empty list but won't error
    # - /sites?search=query performs server-side search
    # - /sites/getAllSites is the recommended endpoint but may not be available in all tenants
    data = None
    last_error = None
    used_search_endpoint = False  # Track if we used a search-capable endpoint

    # Strategy 1: If query provided, use /sites?search=query (server-side search)
    # If query is empty, skip to strategies that get all sites
    if query:
        try:
            params = {"search": query, "$top": "500"}
            data = await graph_get("/sites", token, params=params)
            logger.info("Retrieved sites from /sites?search=%s endpoint", query)
            used_search_endpoint = True
            if data.get("value") and len(data.get("value", [])) > 0:
                logger.info("Found %d sites from /sites?search=%s", len(data.get("value", [])), query)
            else:
                logger.warning("/sites?search=%s returned empty, trying wildcard", query)
                data = None
        except httpx.HTTPStatusError as e:
            logger.warning("Failed /sites?search=%s (status %d): %s",
                          query, e.response.status_code if e.response else 'unknown', str(e))
            data = None
            last_error = e

    # Strategy 2: When query is empty or search failed, try /sites?search=* (asterisk wildcard to get all sites)
    if data is None or not data.get("value") or len(data.get("value", [])) == 0:
        try:
            params = {"search": "*", "$top": "500"}
            data = await graph_get("/sites", token, params=params)
            logger.info("Retrieved sites from /sites?search=* endpoint")
            used_search_endpoint = False  # Wildcard returns all, we'll filter client-side
            if data.get("value") and len(data.get("value", [])) > 0:
                logger.info("Found %d sites from /sites?search=*", len(data.get("value", [])))
            else:
                logger.warning("/sites?search=* returned empty, trying /sites/getAllSites")
                data = None
        except httpx.HTTPStatusError as e2:
            last_error = e2
            logger.warning("Failed /sites?search=* (status %d): %s",
                          e2.response.status_code if e2.response else 'unknown', str(e2))
            data = None

    # Strategy 3: Try /sites/getAllSites (returns all sites in tenant)
    if data is None or not data.get("value") or len(data.get("value", [])) == 0:
        try:
            params = {"$top": "500"}
            data = await graph_get("/sites/getAllSites", token, params=params)
            logger.info("Retrieved sites from /sites/getAllSites endpoint")
            used_search_endpoint = False  # Returns all sites, we'll filter client-side
            if data.get("value") and len(data.get("value", [])) > 0:
                logger.info("Found %d sites from /sites/getAllSites", len(data.get("value", [])))
            else:
                logger.warning("/sites/getAllSites returned empty, trying /sites/root")
                data = None
        except Exception as e3:
            last_error = e3
            logger.warning("Failed /sites/getAllSites: %s", str(e3))
            data = None

    # Strategy 4: Try /sites/root to get root site + try to get subsites
    if data is None or not data.get("value") or len(data.get("value", [])) == 0:
        try:
            root_data = await graph_get("/sites/root", token)
            logger.info("Retrieved root site from /sites/root endpoint")
            used_search_endpoint = False

            # Get root site
            sites_list = []
            if root_data.get("id"):
                sites_list.append(root_data)
                logger.info("Added root site to results")

                # Try to get subsites of root
                try:
                    root_id = root_data.get("id")
                    logger.info("Attempting to retrieve subsites from /sites/%s/sites", root_id)
                    subsites_data = await graph_get(f"/sites/{root_id}/sites", token, params={"$top": "500"})
                    subsites_value = subsites_data.get("value", [])
                    logger.info("Subsites response: %d sites returned", len(subsites_value))
                    if subsites_value:
                        sites_list.extend(subsites_value)
                        logger.info("Found %d subsites under root", len(subsites_value))
                    else:
                        logger.info("No subsites found under root site")
                except Exception as e_sub:
                    logger.warning("Could not retrieve subsites from /sites/%s/sites: %s", root_id, str(e_sub))

                data = {"value": sites_list}
                logger.info("Using root site + subsites (%d total)", len(sites_list))
            else:
                logger.warning("Root site missing id")
                data = None
        except Exception as e_root:
            logger.warning("Failed /sites/root: %s", str(e_root))
            last_error = e_root
            data = None

    # Strategy 5: Try /me/sites (sites the user follows)
    if data is None or not data.get("value") or len(data.get("value", [])) == 0:
        try:
            params = {"$top": "500"}
            data = await graph_get("/me/sites", token, params=params)
            logger.info("Retrieved sites from /me/sites endpoint")
            used_search_endpoint = False
            if data.get("value") and len(data.get("value", [])) > 0:
                logger.info("Found %d sites from /me/sites", len(data.get("value", [])))
        except httpx.HTTPStatusError as e5:
            logger.warning("Failed /me/sites (status %d): %s",
                          e5.response.status_code if e5.response else 'unknown', str(e5))
            last_error = e5
            data = None

    # If all strategies failed with exceptions (data is None), raise an error
    # If data exists but is empty, that's valid - return empty list
    if data is None:
        error_msg = "Failed to retrieve sites from Microsoft Graph API"
        if last_error:
            error_msg += f": {str(last_error)}"
        raise ValueError(error_msg)

    # Start with sites from the main strategies
    all_raw_sites = list(data.get("value", []))

    # Additionally, try to get Team Sites and add them to results
    # This ensures we get both Communication Sites AND Group-connected Team Sites
    try:
        logger.info("Attempting to retrieve Team Sites via /me/joinedTeams")
        # Note: /me/joinedTeams doesn't support $top parameter
        teams_data = await graph_get("/me/joinedTeams", token)
        teams = teams_data.get("value", [])
        logger.info("Retrieved %d teams", len(teams))

        # Get existing site IDs to avoid duplicates
        existing_ids = {site.get("id") for site in all_raw_sites if site.get("id")}

        # For each team, try to get its associated site
        for team in teams:
            try:
                group_id = team.get("id")
                site_data = await graph_get(f"/groups/{group_id}/sites/root", token)
                site_id = site_data.get("id")
                if site_id and site_id not in existing_ids:
                    all_raw_sites.append(site_data)
                    existing_ids.add(site_id)
                    logger.info("Added Team Site: %s", site_data.get("displayName", ""))
            except Exception as e_team:
                logger.warning("Could not get site for team %s: %s", team.get("displayName", ""), str(e_team))

        logger.info("Total sites after adding Team Sites: %d", len(all_raw_sites))
    except Exception as e_teams:
        logger.warning("Could not retrieve Team Sites: %s", str(e_teams))
        # Try alternative approach: get groups the user is a member of
        try:
            logger.info("Trying alternative approach: /me/memberOf to find groups")
            groups_data = await graph_get("/me/memberOf", token, params={"$top": "100", "$filter": "groupTypes/any(c:c eq 'Unified')"})
            groups = groups_data.get("value", [])
            logger.info("Retrieved %d unified groups (Teams)", len(groups))
            
            existing_ids = {site.get("id") for site in all_raw_sites if site.get("id")}
            
            for group in groups:
                try:
                    group_id = group.get("id")
                    site_data = await graph_get(f"/groups/{group_id}/sites/root", token)
                    site_id = site_data.get("id")
                    if site_id and site_id not in existing_ids:
                        all_raw_sites.append(site_data)
                        existing_ids.add(site_id)
                        logger.info("Added Team Site from group: %s", site_data.get("displayName", ""))
                except Exception as e_group:
                    logger.debug("Could not get site for group %s: %s", group.get("displayName", ""), str(e_group))
        except Exception as e_groups:
            logger.warning("Alternative group approach also failed: %s", str(e_groups))

    # Log the raw response structure for debugging
    logger.info("API response keys: %s", list(data.keys()) if data else "None")
    logger.info("API response has 'value' key: %s", "value" in data if data else False)

    sites = []
    query_lower = query.lower() if query else ""
    # Use all_raw_sites which now includes both regular sites and Team Sites
    raw_sites = all_raw_sites if all_raw_sites else []

    logger.info("Processing %d raw sites from API", len(raw_sites))

    # If we only got a few sites (< 10), return them all without filtering
    # This handles cases where permissions are limited and we can only see root + a few subsites
    apply_filter = query_lower and not used_search_endpoint and len(raw_sites) >= 10

    if len(raw_sites) < 10 and query_lower:
        logger.info("Only %d sites available - returning all without filtering to help user find their site", len(raw_sites))

    for s in raw_sites:
        # Log first site structure for debugging
        if len(sites) == 0:
            logger.info("First site structure: %s", list(s.keys()) if isinstance(s, dict) else type(s))

        # Only filter client-side if:
        # - We have a query
        # - We didn't use a search endpoint (search endpoints already filtered server-side)
        # - We have enough sites (>= 10) that filtering makes sense
        if apply_filter:
            name = s.get("name", "").lower()
            display_name = s.get("displayName", s.get("name", "")).lower()
            if query_lower not in name and query_lower not in display_name:
                continue

        sites.append(
            Site(
                id=s["id"],
                name=s.get("name", ""),
                display_name=s.get("displayName", s.get("name", "")),
                web_url=s.get("webUrl", ""),
            )
        )

    logger.info("Found %d sites%s (from %d total)",
                len(sites),
                f" matching query '{query}'" if apply_filter else "",
                len(raw_sites))
    # Return all matching sites (no limit for client-side filtering)
    return sites


async def list_drives(token: str, site_id: str) -> List[Drive]:
    """List document libraries (drives) for a site."""
    from common.logger import get_logger
    logger = get_logger(__name__)

    data = await graph_get(f"/sites/{site_id}/drives", token)
    drives = []
    for d in data.get("value", []):
        drives.append(
            Drive(
                id=d["id"],
                name=d.get("name", ""),
                drive_type=d.get("driveType", ""),
                web_url=d.get("webUrl", ""),
            )
        )
    logger.info("Found %d drives for site %s", len(drives), site_id)
    return drives


async def list_children(
    token: str, drive_id: str, item_id: Optional[str] = None
) -> List[DriveItem]:
    """List children of a drive root or a specific folder."""
    from common.logger import get_logger
    logger = get_logger(__name__)

    if item_id:
        path = f"/drives/{drive_id}/items/{item_id}/children"
    else:
        path = f"/drives/{drive_id}/root/children"

    logger.info("Listing children from path: %s", path)

    raw_items = await graph_get_all_pages(
        path,
        token,
        params={"$top": "200", "$orderby": "name"},
    )
    logger.info("API returned %d items (all pages)", len(raw_items))

    items = []
    for i in raw_items:
        is_folder = "folder" in i
        item_name = i.get("name", "")
        logger.debug("Processing item: %s (folder: %s)", item_name, is_folder)
        items.append(
            DriveItem(
                id=i["id"],
                name=item_name,
                size=i.get("size", 0),
                is_folder=is_folder,
                web_url=i.get("webUrl", ""),
                mime_type=i.get("file", {}).get("mimeType") if not is_folder else None,
                download_url=i.get("@microsoft.graph.downloadUrl"),
                parent_path=i.get("parentReference", {}).get("path"),
            )
        )

    logger.info("Returning %d items to client", len(items))
    return items


async def list_all_files_in_folder(
    token: str,
    drive_id: str,
    folder_item_id: Optional[str] = None,
) -> List[FileTransferItem]:
    """Recursively list all files under a drive root or a specific folder. Used for agent-driven folder copy."""
    from common.logger import get_logger
    logger = get_logger(__name__)
    items: List[FileTransferItem] = []

    async def _recurse(item_id: Optional[str], prefix: str) -> None:
        children = await list_children(token, drive_id, item_id)
        for c in children:
            if c.is_folder:
                await _recurse(c.id, f"{prefix}{c.name}/" if prefix else f"{c.name}/")
            else:
                items.append(
                    FileTransferItem(
                        drive_id=drive_id,
                        item_id=c.id,
                        name=c.name,
                        size=c.size or 0,
                        relative_path=prefix.rstrip("/"),
                    )
                )

    await _recurse(folder_item_id, "")
    logger.info("Collected %d files under drive %s folder %s", len(items), drive_id, folder_item_id or "root")
    return items

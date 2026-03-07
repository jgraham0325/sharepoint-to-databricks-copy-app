#!/usr/bin/env python3
"""
List your SharePoint sites and their document libraries (drives) with IDs.
Use these IDs with create_sharepoint_sample_files.py (--drive-id, --folder-id).
Uses the same device-code auth as the sample-files script.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent.parent
_env = _repo_root / "back-end" / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

import msal
import httpx

MS_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = [
    "User.Read",
    "Sites.Read.All",
    "Sites.FullControl.All",
    "Files.Read.All",
    "Files.ReadWrite.All",
    "Group.Read.All",
    "Team.ReadBasic.All",
]


def get_token() -> str:
    """Authenticate via device code flow; return access token. Uses PublicClientApplication (device flow is not supported on ConfidentialClientApplication)."""
    client_id = os.environ.get("MS_CLIENT_ID")
    tenant_id = os.environ.get("MS_TENANT_ID")
    if not client_id or not tenant_id:
        print("Missing MS_CLIENT_ID or MS_TENANT_ID. Set in back-end/.env or environment.", file=sys.stderr)
        sys.exit(1)
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.PublicClientApplication(
        client_id=client_id,
        authority=authority,
    )
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Device flow failed: {flow.get('error_description', flow)}")
    print(flow["message"])
    result = app.acquire_token_by_device_flow(flow)
    if "error" in result:
        raise RuntimeError(f"Token acquisition failed: {result.get('error_description', result)}")
    return result["access_token"]


def graph_get(token: str, path: str, params: dict | None = None) -> dict:
    url = f"{MS_GRAPH_BASE}{path}"
    with httpx.Client() as client:
        r = client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params or {},
            timeout=30.0,
        )
        r.raise_for_status()
        return r.json()


def main() -> None:
    print("Authenticating (device code)...")
    token = get_token()
    print()

    # Get sites: try search=*, getAllSites, me/sites, then root+subsites
    sites_payload = None
    for path, params in [
        ("/sites", {"search": "*", "$top": "500"}),
        ("/sites/getAllSites", {"$top": "500"}),
        ("/me/sites", {"$top": "500"}),
    ]:
        try:
            sites_payload = graph_get(token, path, params)
            if sites_payload.get("value"):
                break
        except Exception:
            continue

    if not sites_payload or not sites_payload.get("value"):
        try:
            root = graph_get(token, "/sites/root")
            subsites = graph_get(token, f"/sites/{root['id']}/sites", {"$top": "500"})
            sites_payload = {"value": [root] + subsites.get("value", [])}
        except Exception as e:
            print(f"Could not list sites: {e}", file=sys.stderr)
            sys.exit(1)

    if not sites_payload.get("value"):
        print("No sites found for your account.")
        return

    sites = sites_payload["value"]
    print(f"Found {len(sites)} site(s).\n")
    print("Sites and document libraries (drives)")
    print("=" * 80)

    for site in sites:
        site_id = site.get("id", "")
        name = site.get("displayName") or site.get("name") or "(no name)"
        web_url = site.get("webUrl", "")
        print(f"\nSite: {name}")
        print(f"  URL:     {web_url}")
        print(f"  Site ID: {site_id}")

        try:
            drives_payload = graph_get(token, f"/sites/{site_id}/drives")
            drives = drives_payload.get("value", [])
        except Exception as e:
            print(f"  Drives:  (failed: {e})")
            continue

        if not drives:
            print("  Drives:  (none)")
            continue

        for d in drives:
            drive_id = d.get("id", "")
            drive_name = d.get("name", "")
            print(f"  Drive:   \"{drive_name}\"  →  Drive ID: {drive_id}")

    print()
    print("=" * 80)
    print("Use --drive-id <Drive ID> with create_sharepoint_sample_files.py.")
    print("To target a folder, browse in the app or list children of the drive and use --folder-id <item id>.")


if __name__ == "__main__":
    main()

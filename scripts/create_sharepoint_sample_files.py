#!/usr/bin/env python3
"""
Create sample files in a SharePoint document library (Microsoft Graph).
Uses device-code flow for auth, then creates N files of ~SIZE_MB each via upload sessions.
Chunk size is a multiple of 320 KiB per Graph requirements.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

# Load back-end .env so MS_CLIENT_ID, MS_CLIENT_SECRET, MS_TENANT_ID are set
_repo_root = Path(__file__).resolve().parent.parent
_env = _repo_root / "back-end" / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

import msal
import httpx

# Graph: chunk size must be multiple of 320 KiB (327,680 bytes)
CHUNK_SIZE = 327680  # 320 KiB
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


def resolve_sharepoint_url(token: str, library_url: str) -> tuple[str, str]:
    """
    Resolve a SharePoint document library URL to (drive_id, parent_ref).
    parent_ref is 'root' or a folder item id.
    URL form: https://<hostname>/sites/<siteName>/<libraryName>/<optional/folder/path>
    or         https://<hostname>/teams/<teamName>/<libraryName>/<optional/folder/path>
    """
    parsed = urlparse(library_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Invalid URL: missing scheme or hostname")
    hostname = parsed.hostname or parsed.netloc
    path = unquote(parsed.path).strip("/")
    # Strip SharePoint library view suffix so .../Shared%20Documents/Forms/AllItems.aspx is treated as library root
    for suffix in ("/Forms/AllItems.aspx", "/Forms/AllItems"):
        if path.lower().endswith(suffix.lower()):
            path = path[: -len(suffix)].strip("/")
            break
    segments = [s for s in path.split("/") if s]

    if len(segments) < 2:
        raise ValueError(
            "URL must be a document library path, e.g. .../sites/MySite/Shared%20Documents or .../sites/MySite/Shared%20Documents/MyFolder"
        )

    prefix = segments[0].lower()
    if prefix not in ("sites", "teams"):
        raise ValueError("URL path must start with /sites/ or /teams/")

    site_name = segments[1]
    site_relative = f"/{prefix}/{site_name}"
    drive_name = segments[2] if len(segments) > 2 else None
    folder_segments = segments[3:] if len(segments) > 3 else []
    folder_path = "/".join(folder_segments) if folder_segments else ""

    # GET /sites/{hostname}:/{site_relative}
    site_path_param = f"{hostname}:{site_relative}"
    site = graph_get(token, f"/sites/{site_path_param}")
    site_id = site.get("id")
    if not site_id:
        raise ValueError("Could not resolve site from URL")

    if drive_name:
        drives_data = graph_get(token, f"/sites/{site_id}/drives")
        drives = drives_data.get("value", [])
        drive = None
        name_stripped = drive_name.strip()
        # SharePoint URLs often use "Shared Documents" but Graph may return "Documents" (e.g. Teams sites)
        aliases = {name_stripped}
        if name_stripped == "Shared Documents":
            aliases.add("Documents")
        elif name_stripped == "Documents":
            aliases.add("Shared Documents")
        for d in drives:
            if (d.get("name") or "").strip() in aliases:
                drive = d
                break
        if not drive:
            names = [d.get("name", "") for d in drives]
            raise ValueError(
                f"Document library '{drive_name}' not found on site. Available: {names}"
            )
        drive_id = drive["id"]
    else:
        default_drive = graph_get(token, f"/sites/{site_id}/drive")
        drive_id = default_drive.get("id")
        if not drive_id:
            raise ValueError("Site has no default document library")

    if folder_path:
        item = graph_get(token, f"/drives/{drive_id}/root:/{folder_path}")
        if not item.get("folder"):
            raise ValueError(f"Path '{folder_path}' is not a folder")
        parent_ref = item["id"]
    else:
        parent_ref = "root"

    return drive_id, parent_ref


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


def create_upload_session(
    token: str,
    drive_id: str,
    parent_ref: str,
    file_name: str,
) -> str:
    """Create upload session; return upload_url. parent_ref is 'root' or an item id."""
    if parent_ref == "root":
        path = f"/drives/{drive_id}/root:/{file_name}:/createUploadSession"
    else:
        path = f"/drives/{drive_id}/items/{parent_ref}:/{file_name}:/createUploadSession"
    url = f"{MS_GRAPH_BASE}{path}"
    with httpx.Client() as client:
        r = client.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "item": {
                    "@microsoft.graph.conflictBehavior": "rename",
                    "name": file_name,
                }
            },
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
    upload_url = data.get("uploadUrl")
    if not upload_url:
        raise RuntimeError("No uploadUrl in createUploadSession response")
    return upload_url


def upload_file_chunked(
    upload_url: str,
    total_size: int,
    *,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """
    Upload file content to the session in chunks. Do not send Authorization to uploadUrl.
    total_size must be set; content is generated as random bytes (multiple of chunk_size).
    """
    # Ensure total_size is a multiple of chunk_size so we don't send a final odd-sized chunk
    remainder = total_size % chunk_size
    if remainder:
        total_size = total_size + (chunk_size - remainder)
    start = 0
    # Use a fixed seed per file so we could resume; for simplicity we just generate on the fly
    while start < total_size:
        end = min(start + chunk_size, total_size) - 1
        length = end - start + 1
        chunk = os.urandom(length)
        headers = {
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{total_size}",
        }
        # Do NOT send Authorization to uploadUrl (per Microsoft docs)
        with httpx.Client() as client:
            r = client.put(
                upload_url,
                headers=headers,
                content=chunk,
                timeout=120.0,
            )
        if r.status_code == 201:
            return
        if r.status_code != 202:
            r.raise_for_status()
        start = end + 1


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Create sample files in a SharePoint document library (5K files ~10MB each by default)."
    )
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--url",
        metavar="URL",
        help="SharePoint document library URL (e.g. https://tenant.sharepoint.com/sites/MySite/Shared%%20Documents or .../Shared%%20Documents/MyFolder)",
    )
    target.add_argument(
        "--drive-id",
        metavar="ID",
        help="SharePoint drive (document library) ID (use scripts/list_sharepoint_sites_and_drives.py to find it)",
    )
    ap.add_argument(
        "--folder-id",
        default=None,
        help="Folder item ID under the drive (only with --drive-id); omit to use drive root",
    )
    ap.add_argument("--count", type=int, default=5000, help="Number of files to create (default 5000)")
    ap.add_argument("--size-mb", type=float, default=10.0, help="Target size per file in MB (default 10)")
    ap.add_argument(
        "--prefix",
        default="sample",
        help="Filename prefix (default 'sample'); files named {prefix}_00001.bin, ...",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be done and exit",
    )
    args = ap.parse_args()

    if args.folder_id and args.url:
        ap.error("--folder-id cannot be used with --url; put the folder path in the URL instead")

    drive_id = args.drive_id
    parent_ref = args.folder_id if args.folder_id else "root"
    count = args.count
    size_bytes = int(args.size_mb * 1024 * 1024)
    prefix = args.prefix

    if size_bytes < CHUNK_SIZE:
        size_bytes = CHUNK_SIZE

    # Align to chunk size so last chunk is valid
    remainder = size_bytes % CHUNK_SIZE
    if remainder:
        size_bytes += CHUNK_SIZE - remainder

    need_token = args.url or not args.dry_run
    if need_token:
        print("Authenticating (device code flow)...")
        token = get_token()
    else:
        token = ""

    if args.url:
        print("Resolving document library URL...")
        drive_id, parent_ref = resolve_sharepoint_url(token, args.url)
        print(f"  Drive ID: {drive_id}, target: {'root' if parent_ref == 'root' else 'folder'}")

    if args.dry_run:
        print(f"Would create {count} files of {size_bytes / (1024*1024):.2f} MB each in drive {drive_id}, parent={parent_ref}")
        print(f"Filenames: {prefix}_00001.bin .. {prefix}_{count:05d}.bin")
        return

    print("Creating files...")

    failed = 0
    for i in range(1, count + 1):
        file_name = f"{prefix}_{i:05d}.bin"
        try:
            upload_url = create_upload_session(token, drive_id, parent_ref, file_name)
            upload_file_chunked(upload_url, size_bytes)
        except Exception as e:
            failed += 1
            print(f"  Failed {file_name}: {e}", file=sys.stderr)
            continue
        if i % 100 == 0 or i == count:
            print(f"  {i}/{count}")

    if failed:
        print(f"Done with {failed} failures.", file=sys.stderr)
        sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()

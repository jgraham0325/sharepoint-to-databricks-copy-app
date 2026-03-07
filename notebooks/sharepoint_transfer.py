"""
SharePoint transfer: copy files from Microsoft Graph download URLs to a Unity Catalog volume.
Supports two modes:
  - Manifest: [token, manifest_volume_path] — read manifest JSON from volume, transfer each entry.
  - Legacy:   [token, url1, path1, url2, path2, ...] — token first, then pairs of url and volume path.
Downloads each file to a temp file, then uploads to the volume. Runs sequentially so cluster disk
need only hold one file at a time.
"""
import json
import os
import sys
import tempfile

import requests
from databricks.sdk import WorkspaceClient

if len(sys.argv) < 2:
    raise ValueError("Usage: sharepoint_transfer.py <token> [<manifest_path> | <url1> <path1> [<url2> <path2> ...]")

token = sys.argv[1]
manifest_mode = len(sys.argv) == 3
if manifest_mode:
    manifest_path = sys.argv[2]
    w = WorkspaceClient()
    resp = w.files.download(manifest_path)
    raw = resp.contents
    content = raw.read() if hasattr(raw, "read") else raw
    manifest = json.loads(content.decode("utf-8"))
    pairs = [(e["download_url"], e["volume_path"]) for e in manifest]
else:
    if len(sys.argv) < 4 or (len(sys.argv) - 2) % 2 != 0:
        raise ValueError("Usage: sharepoint_transfer.py <token> <url1> <path1> [<url2> <path2> ...]")
    pairs = [(sys.argv[i], sys.argv[i + 1]) for i in range(2, len(sys.argv), 2)]

# Chunk size for streaming download (bounded memory)
DOWNLOAD_CHUNK_SIZE = 32 * 1024 * 1024  # 32 MB


def transfer_one(download_url: str, volume_path: str) -> None:
    """Download from Graph URL to a temp file, then upload to the volume. Raises on failure."""
    tmp = None
    try:
        with requests.get(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=3600,
        ) as r:
            r.raise_for_status()
            fd, tmp = tempfile.mkstemp(suffix=".tmp", prefix="sharepoint_")
            os.close(fd)
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

        w = WorkspaceClient()
        with open(tmp, "rb") as f:
            w.files.upload(volume_path, f, overwrite=True)
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


errors = []
for url, path in pairs:
    try:
        transfer_one(url, path)
    except Exception as e:
        errors.append(f"{path}: {e}")

if errors:
    for msg in errors:
        print(msg, file=sys.stderr)
    sys.exit(1)
print("OK")

"""
SharePoint transfer: copy files from Microsoft Graph download URLs to a Unity Catalog volume.
Single mode: [scope, tokens_key, manifest_path]. Tokens and app credentials are read from the
Databricks secret scope; no secrets in job parameters.
Manifest entries: {"drive_id", "item_id", "volume_path"}. Job resolves one download URL per file
with retry. Downloads each file to a temp file, then uploads to the volume. Retries on 429/503.
On 401 (expired token), refreshes the access token using refresh_token and MSAL (supports runs >1 hr).
"""
import json
import os
import random
import sys
import tempfile
import time

import requests
from databricks.sdk import WorkspaceClient

if len(sys.argv) != 4:
    raise ValueError("Usage: sharepoint_transfer.py <scope> <tokens_key> <manifest_path>")

scope = sys.argv[1]
tokens_key = sys.argv[2]
manifest_path = sys.argv[3]

# Read tokens from secret scope (no secrets in params)
try:
    tokens_json = dbutils.secrets.get(scope=scope, key=tokens_key)
except Exception as e:
    raise RuntimeError(f"Failed to read tokens from scope={scope} key={tokens_key}: {e}") from e
tokens = json.loads(tokens_json)
current_token = [tokens["access_token"]]
refresh_token = tokens.get("refresh_token")

# Read app credentials from same scope (for MSAL refresh)
ms_client_id = dbutils.secrets.get(scope=scope, key="ms-client-id")
ms_client_secret = dbutils.secrets.get(scope=scope, key="ms-client-secret")
ms_tenant_id = dbutils.secrets.get(scope=scope, key="ms-tenant-id")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Load manifest from volume
w = WorkspaceClient()
resp = w.files.download(manifest_path)
raw = resp.contents
content = raw.read() if hasattr(raw, "read") else raw
manifest = json.loads(content.decode("utf-8"))
entries = []
for e in manifest:
    path = e["volume_path"]
    entries.append((None, path, e["drive_id"], e["item_id"]))

# Chunk size for streaming download (bounded memory)
DOWNLOAD_CHUNK_SIZE = 32 * 1024 * 1024  # 32 MB

# Retry on 429 (throttle) / 503 (server busy): respect Retry-After or exponential backoff
MAX_RETRIES = 8
RETRY_BACKOFF_CAP_SECONDS = 300
INTER_FILE_DELAY_SECONDS = 0.2
REFRESH_TOKEN_BEFORE_SECONDS = 50 * 60  # 50 minutes
_last_refresh_time = [0.0]


def _refresh_access_token():
    """Get a new access token using refresh_token and MSAL. Updates current_token in place."""
    if not refresh_token or not ms_client_id or not ms_client_secret or not ms_tenant_id:
        return False
    try:
        import msal
        authority = f"https://login.microsoftonline.com/{ms_tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id=ms_client_id,
            client_credential=ms_client_secret,
            authority=authority,
        )
        result = app.acquire_token_by_refresh_token(
            refresh_token,
            scopes=["User.Read", "Sites.Read.All", "Sites.FullControl.All", "Files.Read.All", "Group.Read.All", "Team.ReadBasic.All"],
        )
        if "error" in result:
            return False
        current_token[0] = result["access_token"]
        _last_refresh_time[0] = time.time()
        return True
    except Exception:
        return False


def _maybe_refresh_token():
    """Refresh token if we have credentials and it's been long enough or never refreshed."""
    if not refresh_token:
        return
    now = time.time()
    if now - _last_refresh_time[0] >= REFRESH_TOKEN_BEFORE_SECONDS:
        _refresh_access_token()


def _retry_after_seconds(response: requests.Response, attempt: int) -> float:
    raw = response.headers.get("Retry-After")
    if raw:
        try:
            base = min(int(raw), RETRY_BACKOFF_CAP_SECONDS)
        except ValueError:
            base = min(2 ** attempt, RETRY_BACKOFF_CAP_SECONDS)
    else:
        base = min(2 ** attempt, RETRY_BACKOFF_CAP_SECONDS)
    return base * (0.5 + random.random())


def resolve_download_url(drive_id: str, item_id: str) -> str:
    """GET Graph item and return @microsoft.graph.downloadUrl. Retries on 429/503; on 401 refreshes token and retries once."""
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}"
    last_401_refreshed = False
    for attempt in range(MAX_RETRIES):
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {current_token[0]}"},
            timeout=30,
        )
        if r.status_code == 401 and refresh_token and not last_401_refreshed:
            r.close()
            if _refresh_access_token():
                last_401_refreshed = True
                continue
        if r.status_code in (429, 503):
            if attempt == MAX_RETRIES - 1:
                r.raise_for_status()
            delay = _retry_after_seconds(r, attempt)
            r.close()
            time.sleep(delay)
            continue
        r.raise_for_status()
        data = r.json()
        download_url = data.get("@microsoft.graph.downloadUrl")
        if not download_url:
            raise ValueError("No download URL in Graph item response")
        return download_url
    raise RuntimeError("resolve_download_url exhausted retries")


def transfer_one(download_url: str, volume_path: str) -> None:
    """Download from SharePoint URL to a temp file, then upload to the volume. Retries on 429/503; on 401 refreshes token and retries once."""
    tmp = None
    try:
        last_401_refreshed = False
        for attempt in range(MAX_RETRIES):
            r = requests.get(
                download_url,
                headers={"Authorization": f"Bearer {current_token[0]}"},
                stream=True,
                timeout=3600,
            )
            if r.status_code == 401 and refresh_token and not last_401_refreshed:
                r.close()
                if _refresh_access_token():
                    last_401_refreshed = True
                    continue
            if r.status_code in (429, 503):
                if attempt == MAX_RETRIES - 1:
                    r.raise_for_status()
                delay = _retry_after_seconds(r, attempt)
                r.close()
                time.sleep(delay)
                continue
            r.raise_for_status()
            fd, tmp = tempfile.mkstemp(suffix=".tmp", prefix="sharepoint_")
            os.close(fd)
            try:
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
            finally:
                r.close()

            w = WorkspaceClient()
            with open(tmp, "rb") as f:
                w.files.upload(volume_path, f, overwrite=True)
            return
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


errors = []
for i, (url, path, drive_id, item_id) in enumerate(entries):
    try:
        _maybe_refresh_token()
        if url is None:
            url = resolve_download_url(drive_id, item_id)
        transfer_one(url, path)
        if INTER_FILE_DELAY_SECONDS > 0 and i < len(entries) - 1:
            time.sleep(INTER_FILE_DELAY_SECONDS)
    except Exception as e:
        errors.append(f"{path}: {e}")

if errors:
    for msg in errors:
        print(msg, file=sys.stderr)
    sys.exit(1)
print("OK")

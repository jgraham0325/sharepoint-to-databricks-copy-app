from typing import Optional

import msal
import httpx

from common import config
from common.logger import get_logger

logger = get_logger(__name__)


def _build_msal_app() -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        client_id=config.MS_CLIENT_ID,
        client_credential=config.MS_CLIENT_SECRET,
        authority=config.MS_AUTHORITY,
    )


def get_login_url(redirect_uri: str, state: str = "") -> dict:
    """Return the Microsoft authorization URL + state for the OAuth2 code flow."""
    app = _build_msal_app()
    flow = app.initiate_auth_code_flow(
        scopes=["User.Read", "Sites.Read.All", "Sites.FullControl.All", "Files.Read.All", "Group.Read.All", "Team.ReadBasic.All"],
        redirect_uri=redirect_uri,
        state=state,
    )
    return flow


def exchange_code_for_token(flow: dict, auth_response: dict) -> dict:
    """Exchange the authorization code for access + refresh tokens."""
    app = _build_msal_app()
    result = app.acquire_token_by_auth_code_flow(flow, auth_response)
    if "error" in result:
        logger.error("Token exchange failed: %s", result.get("error_description"))
        raise ValueError(result.get("error_description", "Token exchange failed"))
    return result


def refresh_access_token(refresh_token: str) -> dict:
    """Use a refresh token to get a new access token."""
    app = _build_msal_app()
    result = app.acquire_token_by_refresh_token(
        refresh_token,
        scopes=["User.Read", "Sites.Read.All", "Sites.FullControl.All", "Files.Read.All", "Group.Read.All", "Team.ReadBasic.All"],
    )
    if "error" in result:
        logger.error("Token refresh failed: %s", result.get("error_description"))
        raise ValueError(result.get("error_description", "Token refresh failed"))
    return result


async def graph_get(path: str, token: str, params: Optional[dict] = None) -> dict:
    """Perform an authenticated GET against the Microsoft Graph API."""
    url = f"{config.MS_GRAPH_BASE}{path}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30.0,
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            error_detail = f"Microsoft Graph API error: {e.response.status_code}"
            try:
                error_body = e.response.json()
                if "error" in error_body:
                    error_msg = error_body["error"].get("message", "")
                    error_detail = f"{error_detail} - {error_msg}"
            except Exception:
                pass
            logger.error("%s for URL: %s", error_detail, url)
            raise httpx.HTTPStatusError(
                error_detail, request=e.request, response=e.response
            ) from e
        return resp.json()


async def graph_download(url: str, token: str) -> bytes:
    """Download file content from a Graph download URL (loads full response into memory)."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120.0,
        )
        resp.raise_for_status()
        return resp.content


async def graph_download_to_path(
    url: str,
    token: str,
    path: str,
    *,
    chunk_size: int = 8 * 1024 * 1024,
) -> int:
    """Stream download from a Graph download URL to a local file. Returns total bytes written.
    Uses bounded memory (chunk_size buffer) so large files do not exhaust server memory.
    """
    written = 0
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with client.stream(
            "GET",
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=300.0,
        ) as resp:
            resp.raise_for_status()
            with open(path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                    f.write(chunk)
                    written += len(chunk)
    return written

import json
from typing import Dict

from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query, Header
from fastapi.responses import HTMLResponse

from common import config
from common.connectors.microsoft_graph import (
    get_login_url,
    exchange_code_for_token,
    refresh_access_token,
    get_me_id,
    graph_get,
)
from services.job_service import delete_user_tokens
from models.auth import LoginUrlResponse, TokenResponse, RefreshRequest, MeResponse

router = APIRouter(prefix="/auth")

# Temporary in-memory store for OAuth flows keyed by state
_pending_flows: Dict[str, dict] = {}


def _allowed_post_message_origin(origin: Optional[str]) -> bool:
    """Allow localhost (any port) or HTTPS. Used when frontend runs on a different port (e.g. Vite 5173)."""
    if not origin or not isinstance(origin, str):
        return False
    origin = origin.strip().lower()
    if origin.startswith("http://localhost:") or origin.startswith("http://127.0.0.1:"):
        return True
    if origin.startswith("https://"):
        return True
    return False


@router.get("/login", response_model=LoginUrlResponse)
async def login(request: Request, origin: Optional[str] = Query(None)):
    """Return the Microsoft login URL that the frontend should redirect to.
    If the frontend runs on a different origin (e.g. Vite dev on 5173), pass origin=
    so the OAuth callback can postMessage back to that origin."""
    redirect_uri = f"{config.APP_URL}{config.MS_REDIRECT_PATH}"
    flow = get_login_url(redirect_uri=redirect_uri)
    state = flow.get("state", "")
    if origin and _allowed_post_message_origin(origin):
        flow["frontend_origin"] = origin
    _pending_flows[state] = flow
    auth_uri = flow.get("auth_uri", "")
    return LoginUrlResponse(login_url=auth_uri)


def _callback_post_message_origin(state: str) -> str:
    """Origin to use in callback postMessage so the opener (e.g. Vite on 5173) receives the message."""
    flow = _pending_flows.get(state) if state else None
    if flow and isinstance(flow, dict):
        o = flow.get("frontend_origin")
        if o and _allowed_post_message_origin(o):
            return o
    base = (config.APP_URL or "").rstrip("/")
    return base if base.startswith("http") else f"http://{base}"


@router.get("/callback", response_class=HTMLResponse)
async def callback(
    request: Request,
    code: str = Query(None),
    state: str = Query(""),
    error: str = Query(None),
    error_description: str = Query(None),
):
    """Handle the Microsoft OAuth callback.

    Exchanges the code for tokens and renders a small HTML page that posts
    the tokens back to the parent window via postMessage.
    """
    target_origin = json.dumps(_callback_post_message_origin(state))

    # Handle OAuth errors from Microsoft
    if error:
        error_msg = error_description or error
        error_msg_escaped = json.dumps(error_msg)
        html = f"""<!DOCTYPE html>
<html><head><title>Authentication Error</title></head>
<body>
<p style="color: red;">Authentication failed: {error_msg}</p>
<script>
  if (window.opener) {{
    window.opener.postMessage({{ type: "ms-auth-error", error: {error_msg_escaped} }}, {target_origin});
    setTimeout(() => window.close(), 2000);
  }} else {{
    document.body.innerHTML = '<p>Please close this window and try again.</p>';
  }}
</script>
</body></html>"""
        return HTMLResponse(content=html, status_code=400)

    # Check if code is present
    if not code:
        error_msg = "Missing authorization code"
        error_msg_escaped = json.dumps(error_msg)
        html = f"""<!DOCTYPE html>
<html><head><title>Authentication Error</title></head>
<body>
<p style="color: red;">{error_msg}</p>
<script>
  if (window.opener) {{
    window.opener.postMessage({{ type: "ms-auth-error", error: {error_msg_escaped} }}, {target_origin});
    setTimeout(() => window.close(), 2000);
  }} else {{
    document.body.innerHTML = '<p>Please close this window and try again.</p>';
  }}
</script>
</body></html>"""
        return HTMLResponse(content=html, status_code=400)

    flow = _pending_flows.pop(state, None)
    if flow is None:
        error_msg = "Unknown or expired OAuth state. Please try logging in again."
        error_msg_escaped = json.dumps(error_msg)
        html = f"""<!DOCTYPE html>
<html><head><title>Authentication Error</title></head>
<body>
<p style="color: red;">{error_msg}</p>
<script>
  if (window.opener) {{
    window.opener.postMessage({{ type: "ms-auth-error", error: {error_msg_escaped} }}, {target_origin});
    setTimeout(() => window.close(), 2000);
  }} else {{
    document.body.innerHTML = '<p>Please close this window and try again.</p>';
  }}
</script>
</body></html>"""
        return HTMLResponse(content=html, status_code=400)

    try:
        result = exchange_code_for_token(flow, dict(request.query_params))
    except ValueError as exc:
        error_msg = str(exc)
        error_msg_escaped = json.dumps(error_msg)
        html = f"""<!DOCTYPE html>
<html><head><title>Authentication Error</title></head>
<body>
<p style="color: red;">Token exchange failed: {error_msg}</p>
<script>
  if (window.opener) {{
    window.opener.postMessage({{ type: "ms-auth-error", error: {error_msg_escaped} }}, {target_origin});
    setTimeout(() => window.close(), 2000);
  }} else {{
    document.body.innerHTML = '<p>Please close this window and try again.</p>';
  }}
</script>
</body></html>"""
        return HTMLResponse(content=html, status_code=400)

    access_token = result.get("access_token", "")
    refresh_token = result.get("refresh_token", "")
    expires_in = result.get("expires_in", 3600)

    # postMessage target: use frontend origin when provided (e.g. Vite dev on 5173), else callback origin
    frontend_origin = flow.get("frontend_origin") if isinstance(flow, dict) else None
    if not frontend_origin or not _allowed_post_message_origin(frontend_origin):
        frontend_origin = f"{config.APP_URL.rstrip('/')}"
        if not frontend_origin.startswith("http"):
            frontend_origin = "http://" + frontend_origin
    target_origin_escaped = json.dumps(frontend_origin)

    # Properly escape tokens for JavaScript to prevent XSS and syntax errors
    access_token_escaped = json.dumps(access_token)
    refresh_token_escaped = json.dumps(refresh_token)
    
    # Return a small HTML page that sends tokens to the opener via postMessage
    html = f"""<!DOCTYPE html>
<html><head><title>Signing in...</title></head>
<body>
<p>Signing you in&hellip;</p>
<script>
  try {{
    if (window.opener && !window.opener.closed) {{
      window.opener.postMessage({{
        type: "ms-auth-callback",
        access_token: {access_token_escaped},
        refresh_token: {refresh_token_escaped},
        expires_in: {expires_in}
      }}, {target_origin_escaped});
      window.close();
    }} else {{
      document.body.innerHTML = '<p style="color: orange;">The login window was closed. Please close this window and try again.</p>';
    }}
  }} catch (e) {{
    document.body.innerHTML = '<p style="color: red;">Error sending authentication data. Please close this window and try again.</p>';
    console.error('Auth callback error:', e);
  }}
</script>
</body></html>"""
    return HTMLResponse(content=html)


@router.get("/me", response_model=MeResponse)
async def me(x_ms_token: Optional[str] = Header(None, alias="X-MS-Token")):
    """Return the current Microsoft user (display name and UPN) for the given access token."""
    if not x_ms_token:
        raise HTTPException(status_code=401, detail="Missing X-MS-Token header")
    try:
        data = await graph_get(
            "/me?$select=displayName,userPrincipalName",
            x_ms_token,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from e
    return MeResponse(
        display_name=data.get("displayName") or data.get("userPrincipalName") or "Unknown",
        user_principal_name=data.get("userPrincipalName") or "",
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(body: RefreshRequest):
    """Exchange a refresh token for a new access token."""
    try:
        result = refresh_access_token(body.refresh_token)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return TokenResponse(
        access_token=result["access_token"],
        refresh_token=result.get("refresh_token", body.refresh_token),
        expires_in=result.get("expires_in", 3600),
    )


@router.post("/logout")
async def logout(x_ms_token: Optional[str] = Header(None, alias="X-MS-Token")):
    """Remove the current user's tokens from the secret scope (if scope is configured and token is valid). Call on logout to avoid leaving refresh tokens in the scope."""
    if not x_ms_token:
        return {"ok": True}
    try:
        user_oid = await get_me_id(x_ms_token)
        delete_user_tokens(user_oid)
    except Exception:
        pass
    return {"ok": True}

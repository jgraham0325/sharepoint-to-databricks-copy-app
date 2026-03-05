import json
from typing import Dict

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import HTMLResponse

from common import config
from common.connectors.microsoft_graph import (
    get_login_url,
    exchange_code_for_token,
    refresh_access_token,
)
from models.auth import LoginUrlResponse, TokenResponse, RefreshRequest

router = APIRouter(prefix="/auth")

# Temporary in-memory store for OAuth flows keyed by state
_pending_flows: Dict[str, dict] = {}


@router.get("/login", response_model=LoginUrlResponse)
async def login(request: Request):
    """Return the Microsoft login URL that the frontend should redirect to."""
    redirect_uri = f"{config.APP_URL}{config.MS_REDIRECT_PATH}"
    flow = get_login_url(redirect_uri=redirect_uri)
    state = flow.get("state", "")
    _pending_flows[state] = flow
    auth_uri = flow.get("auth_uri", "")
    return LoginUrlResponse(login_url=auth_uri)


@router.get("/callback", response_class=HTMLResponse)
async def callback(
    request: Request, 
    code: str = Query(None), 
    state: str = Query(""),
    error: str = Query(None),
    error_description: str = Query(None)
):
    """Handle the Microsoft OAuth callback.

    Exchanges the code for tokens and renders a small HTML page that posts
    the tokens back to the parent window via postMessage.
    """
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
    window.opener.postMessage({{
      type: "ms-auth-error",
      error: {error_msg_escaped}
    }}, window.location.origin);
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
    window.opener.postMessage({{
      type: "ms-auth-error",
      error: {error_msg_escaped}
    }}, window.location.origin);
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
    window.opener.postMessage({{
      type: "ms-auth-error",
      error: {error_msg_escaped}
    }}, window.location.origin);
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
    window.opener.postMessage({{
      type: "ms-auth-error",
      error: {error_msg_escaped}
    }}, window.location.origin);
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
      }}, window.location.origin);
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

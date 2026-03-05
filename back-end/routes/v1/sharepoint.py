from typing import List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Header

from models.sharepoint import Site, Drive, DriveItem
from services.sharepoint_service import search_sites, list_drives, list_children

router = APIRouter(prefix="/sharepoint")


def _require_token(x_ms_token: Optional[str]) -> str:
    if not x_ms_token:
        raise HTTPException(status_code=401, detail="Missing X-MS-Token header")
    return x_ms_token


@router.get("/sites", response_model=List[Site])
async def sites(query: str = "", x_ms_token: Optional[str] = Header(None)):
    token = _require_token(x_ms_token)
    try:
        return await search_sites(token, query)
    except httpx.HTTPStatusError as e:
        # Convert Microsoft Graph API errors to HTTP exceptions
        status_code = e.response.status_code if e.response else 500
        error_detail = f"Microsoft Graph API error: {status_code}"
        try:
            error_body = e.response.json()
            if "error" in error_body:
                error_msg = error_body["error"].get("message", "")
                error_detail = f"{error_detail} - {error_msg}"
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=error_detail) from e
    except Exception as e:
        # Catch any other unexpected errors
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


@router.get("/sites/{site_id}/drives", response_model=List[Drive])
async def drives(site_id: str, x_ms_token: Optional[str] = Header(None)):
    token = _require_token(x_ms_token)
    return await list_drives(token, site_id)


@router.get("/drives/{drive_id}/children", response_model=List[DriveItem])
async def children(
    drive_id: str,
    item_id: Optional[str] = None,
    x_ms_token: Optional[str] = Header(None),
):
    token = _require_token(x_ms_token)
    return await list_children(token, drive_id, item_id)

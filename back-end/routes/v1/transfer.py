from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Header

from models.transfer import TransferRequest, TransferState, FolderTransferRequest
from services.transfer_service import (
    start_transfer,
    get_transfer,
    start_folder_transfer,
    expand_folders_to_files,
)

router = APIRouter(prefix="/transfer")


def _require_token(x_ms_token: Optional[str]) -> str:
    if not x_ms_token:
        raise HTTPException(status_code=401, detail="Missing X-MS-Token header")
    return x_ms_token


@router.post("/start", response_model=TransferState)
async def start(
    body: TransferRequest,
    x_ms_token: Optional[str] = Header(None),
    x_ms_refresh_token: Optional[str] = Header(None),
):
    token = _require_token(x_ms_token)
    files = list(body.files)
    if body.folders:
        folder_files = await expand_folders_to_files(token, body.folders)
        files.extend(folder_files)
    state = await start_transfer(
        files=files,
        catalog=body.catalog,
        schema_name=body.schema_name,
        volume=body.volume,
        subfolder=body.subfolder,
        ms_token=token,
        ms_refresh_token=x_ms_refresh_token,
    )
    return state


@router.post("/copy-folder", response_model=TransferState)
async def copy_folder(
    body: FolderTransferRequest,
    x_ms_token: Optional[str] = Header(None),
    x_ms_refresh_token: Optional[str] = Header(None),
):
    """
    Copy an entire SharePoint folder (recursively) to a Unity Catalog volume.
    Agent-friendly: provide site drive + optional folder ID and destination volume; all files are copied automatically.
    Use GET /api/v1/sharepoint/sites to find sites, GET .../sites/{id}/drives for drive_id,
    GET .../drives/{id}/children?item_id=... for folder_id. Omit folder_item_id to copy from drive root.
    """
    token = _require_token(x_ms_token)
    try:
        return await start_folder_transfer(
            drive_id=body.drive_id,
            folder_item_id=body.folder_item_id,
            catalog=body.catalog,
            schema_name=body.schema_name,
            volume=body.volume,
            subfolder=body.subfolder,
            ms_token=token,
            ms_refresh_token=x_ms_refresh_token,
        )
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code if e.response else 502
        detail = f"Microsoft Graph API error: {status_code}"
        try:
            if e.response:
                err = e.response.json()
                if "error" in err:
                    detail = f"{detail} - {err['error'].get('message', '')}"
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=detail) from e


@router.get("/status/{transfer_id}", response_model=TransferState)
async def status(transfer_id: str):
    state = get_transfer(transfer_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Transfer not found")
    return state

from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Header

from models.transfer import TransferRequest, TransferState, TransferSummary, FolderTransferRequest
from services.transfer_service import (
    start_transfer,
    get_transfer,
    get_transfer_by_run_id,
    list_transfers,
    start_folder_transfer,
    expand_folders_to_files,
    build_batches_for_response,
)

router = APIRouter(prefix="/transfer")


def _require_token(x_ms_token: Optional[str]) -> str:
    if not x_ms_token:
        raise HTTPException(status_code=401, detail="Missing X-MS-Token header")
    return x_ms_token


def _state_for_response(state: TransferState) -> TransferState:
    """
    Response shape for single-run For Each: iteration-centric.
    - Populate batches (status + file list per batch); omit run_id_to_file.
    - For single run, job_run_statuses entry keeps run_id/url/status but drops file_names (use batches).
    """
    if not state.run_ids or len(state.run_ids) != 1:
        return state
    batches = build_batches_for_response(state)
    updates = {"run_id_to_file": None}
    if batches is not None:
        updates["batches"] = batches
        if state.job_run_statuses and len(state.job_run_statuses) >= 1:
            js = state.job_run_statuses[0]
            updates["job_run_statuses"] = [
                js.model_copy(update={"file_names": []}),  # file list lives in batches
            ]
    return state.model_copy(update=updates)


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
    return _state_for_response(state)


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
        state = await start_folder_transfer(
            drive_id=body.drive_id,
            folder_item_id=body.folder_item_id,
            catalog=body.catalog,
            schema_name=body.schema_name,
            volume=body.volume,
            subfolder=body.subfolder,
            ms_token=token,
            ms_refresh_token=x_ms_refresh_token,
        )
        return _state_for_response(state)
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


@router.get("/status/{id}", response_model=TransferState)
async def status(id: str):
    """
    Get transfer status by transfer_id or run_id.
    If id is numeric, treat as run_id (Databricks job run); otherwise treat as transfer_id.
    """
    if id.isdigit():
        run_id = int(id)
        state = get_transfer_by_run_id(run_id)
    else:
        state = get_transfer(id)
    if state is None:
        raise HTTPException(status_code=404, detail="Transfer not found")
    return _state_for_response(state)


@router.get("", response_model=list[TransferSummary])
async def list_transfers_route():
    """List all transfers (high-level summary), most recent first."""
    return list_transfers()

from typing import Dict, List, Optional
from urllib.parse import quote

import asyncio
import os
import re
import uuid

from common.connectors.microsoft_graph import graph_get, graph_download
from common.connectors.workspace import upload_to_volume
from common.authentication.workspace import get_workspace_client
from common.logger import get_logger
from models.transfer import (
    FileTransferItem,
    FolderTransferItem,
    TransferState,
    TransferStatus,
    FileResult,
)
from services.sharepoint_service import list_all_files_in_folder

logger = get_logger(__name__)

# In-memory transfer tracking (single-instance app)
_transfers: Dict[str, TransferState] = {}


def get_transfer(transfer_id: str) -> Optional[TransferState]:
    return _transfers.get(transfer_id)


def _catalog_explorer_url(
    catalog: str,
    schema_name: str,
    volume: str,
    subfolder: str = "",
) -> Optional[str]:
    """Build a link to open Catalog Explorer at the destination volume (and subfolder).
    Uses: /explore/data/volumes/{catalog}/{schema}/{volume}?o={id}&volumePath={encoded_path}
    """
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if not host or not catalog or not schema_name or not volume:
        return None
    path = f"/explore/data/volumes/{catalog}/{schema_name}/{volume}"
    o_match = re.search(r"adb-(\d+)\.", host)
    params: List[str] = []
    if o_match:
        params.append(f"o={o_match.group(1)}")
    # Deep-link to subfolder inside the volume (e.g. /Volumes/catalog/schema/volume/test44/)
    if subfolder:
        volume_path = f"/Volumes/{catalog}/{schema_name}/{volume}/{subfolder.strip('/')}/"
        params.append(f"volumePath={quote(volume_path, safe='')}")
    query = "?" + "&".join(params) if params else ""
    return f"{host}{path}{query}"


async def expand_folders_to_files(
    ms_token: str, folders: List[FolderTransferItem]
) -> List[FileTransferItem]:
    """Expand each folder into a flat list of files with relative_path = folder_name/... to preserve hierarchy."""
    all_files: List[FileTransferItem] = []
    for folder in folders:
        files = await list_all_files_in_folder(
            ms_token, folder.drive_id, folder.folder_item_id
        )
        for f in files:
            rel = (f.relative_path or "").strip()
            new_rel = (
                f"{folder.folder_name}/{rel}".strip("/")
                if rel
                else folder.folder_name
            )
            all_files.append(
                FileTransferItem(
                    drive_id=f.drive_id,
                    item_id=f.item_id,
                    name=f.name,
                    size=f.size,
                    relative_path=new_rel,
                )
            )
    return all_files


async def start_transfer(
    files: List[FileTransferItem],
    catalog: str,
    schema_name: str,
    volume: str,
    subfolder: str,
    ms_token: str,
) -> TransferState:
    """Kick off an async transfer of files from SharePoint to a Volume."""
    transfer_id = uuid.uuid4().hex[:12]
    state = TransferState(
        transfer_id=transfer_id,
        status=TransferStatus.IN_PROGRESS,
        total=len(files),
        completed=0,
        failed=0,
        results=[],
        catalog=catalog,
        schema_name=schema_name,
        volume=volume,
        catalog_explorer_url=_catalog_explorer_url(catalog, schema_name, volume, subfolder),
    )
    _transfers[transfer_id] = state

    # Run the actual transfers in background
    asyncio.create_task(_execute_transfer(state, files, catalog, schema_name, volume, subfolder, ms_token))
    return state


async def _execute_transfer(
    state: TransferState,
    files: List[FileTransferItem],
    catalog: str,
    schema_name: str,
    volume: str,
    subfolder: str,
    ms_token: str,
) -> None:
    ws_client = get_workspace_client()

    for f in files:
        try:
            # Get the download URL for the item
            item_data = await graph_get(
                f"/drives/{f.drive_id}/items/{f.item_id}",
                ms_token,
            )
            download_url = item_data.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise ValueError("No download URL available for this item")

            # Download the file content
            content = await graph_download(download_url, ms_token)

            # Build the target path (preserve folder hierarchy when relative_path is set)
            path_parts = [p for p in (subfolder, getattr(f, "relative_path", None) or "", f.name) if p]
            target_path = "/".join(path_parts).strip("/") or f.name

            # Upload to Volume
            upload_to_volume(ws_client, catalog, schema_name, volume, target_path, content)

            state.completed += 1
            state.results.append(FileResult(name=f.name, status=TransferStatus.COMPLETED))
            logger.info("Transferred %s (%d bytes)", f.name, len(content))

        except Exception as exc:
            state.failed += 1
            state.results.append(
                FileResult(name=f.name, status=TransferStatus.FAILED, error=str(exc))
            )
            logger.error("Failed to transfer %s: %s", f.name, exc)

    state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED


async def start_folder_transfer(
    drive_id: str,
    folder_item_id: Optional[str],
    catalog: str,
    schema_name: str,
    volume: str,
    subfolder: str,
    ms_token: str,
) -> TransferState:
    """Enumerate all files under a SharePoint folder (recursively) and start transfer to a volume. Agent-friendly."""
    files = await list_all_files_in_folder(ms_token, drive_id, folder_item_id)
    if not files:
        transfer_id = uuid.uuid4().hex[:12]
        state = TransferState(
            transfer_id=transfer_id,
            status=TransferStatus.COMPLETED,
            total=0,
            completed=0,
            failed=0,
            results=[],
            catalog=catalog,
            schema_name=schema_name,
            volume=volume,
            catalog_explorer_url=_catalog_explorer_url(catalog, schema_name, volume, subfolder),
        )
        _transfers[transfer_id] = state
        logger.info("Folder copy: no files found under drive=%s folder=%s", drive_id, folder_item_id)
        return state
    return await start_transfer(
        files=files,
        catalog=catalog,
        schema_name=schema_name,
        volume=volume,
        subfolder=subfolder,
        ms_token=ms_token,
    )

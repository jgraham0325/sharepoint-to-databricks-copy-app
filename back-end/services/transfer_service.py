from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import asyncio
import os
import re
import tempfile
import time
import uuid

from common import config
from common.connectors.microsoft_graph import graph_get, graph_download, graph_download_to_path
from common.connectors.workspace import upload_to_volume, upload_to_volume_from_file
from common.authentication.workspace import get_workspace_client
from common.logger import get_logger
from models.transfer import (
    FileTransferItem,
    FolderTransferItem,
    TransferState,
    TransferStatus,
    FileResult,
    JobRunStatus,
)
from services.sharepoint_service import list_all_files_in_folder
from services.job_service import submit_transfer_batch, get_run_statuses, get_transfer_job_id, TRANSFER_BATCH_SIZE

logger = get_logger(__name__)

# In-memory transfer tracking (single-instance app)
_transfers: Dict[str, TransferState] = {}


def get_transfer(transfer_id: str) -> Optional[TransferState]:
    state = _transfers.get(transfer_id)
    if state and state.run_ids:
        _sync_state_from_job_runs(state)
    return state


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


def _full_volume_path(catalog: str, schema_name: str, volume: str, file_path: str) -> str:
    """Build full Unity Catalog volume path for a file."""
    return f"/Volumes/{catalog}/{schema_name}/{volume}/{file_path}"


def _sync_state_from_job_runs(state: TransferState) -> None:
    """
    Poll job run statuses and incrementally update state: merge outcomes for each run as it
    terminates, update job_run_statuses for UI, and set final status when all runs are done.
    """
    if not state.run_ids or not state.run_id_to_file:
        return
    run_id_to_file = state.run_id_to_file
    statuses = get_run_statuses(state.run_ids)
    # Build map run_id -> (life_cycle, result_state, state_message)
    status_by_run = {run_id: (lc, res, msg) for run_id, lc, res, msg in statuses}
    # Ensure job_run_statuses list exists and is indexed by current run_ids order
    if state.job_run_statuses is None:
        state.job_run_statuses = []
    job_status_by_run_id = {s.run_id: s for s in state.job_run_statuses}
    still_pending: List[int] = []
    run_id_to_file_next: Dict[str, List[str]] = {}

    for run_id in state.run_ids:
        lc, result_state, state_message = status_by_run.get(
            run_id, ("UNKNOWN", "", "")
        )
        names = run_id_to_file.get(str(run_id), [])
        if isinstance(names, str):
            names = [names]
        # Update job_run_statuses for this run (for UI)
        js = job_status_by_run_id.get(run_id)
        if js is None:
            url = None
            if state.job_run_urls and state.run_ids:
                idx = state.run_ids.index(run_id)
                if idx < len(state.job_run_urls):
                    url = state.job_run_urls[idx]
            js = JobRunStatus(run_id=run_id, url=url, status="running", file_names=list(names))
            state.job_run_statuses.append(js)
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            js.status = "success" if result_state == "SUCCESS" else "failed"
            js.error = None if result_state == "SUCCESS" else (state_message or "Job task failed")
            # Merge this run's outcomes into results and remove from pending
            for name in names:
                if result_state == "SUCCESS":
                    state.completed += 1
                    state.results.append(FileResult(name=name, status=TransferStatus.COMPLETED))
                else:
                    state.failed += 1
                    state.results.append(
                        FileResult(name=name, status=TransferStatus.FAILED, error=state_message or "Job task failed")
                    )
        else:
            js.status = "running"
            still_pending.append(run_id)
            run_id_to_file_next[str(run_id)] = names

    state.run_ids = still_pending if still_pending else None
    state.run_id_to_file = run_id_to_file_next if run_id_to_file_next else None
    if not state.run_ids:
        state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)
        logger.info(
            "Transfer %s job runs finished; total completed=%d failed=%d (%.2fs)",
            state.transfer_id, state.completed, state.failed, state.duration_seconds or 0,
        )


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
        started_at=time.time(),
    )
    _transfers[transfer_id] = state

    # Run the actual transfers in background
    asyncio.create_task(_execute_transfer(state, files, catalog, schema_name, volume, subfolder, ms_token))
    return state


def _target_path(subfolder: str, f: FileTransferItem) -> str:
    path_parts = [p for p in (subfolder, getattr(f, "relative_path", None) or "", f.name) if p]
    return "/".join(path_parts).strip("/") or f.name


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
    threshold = config.LARGE_FILE_THRESHOLD_BYTES

    # Resolve download URL and target path for each file
    items: List[tuple] = []
    for f in files:
        try:
            item_data = await graph_get(
                f"/drives/{f.drive_id}/items/{f.item_id}",
                ms_token,
            )
            download_url = item_data.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise ValueError("No download URL available for this item")
            target_path = _target_path(subfolder, f)
            items.append((f, download_url, target_path))
        except Exception as exc:
            state.failed += 1
            state.results.append(
                FileResult(name=f.name, status=TransferStatus.FAILED, error=str(exc))
            )
            logger.error("Failed to get URL for %s: %s", f.name, exc)

    if not items:
        state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)
        return

    full_volume_base = f"/Volumes/{catalog}/{schema_name}/{volume}"
    run_ids: List[int] = []
    run_id_to_file: Dict[str, List[str]] = {}

    # Route per file: small files on this server, large/unknown-size files to Databricks job
    small_items = [(f, download_url, target_path) for f, download_url, target_path in items if 0 < f.size < threshold]
    large_items = [(f, download_url, target_path) for f, download_url, target_path in items if f.size >= threshold or f.size <= 0]

    # Fail fast if we have large files but the Databricks transfer job is not available
    job_not_found_msg = (
        "Databricks job 'sharepoint-transfer' not found. Deploy the bundle (databricks bundle deploy) "
        "or set SHAREPOINT_TRANSFER_JOB_ID in the app environment."
    )
    if large_items:
        job_id = get_transfer_job_id()
        if not job_id:
            logger.warning("Large files present but %s", job_not_found_msg)
            for f, _, _ in large_items:
                state.failed += 1
                state.results.append(
                    FileResult(name=f.name, status=TransferStatus.FAILED, error=job_not_found_msg)
                )
            large_items = []

    # Batch large files; one job run per batch. Submit all batches in parallel when there is more than one.
    num_batches = (len(large_items) + TRANSFER_BATCH_SIZE - 1) // TRANSFER_BATCH_SIZE
    if large_items:
        logger.info("Submitting %d large file(s) in %d batch(es) to Databricks job", len(large_items), num_batches)

    async def submit_batch(i: int) -> Tuple[Optional[int], List[str]]:
        batch = large_items[i : i + TRANSFER_BATCH_SIZE]
        url_path_pairs = [(url, f"{full_volume_base}/{path}") for _, url, path in batch]
        file_names = [f.name for f, _, _ in batch]
        run_id = await asyncio.to_thread(
            submit_transfer_batch,
            ms_token,
            url_path_pairs,
            f"batch {i // TRANSFER_BATCH_SIZE}",
        )
        return (run_id, file_names)

    if num_batches > 1:
        batch_results = await asyncio.gather(
            *[submit_batch(i) for i in range(0, len(large_items), TRANSFER_BATCH_SIZE)]
        )
    else:
        batch_results = [await submit_batch(0)] if large_items else []

    for run_id, file_names in batch_results:
        if run_id is not None:
            run_ids.append(run_id)
            run_id_to_file[str(run_id)] = file_names
        else:
            for name in file_names:
                state.failed += 1
                state.results.append(
                    FileResult(name=name, status=TransferStatus.FAILED, error=job_not_found_msg)
                )

    if run_ids:
        state.run_ids = run_ids
        state.run_id_to_file = run_id_to_file
        # Build workspace job run URLs and per-run status for the UI
        host = (config.DATABRICKS_HOST or "").rstrip("/")
        job_id = get_transfer_job_id()
        if host and job_id:
            state.job_run_urls = [f"{host}#job/{job_id}/run/{rid}" for rid in run_ids]
        else:
            state.job_run_urls = None
        state.job_run_statuses = [
            JobRunStatus(
                run_id=rid,
                url=state.job_run_urls[i] if state.job_run_urls and i < len(state.job_run_urls) else None,
                status="running",
                file_names=run_id_to_file.get(str(rid), []),
            )
            for i, rid in enumerate(run_ids)
        ]

    # Handle small files on the Python server: stream to temp then upload (bounded memory).
    # Run blocking upload in a thread so the event loop stays responsive for status polls.
    for f, download_url, target_path in small_items:
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(suffix=".tmp", prefix="sharepoint_")
            os.close(fd)
            await graph_download_to_path(download_url, ms_token, tmp)
            await asyncio.to_thread(
                upload_to_volume_from_file,
                ws_client,
                catalog,
                schema_name,
                volume,
                target_path,
                tmp,
            )
            state.completed += 1
            state.results.append(FileResult(name=f.name, status=TransferStatus.COMPLETED))
            logger.info("Transferred %s (server)", f.name)
        except Exception as exc:
            state.failed += 1
            state.results.append(
                FileResult(name=f.name, status=TransferStatus.FAILED, error=str(exc))
            )
            logger.error("Failed to transfer %s: %s", f.name, exc)
        finally:
            if tmp and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    # If no job runs are pending, we can set final status now; otherwise get_transfer() will sync when jobs finish
    if not state.run_ids:
        state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)


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
            started_at=time.time(),
            duration_seconds=0.0,
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

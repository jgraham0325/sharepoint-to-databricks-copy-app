from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import asyncio
import os
import re
import tempfile
import time
import uuid

import json

from common import config
from common.connectors.microsoft_graph import (
    get_me_id,
    graph_download_to_path,
    graph_get_download_urls_concurrent,
)
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
from services.job_service import (
    get_run_statuses,
    get_transfer_job_id,
    submit_transfer_via_manifest,
)

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


def _append_result(state: TransferState, result: FileResult) -> None:
    """Append a file result to state, capping at MAX_TRANSFER_RESULTS_IN_MEMORY."""
    if len(state.results) < config.MAX_TRANSFER_RESULTS_IN_MEMORY:
        state.results.append(result)
    else:
        state.results_truncated = True


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
                    _append_result(state, FileResult(name=name, status=TransferStatus.COMPLETED))
                else:
                    state.failed += 1
                    _append_result(
                        state,
                        FileResult(name=name, status=TransferStatus.FAILED, error=state_message or "Job task failed"),
                    )
        else:
            # PENDING, QUEUED, BLOCKED, WAITING_FOR_RETRY = queued; RUNNING, TERMINATING = running
            js.status = "queued" if lc in ("PENDING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY") else "running"
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
    ms_refresh_token: Optional[str] = None,
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
    asyncio.create_task(
        _execute_transfer(state, files, catalog, schema_name, volume, subfolder, ms_token, ms_refresh_token)
    )
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
    ms_refresh_token: Optional[str] = None,
) -> None:
    ws_client = get_workspace_client()
    threshold = config.LARGE_FILE_THRESHOLD_BYTES
    max_on_server = config.MAX_FILES_ON_SERVER
    full_volume_base = f"/Volumes/{catalog}/{schema_name}/{volume}"

    # Decide server vs job path before resolving URLs. For job path we never resolve on the backend
    # (avoids N Graph calls and throttling); the job resolves one URL per file sequentially with retry.
    use_server_only = (
        len(files) < max_on_server
        and all(0 < f.size < threshold for f in files)
    )

    if use_server_only:
        # Resolve download URLs only for the small batch we'll transfer on this server
        paths = [f"/drives/{f.drive_id}/items/{f.item_id}" for f in files]
        url_results = await graph_get_download_urls_concurrent(paths, ms_token)
        items: List[Tuple[FileTransferItem, str, str]] = []
        for f, (download_url, error_msg) in zip(files, url_results):
            if error_msg or not download_url:
                state.failed += 1
                _append_result(
                    state,
                    FileResult(name=f.name, status=TransferStatus.FAILED, error=error_msg or "No download URL"),
                )
                logger.error("Failed to get URL for %s: %s", f.name, error_msg)
            else:
                items.append((f, download_url, _target_path(subfolder, f)))
        if not items:
            state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED
            if state.started_at is not None:
                state.duration_seconds = round(time.time() - state.started_at, 2)
            return
        # Transfer all on this server (no job)
        for f, download_url, target_path in items:
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
                _append_result(state, FileResult(name=f.name, status=TransferStatus.COMPLETED))
                logger.info("Transferred %s (server)", f.name)
            except Exception as exc:
                state.failed += 1
                _append_result(state, FileResult(name=f.name, status=TransferStatus.FAILED, error=str(exc)))
                logger.error("Failed to transfer %s: %s", f.name, exc)
            finally:
                if tmp and os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
        state.status = TransferStatus.COMPLETED if state.failed == 0 else TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)
        return

    # Bulk path: no URL resolution on backend (job will resolve per file with retry). Write manifests with drive_id + item_id.
    job_items: List[Tuple[FileTransferItem, str]] = [
        (f, _target_path(subfolder, f)) for f in files
    ]
    job_id = get_transfer_job_id()
    job_not_found_msg = (
        "Databricks job 'sharepoint-transfer' not found. Deploy the bundle (databricks bundle deploy) "
        "or set SHAREPOINT_TRANSFER_JOB_ID in the app environment."
    )
    if not job_id:
        logger.warning("%s", job_not_found_msg)
        for f, _ in job_items:
            state.failed += 1
            _append_result(state, FileResult(name=f.name, status=TransferStatus.FAILED, error=job_not_found_msg))
        state.status = TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)
        return

    try:
        user_oid = await get_me_id(ms_token)
    except Exception as e:
        logger.exception("Failed to get user id for transfer job")
        for f, _ in job_items:
            state.failed += 1
            _append_result(
                state,
                FileResult(name=f.name, status=TransferStatus.FAILED, error=f"User identity lookup failed: {e}"),
            )
        state.status = TransferStatus.FAILED
        if state.started_at is not None:
            state.duration_seconds = round(time.time() - state.started_at, 2)
        return

    chunk_size = config.FILES_PER_MANIFEST_CHUNK
    run_ids: List[int] = []
    run_id_to_file: Dict[str, List[str]] = {}

    for chunk_index in range(0, len(job_items), chunk_size):
        chunk = job_items[chunk_index : chunk_index + chunk_size]
        manifest = [
            {
                "drive_id": f.drive_id,
                "item_id": f.item_id,
                "volume_path": f"{full_volume_base}/{path}",
            }
            for f, path in chunk
        ]
        manifest_path_in_volume = f"_manifests/transfer_{state.transfer_id}_{chunk_index}.json"
        manifest_bytes = json.dumps(manifest).encode("utf-8")
        upload_to_volume(ws_client, catalog, schema_name, volume, manifest_path_in_volume, manifest_bytes)
        full_manifest_path = f"{full_volume_base}/{manifest_path_in_volume}"

        run_id = await asyncio.to_thread(
            submit_transfer_via_manifest,
            ms_token,
            full_manifest_path,
            f"chunk {chunk_index}",
            ms_refresh_token=ms_refresh_token,
            user_oid=user_oid,
        )
        file_names = [f.name for f, _ in chunk]
        if run_id is not None:
            run_ids.append(run_id)
            run_id_to_file[str(run_id)] = file_names
        else:
            for name in file_names:
                state.failed += 1
                _append_result(
                    state,
                    FileResult(name=name, status=TransferStatus.FAILED, error=job_not_found_msg),
                )

    if run_ids:
        state.run_ids = run_ids
        state.run_id_to_file = run_id_to_file
        host = (config.DATABRICKS_HOST or "").rstrip("/")
        if host and job_id:
            state.job_run_urls = [f"{host}#job/{job_id}/run/{rid}" for rid in run_ids]
        else:
            state.job_run_urls = None
        state.job_run_statuses = [
            JobRunStatus(
                run_id=rid,
                url=state.job_run_urls[i] if state.job_run_urls and i < len(state.job_run_urls) else None,
                status="queued",
                file_names=run_id_to_file.get(str(rid), []),
            )
            for i, rid in enumerate(run_ids)
        ]

    # Sync outcomes from job runs is done in get_transfer via _sync_state_from_job_runs; use _append_result there too
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
    ms_refresh_token: Optional[str] = None,
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
        ms_refresh_token=ms_refresh_token,
    )

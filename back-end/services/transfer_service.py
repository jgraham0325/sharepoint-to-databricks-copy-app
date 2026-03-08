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
    TaskIterationStatus,
    TransferBatch,
)
from services.sharepoint_service import list_all_files_in_folder
from services.job_service import (
    get_run_for_each_iterations,
    get_run_statuses,
    get_transfer_job_id,
    submit_transfer_via_manifests,
)

logger = get_logger(__name__)

# In-memory transfer tracking (single-instance app)
_transfers: Dict[str, TransferState] = {}


def build_batches_for_response(state: TransferState) -> Optional[List[TransferBatch]]:
    """
    Build iteration-centric batches (status + file list per batch) for API response.
    Used when a single run processes all files via For Each; requires task_iterations,
    batch_file_counts, and the full file list from run_id_to_file.
    """
    if not state.run_ids or len(state.run_ids) != 1 or not state.run_id_to_file:
        return None
    names = state.run_id_to_file.get(str(state.run_ids[0]), [])
    if isinstance(names, str):
        names = [names]
    counts = state.batch_file_counts
    iterations = state.task_iterations
    if not counts or not iterations or sum(counts) != len(names):
        return None
    by_index = {it.index: it for it in iterations}
    batches: List[TransferBatch] = []
    offset = 0
    for i in range(len(counts)):
        n = counts[i]
        batch_names = names[offset : offset + n]
        offset += n
        it = by_index.get(i)
        if it is None:
            batches.append(
                TransferBatch(
                    index=i,
                    life_cycle_state="PENDING",
                    result_state="",
                    state_message="",
                    file_count=n,
                    file_names=batch_names,
                )
            )
        else:
            batches.append(
                TransferBatch(
                    index=it.index,
                    life_cycle_state=it.life_cycle_state,
                    result_state=it.result_state or "",
                    state_message=it.state_message or "",
                    file_count=n,
                    file_names=batch_names,
                )
            )
    return batches


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
    terminates, update job_run_statuses and task_iterations for UI, and set final status when all runs are done.
    """
    if not state.run_ids or not state.run_id_to_file:
        return
    run_id_to_file = state.run_id_to_file
    statuses = get_run_statuses(state.run_ids)
    # Build map run_id -> (life_cycle, result_state, state_message)
    status_by_run = {run_id: (lc, res, msg) for run_id, lc, res, msg in statuses}
    # For Each iterations: fetch from first run (we have a single run per transfer now).
    # Note: Databricks may only return running/completed iterations, not queued ones.
    if len(state.run_ids) == 1:
        run_id = state.run_ids[0]
        raw_iterations = get_run_for_each_iterations(run_id)
        # Use API order as-is (run.iterations is typically in input/manifest order; sorting by start_time
        # put completed-first iterations first and broke the batch index mapping).
        state.task_iterations = [
            TaskIterationStatus(
                index=i,
                life_cycle_state=it.get("life_cycle_state", "PENDING"),
                result_state=it.get("result_state", ""),
                state_message=it.get("state_message", ""),
            )
            for i, it in enumerate(raw_iterations)
        ]
        logger.info(
            "transfer_sync: transfer_id=%s run_id=%s raw_iterations=%s total_iterations=%s total=%s task_iterations(len)=%s",
            state.transfer_id,
            run_id,
            len(raw_iterations),
            state.total_iterations,
            state.total,
            len(state.task_iterations or []),
        )
        # Estimate completed/failed from iterations so progress bar updates before run terminates.
        # Use actual file counts per batch when batch_file_counts is set; otherwise fall back to average.
        if (
            state.task_iterations
            and state.total_iterations is not None
            and state.total_iterations > 0
            and state.total > 0
        ):
            counts = state.batch_file_counts
            if counts and len(counts) >= len(state.task_iterations):
                completed_from_iterations = sum(
                    counts[it.index] for it in state.task_iterations if it.result_state == "SUCCESS"
                )
                failed_from_iterations = sum(
                    counts[it.index] for it in state.task_iterations if it.result_state == "FAILED"
                )
                state.completed = min(completed_from_iterations, state.total)
                state.failed = min(failed_from_iterations, state.total - state.completed)
                logger.info(
                    "transfer_sync: transfer_id=%s progress from iterations (batch counts) -> completed=%s failed=%s",
                    state.transfer_id,
                    state.completed,
                    state.failed,
                )
            else:
                success_count = sum(1 for it in state.task_iterations if it.result_state == "SUCCESS")
                failed_count = sum(1 for it in state.task_iterations if it.result_state == "FAILED")
                files_per_iteration = state.total // state.total_iterations
                if files_per_iteration > 0:
                    state.completed = min(success_count * files_per_iteration, state.total)
                    state.failed = min(failed_count * files_per_iteration, state.total - state.completed)
                    logger.info(
                        "transfer_sync: transfer_id=%s progress from iterations (average) success_count=%s failed_count=%s files_per_iteration=%s -> completed=%s failed=%s",
                        state.transfer_id,
                        success_count,
                        failed_count,
                        files_per_iteration,
                        state.completed,
                        state.failed,
                    )
                else:
                    logger.info(
                        "transfer_sync: transfer_id=%s skipped progress update (files_per_iteration=0)",
                        state.transfer_id,
                    )
        else:
            logger.info(
                "transfer_sync: transfer_id=%s not updating progress from iterations (task_iterations=%s total_iterations=%s total=%s)",
                state.transfer_id,
                bool(state.task_iterations),
                state.total_iterations,
                state.total,
            )
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
            # Set completed/failed and results. For single run with For Each: use per-iteration outcomes
            # so each file gets the status of its batch; otherwise apply run-level outcome to all files.
            if names:
                state.completed = 0
                state.failed = 0
                state.results = []
                counts = state.batch_file_counts
                iterations = state.task_iterations
                if (
                    len(state.run_ids) == 1
                    and counts
                    and iterations
                    and len(counts) >= len(iterations)
                    and sum(counts) == len(names)
                ):
                    # Per-batch: names are in batch order (batch 0, then batch 1, ...)
                    by_index = {it.index: it for it in iterations}
                    offset = 0
                    for i in range(len(counts)):
                        batch_size = counts[i]
                        batch_names = names[offset : offset + batch_size]
                        offset += batch_size
                        it = by_index.get(i)
                        if it is None:
                            for name in batch_names:
                                state.failed += 1
                                _append_result(
                                    state,
                                    FileResult(name=name, status=TransferStatus.FAILED, error="Unknown batch status"),
                                )
                            continue
                        file_status = TransferStatus.COMPLETED if it.result_state == "SUCCESS" else TransferStatus.FAILED
                        err = None if it.result_state == "SUCCESS" else (it.state_message or "Batch failed")
                        for name in batch_names:
                            if it.result_state == "SUCCESS":
                                state.completed += 1
                                _append_result(state, FileResult(name=name, status=TransferStatus.COMPLETED))
                            else:
                                state.failed += 1
                                _append_result(state, FileResult(name=name, status=TransferStatus.FAILED, error=err))
                else:
                    # Run-level outcome for all files (no per-iteration breakdown or multi-run)
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
    manifest_paths: List[str] = []
    batch_file_counts: List[int] = []
    all_file_names: List[str] = []

    for chunk_index in range(0, len(job_items), chunk_size):
        chunk = job_items[chunk_index : chunk_index + chunk_size]
        batch_file_counts.append(len(chunk))
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
        manifest_paths.append(full_manifest_path)
        all_file_names.extend(f.name for f, _ in chunk)

    if manifest_paths:
        run_id = await asyncio.to_thread(
            submit_transfer_via_manifests,
            ms_token,
            manifest_paths,
            task_label="bulk",
            ms_refresh_token=ms_refresh_token,
            user_oid=user_oid,
        )
        if run_id is not None:
            state.run_ids = [run_id]
            state.run_id_to_file = {str(run_id): all_file_names}
            host = (config.DATABRICKS_HOST or "").rstrip("/")
            if host and job_id:
                job_run_url = f"{host}#job/{job_id}/run/{run_id}"
                state.job_run_urls = [job_run_url]
                state.job_run_url = job_run_url
            else:
                state.job_run_urls = None
                state.job_run_url = None
            state.job_run_statuses = [
                JobRunStatus(
                    run_id=run_id,
                    url=state.job_run_url,
                    status="queued",
                    file_names=all_file_names,
                )
            ]
            state.task_iterations = []
            state.total_iterations = len(manifest_paths)
            state.batch_file_counts = batch_file_counts
        else:
            for name in all_file_names:
                state.failed += 1
                _append_result(
                    state,
                    FileResult(name=name, status=TransferStatus.FAILED, error=job_not_found_msg),
                )

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

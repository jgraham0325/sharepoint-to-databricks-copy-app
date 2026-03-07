"""Resolve and run the SharePoint transfer job on Databricks for large files (multiple files per run)."""
from typing import List, Optional, Tuple

from databricks.sdk import WorkspaceClient

from common import config
from common.authentication.workspace import get_workspace_client
from common.logger import get_logger

logger = get_logger(__name__)

TRANSFER_JOB_NAME = "sharepoint-transfer"

# Number of files per job run (batch size); each file is transferred in full.
# Keep small: SharePoint download URLs are long (1–3 KB+); many in one run can exceed
# job parameter/argv limits. 4 files per run is a safe limit.
TRANSFER_BATCH_SIZE = 4


def _get_transfer_job_id(client: WorkspaceClient) -> Optional[int]:
    """Return job_id for the sharepoint-transfer job by name."""
    for j in client.jobs.list(name=TRANSFER_JOB_NAME, expand_tasks=True):
        return j.job_id
    return None


def get_transfer_job_id() -> Optional[int]:
    """Return job_id for the sharepoint-transfer job (from config or by name)."""
    client = get_workspace_client()
    return config.SHAREPOINT_TRANSFER_JOB_ID or _get_transfer_job_id(client)


def submit_transfer_batch(
    token: str,
    url_path_pairs: List[Tuple[str, str]],
    task_label: str = "transfer",
) -> Optional[int]:
    """
    Submit one job run for a batch of files (legacy: argv url/path pairs).
    Use submit_transfer_via_manifest for large batches instead.
    """
    if not url_path_pairs:
        return None
    client = get_workspace_client()
    job_id = get_transfer_job_id()
    if not job_id:
        logger.warning("No sharepoint-transfer job found; deploy the bundle job or set SHAREPOINT_TRANSFER_JOB_ID")
        return None
    flat: List[str] = [token]
    for url, path in url_path_pairs:
        flat.append(url)
        flat.append(path)
    response = client.jobs.run_now(job_id=job_id, python_params=flat)
    run_id = response.run_id if response else None
    if run_id:
        logger.info("Submitted transfer run_id=%s for batch of %d file(s) (%s)", run_id, len(url_path_pairs), task_label)
    return run_id


def submit_transfer_via_manifest(
    token: str,
    manifest_volume_path: str,
    task_label: str = "manifest",
) -> Optional[int]:
    """
    Submit one job run that reads a manifest from a UC volume path and transfers all files listed.
    python_params: [token, manifest_volume_path]. Does not wait for completion.
    """
    if not manifest_volume_path or not token:
        return None
    job_id = get_transfer_job_id()
    if not job_id:
        logger.warning("No sharepoint-transfer job found; deploy the bundle job or set SHAREPOINT_TRANSFER_JOB_ID")
        return None
    response = get_workspace_client().jobs.run_now(
        job_id=job_id,
        python_params=[token, manifest_volume_path],
    )
    run_id = response.run_id if response else None
    if run_id:
        logger.info("Submitted transfer run_id=%s via manifest %s (%s)", run_id, manifest_volume_path, task_label)
    return run_id


def _state_value(x) -> Optional[str]:
    """Normalize enum or string to string (e.g. RunLifeCycleState.TERMINATED -> 'TERMINATED')."""
    if x is None:
        return None
    return getattr(x, "value", x) if not isinstance(x, str) else x


def _run_state_from_api(run) -> Tuple[str, str, str]:
    """Extract (life_cycle_state, result_state, state_message) from a jobs.get_run response."""
    run_state = getattr(run, "state", None)
    life_cycle = _state_value(
        getattr(run_state, "life_cycle_state", None) if run_state else None
    ) or _state_value(getattr(run, "life_cycle_state", None)) or _state_value(
        getattr(run, "run_life_cycle_state", None)
    ) or "UNKNOWN"
    result_state = _state_value(
        getattr(run_state, "result_state", None) if run_state else None
    ) or _state_value(getattr(run, "result_state", None)) or ""
    state_message = (
        getattr(run_state, "state_message", None) if run_state else None
        or getattr(run, "state_message", None)
        or getattr(run, "message", None)
        or ""
    )
    if state_message is None:
        state_message = ""
    return (life_cycle, result_state, str(state_message))


def get_run_statuses(run_ids: List[int]) -> List[Tuple[int, str, str, str]]:
    """
    Get current status for each run_id. Returns list of (run_id, life_cycle_state, result_state, state_message).
    life_cycle_state is e.g. PENDING, RUNNING, TERMINATED, SKIPPED, INTERNAL_ERROR.
    result_state is e.g. SUCCESS, FAILED (only when terminated).
    """
    client = get_workspace_client()
    out: List[Tuple[int, str, str, str]] = []
    for run_id in run_ids:
        try:
            run = client.jobs.get_run(run_id=run_id)
            lc, res, msg = _run_state_from_api(run)
            if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR") and not res:
                # Prefer task result when available
                tasks = getattr(run, "tasks", None) or []
                if tasks:
                    t = tasks[0]
                    task_state = getattr(t, "state", None)
                    raw = (
                        getattr(task_state, "result_state", None) if task_state else None
                    ) or getattr(t, "result_state", None)
                    res = _state_value(raw) or res
                    msg = (
                        getattr(task_state, "state_message", None) if task_state else None
                    ) or getattr(t, "state_message", None) or msg
                else:
                    res = _state_value(getattr(run.state, "result_state", None)) if getattr(run, "state", None) else ""
            out.append((run_id, lc, res or "", msg or ""))
        except Exception as e:
            logger.warning("Failed to get run %s: %s", run_id, e)
            out.append((run_id, "INTERNAL_ERROR", "FAILED", str(e)))
    return out


def get_run_outcomes(run_ids: List[int]) -> List[Tuple[int, str, str]]:
    """
    Poll run status for the given run_ids. Returns list of (run_id, "SUCCESS"|"FAILED", error_message)
    only for runs that have terminated. Runs still in progress are omitted.

    The Databricks SDK returns run.state (RunState) with life_cycle_state and result_state;
    we read from run.state, with fallbacks for older/different API shapes.
    """
    statuses = get_run_statuses(run_ids)
    outcomes: List[Tuple[int, str, str]] = []
    for run_id, life_cycle, result_state, state_message in statuses:
        if life_cycle not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            continue
        if result_state == "SUCCESS":
            outcomes.append((run_id, "SUCCESS", ""))
        else:
            outcomes.append((run_id, "FAILED", state_message or str(result_state or "FAILED")))
    return outcomes

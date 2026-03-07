"""Resolve and run the SharePoint transfer job on Databricks for large files (multiple files per run)."""
import json
from typing import List, Optional, Tuple

from databricks.sdk import WorkspaceClient

from common import config
from common.authentication.workspace import get_workspace_client
from common.logger import get_logger

logger = get_logger(__name__)

TRANSFER_JOB_NAME = "sharepoint-transfer"


def _get_transfer_job_id(client: WorkspaceClient) -> Optional[int]:
    """Return job_id for the sharepoint-transfer job by name."""
    for j in client.jobs.list(name=TRANSFER_JOB_NAME, expand_tasks=True):
        return j.job_id
    return None


def get_transfer_job_id() -> Optional[int]:
    """Return job_id for the sharepoint-transfer job (from config or by name)."""
    client = get_workspace_client()
    return config.SHAREPOINT_TRANSFER_JOB_ID or _get_transfer_job_id(client)


def _tokens_secret_key(user_oid: str) -> str:
    """Secret key for per-user tokens (GUID with dashes replaced by underscores)."""
    return "tokens_" + user_oid.replace("-", "_")


def _write_user_tokens(
    scope: str,
    user_oid: str,
    access_token: str,
    refresh_token: Optional[str] = None,
) -> str:
    """Write access_token and optional refresh_token to the secret scope. Returns the secret key used."""
    key = _tokens_secret_key(user_oid)
    payload = {"access_token": access_token}
    if refresh_token:
        payload["refresh_token"] = refresh_token
    get_workspace_client().secrets.put_secret(
        scope=scope,
        key=key,
        string_value=json.dumps(payload),
    )
    return key


def delete_user_tokens(user_oid: str) -> None:
    """Delete the per-user tokens secret for the given user (e.g. on logout). No-op if scope not configured."""
    if not config.SHAREPOINT_SECRET_SCOPE:
        return
    key = _tokens_secret_key(user_oid)
    try:
        get_workspace_client().secrets.delete_secret(scope=config.SHAREPOINT_SECRET_SCOPE, key=key)
    except Exception as e:
        logger.warning("Failed to delete user tokens secret key=%s: %s", key, e)


def submit_transfer_via_manifest(
    token: str,
    manifest_volume_path: str,
    task_label: str = "manifest",
    ms_refresh_token: Optional[str] = None,
    user_oid: Optional[str] = None,
) -> Optional[int]:
    """
    Submit one job run that reads a manifest from a UC volume path and transfers all files listed.
    Tokens are written to the configured secret scope; job params are only [scope, tokens_key, manifest_path].
    Requires SHAREPOINT_SECRET_SCOPE and user_oid (from Graph /me). Does not wait for completion.
    """
    if not manifest_volume_path or not token:
        return None
    if not config.SHAREPOINT_SECRET_SCOPE:
        logger.error("SHAREPOINT_SECRET_SCOPE is not set; cannot submit transfer job")
        return None
    if not user_oid:
        logger.error("user_oid is required for transfer job (resolve via get_me_id)")
        return None
    job_id = get_transfer_job_id()
    if not job_id:
        logger.warning("No sharepoint-transfer job found; deploy the bundle job or set SHAREPOINT_TRANSFER_JOB_ID")
        return None
    scope = config.SHAREPOINT_SECRET_SCOPE
    tokens_key = _write_user_tokens(scope, user_oid, token, ms_refresh_token)
    python_params = [scope, tokens_key, manifest_volume_path]
    response = get_workspace_client().jobs.run_now(
        job_id=job_id,
        python_params=python_params,
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

"""Resolve and run the SharePoint transfer job on Databricks for large files (multiple files per run)."""
import json
from typing import Any, Dict, List, Optional, Tuple

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
    Convenience wrapper: builds a single-element manifest_paths list and calls submit_transfer_via_manifests.
    """
    return submit_transfer_via_manifests(
        token=token,
        manifest_paths=[manifest_volume_path],
        task_label=task_label,
        ms_refresh_token=ms_refresh_token,
        user_oid=user_oid,
    )


def submit_transfer_via_manifests(
    token: str,
    manifest_paths: List[str],
    task_label: str = "manifests",
    ms_refresh_token: Optional[str] = None,
    user_oid: Optional[str] = None,
) -> Optional[int]:
    """
    Submit one job run that processes all given manifest paths via the job's For Each task.
    Tokens are written to the configured secret scope; job run is started with job_parameters
    (manifest_paths_json, scope, tokens_key). Requires SHAREPOINT_SECRET_SCOPE and user_oid.
    Does not wait for completion. Returns the single run_id.
    """
    if not manifest_paths or not token:
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
    job_parameters = {
        "manifest_paths_json": json.dumps(manifest_paths),
        "scope": scope,
        "tokens_key": tokens_key,
    }
    response = get_workspace_client().jobs.run_now(
        job_id=job_id,
        job_parameters=job_parameters,
    )
    run_id = response.run_id if response else None
    if run_id:
        logger.info(
            "Submitted transfer run_id=%s via %d manifest(s) (%s)",
            run_id,
            len(manifest_paths),
            task_label,
        )
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


def _task_state_to_status(task: Any) -> Dict[str, Any]:
    """Extract life_cycle_state, result_state, state_message, start_time, run_id from a task (or run) object.
    start_time is used to sort iterations into input/batch order (earliest started = batch 0).
    run_id is used as tie-breaker when start_times are equal."""
    task_state = getattr(task, "state", None)
    life_cycle = _state_value(
        getattr(task_state, "life_cycle_state", None) if task_state else None
    ) or _state_value(getattr(task, "life_cycle_state", None)) or "PENDING"
    result_state = _state_value(
        getattr(task_state, "result_state", None) if task_state else None
    ) or _state_value(getattr(task, "result_state", None)) or ""
    state_message = (
        getattr(task_state, "state_message", None) if task_state else None
        or getattr(task, "state_message", None)
        or getattr(task, "message", None)
        or ""
    )
    if state_message is None:
        state_message = ""
    start_time = getattr(task, "start_time", None) or (task.get("start_time") if isinstance(task, dict) else None)
    run_id = getattr(task, "run_id", None) or (task.get("run_id") if isinstance(task, dict) else None)
    return {
        "life_cycle_state": life_cycle or "PENDING",
        "result_state": result_state or "",
        "state_message": str(state_message),
        "start_time": start_time if start_time is not None else 0,
        "run_id": run_id,
    }


def _collect_transfer_task_runs(tasks: List[Any], out: List[Dict[str, Any]], _depth: int = 0) -> None:
    """Recursively collect state from every task with task_key 'transfer' (For Each iterations).
    Tries both .tasks (nested task definitions) and .task_runs (per-iteration runs, API 2.2).
    """
    for t in tasks or []:
        task_key = getattr(t, "task_key", None) or getattr(t, "task_key", "")
        nested = getattr(t, "tasks", None) or []
        task_runs = getattr(t, "task_runs", None) or []
        if _depth <= 1:
            logger.info(
                "transfer_iterations: task task_key=%s has .tasks(len=%s) .task_runs(len=%s)",
                task_key,
                len(nested),
                len(task_runs),
            )
        if task_key == "transfer":
            st = _task_state_to_status(t)
            out.append(st)
            logger.info("transfer_iterations: collected transfer iteration life_cycle=%s result_state=%s", st.get("life_cycle_state"), st.get("result_state"))
        if nested:
            _collect_transfer_task_runs(nested, out, _depth + 1)
        for tr in task_runs:
            key = getattr(tr, "task_key", None) or getattr(tr, "task_key", "")
            if key == "transfer":
                st = _task_state_to_status(tr)
                out.append(st)
                logger.info("transfer_iterations: collected task_run transfer life_cycle=%s result_state=%s", st.get("life_cycle_state"), st.get("result_state"))


def _inspect_run_tasks(run_id: int, run: Any, tasks: List[Any]) -> None:
    """Log structure of run.tasks so we can see actual API shape (e.g. task_key names, nested keys)."""
    attrs_run = [a for a in dir(run) if not a.startswith("_")]
    logger.info("transfer_iterations: run_id=%s run attributes (no _): %s", run_id, attrs_run[:40])
    # If run has task_runs at top level, log its length
    top_task_runs = getattr(run, "task_runs", None)
    if top_task_runs is not None:
        logger.info("transfer_iterations: run_id=%s run.task_runs(len)=%s", run_id, len(top_task_runs))
    for i, t in enumerate(tasks[:5]):  # first 5 tasks only
        attrs = [a for a in dir(t) if not a.startswith("_")]
        task_key = getattr(t, "task_key", None)
        has_tasks = getattr(t, "tasks", None)
        has_task_runs = getattr(t, "task_runs", None)
        has_run_id = getattr(t, "run_id", None)
        # Try dict-style too (e.g. from JSON)
        if isinstance(t, dict):
            task_key = t.get("task_key", task_key)
            has_tasks = t.get("tasks") or has_tasks
            has_task_runs = t.get("task_runs") or has_task_runs
            has_run_id = t.get("run_id", has_run_id)
        st = getattr(t, "state", None)
        if isinstance(t, dict):
            st = t.get("state", st)
        st_lc = _state_value(getattr(st, "life_cycle_state", None)) if st and not isinstance(st, dict) else (st.get("life_cycle_state") if isinstance(st, dict) else None)
        logger.info(
            "transfer_iterations: run_id=%s task[%s] task_key=%s .tasks=%s .task_runs=%s .run_id=%s state.life_cycle=%s attrs_sample=%s",
            run_id,
            i,
            task_key,
            len(has_tasks) if has_tasks else None,
            len(has_task_runs) if has_task_runs else None,
            has_run_id,
            st_lc,
            attrs[:25] if not isinstance(t, dict) else list(t.keys())[:25],
        )
        if task_key == "for_each_transfer":
            fe = getattr(t, "for_each_task", None) or (t.get("for_each_task") if isinstance(t, dict) else None)
            if fe is not None:
                fe_attrs = [a for a in dir(fe) if not a.startswith("_")] if not isinstance(fe, dict) else list(fe.keys())
                fe_iter = getattr(fe, "iterations", None) or (fe.get("iterations") if isinstance(fe, dict) else None)
                fe_tr = getattr(fe, "task_runs", None) or (fe.get("task_runs") if isinstance(fe, dict) else None)
                logger.info(
                    "transfer_iterations: run_id=%s for_each_task attrs=%s .iterations=%s .task_runs=%s",
                    run_id,
                    fe_attrs[:30],
                    len(fe_iter) if fe_iter else None,
                    len(fe_tr) if fe_tr else None,
                )


def _task_run_stats_from_for_each_task(tasks: List[Any]) -> Optional[Any]:
    """Get ForEachTaskTaskRunStats from the for_each_transfer task (task.for_each_task.stats.task_run_stats)."""
    for t in tasks or []:
        task_key = getattr(t, "task_key", None) or (t.get("task_key") if isinstance(t, dict) else None)
        if task_key != "for_each_transfer":
            continue
        fe = getattr(t, "for_each_task", None) or (t.get("for_each_task") if isinstance(t, dict) else None)
        if fe is None:
            continue
        stats = getattr(fe, "stats", None) or (fe.get("stats") if isinstance(fe, dict) else None)
        if stats is None:
            continue
        return getattr(stats, "task_run_stats", None) or (stats.get("task_run_stats") if isinstance(stats, dict) else None)
    return None


def _synthetic_iterations_from_task_run_stats(task_run_stats: Any) -> List[Dict[str, Any]]:
    """
    Build a list of iteration status dicts from ForEachTaskTaskRunStats (counts only).
    Order: succeeded, failed, active (running), then pending. Used when run.iterations is not populated.
    Includes start_time (index) so transfer_service can sort consistently.
    """
    total = getattr(task_run_stats, "total_iterations", None) or (task_run_stats.get("total_iterations") if isinstance(task_run_stats, dict) else None)
    if total is None or total <= 0:
        return []
    succeeded = getattr(task_run_stats, "succeeded_iterations", None) or (task_run_stats.get("succeeded_iterations") if isinstance(task_run_stats, dict) else 0) or 0
    failed = getattr(task_run_stats, "failed_iterations", None) or (task_run_stats.get("failed_iterations") if isinstance(task_run_stats, dict) else 0) or 0
    active = getattr(task_run_stats, "active_iterations", None) or (task_run_stats.get("active_iterations") if isinstance(task_run_stats, dict) else 0) or 0
    out: List[Dict[str, Any]] = []
    t = 0
    for _ in range(succeeded):
        out.append({"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "", "start_time": t})
        t += 1
    for _ in range(failed):
        out.append({"life_cycle_state": "TERMINATED", "result_state": "FAILED", "state_message": "", "start_time": t})
        t += 1
    for _ in range(active):
        out.append({"life_cycle_state": "RUNNING", "result_state": "", "state_message": "", "start_time": t})
        t += 1
    for _ in range(total - len(out)):
        out.append({"life_cycle_state": "PENDING", "result_state": "", "state_message": "", "start_time": t})
        t += 1
    return out[:total]


def get_run_for_each_iterations(run_id: int) -> List[Dict[str, Any]]:
    """
    Get status for each For Each iteration (transfer task) of a sharepoint-transfer job run.
    Returns a list of dicts with life_cycle_state, result_state, state_message (Databricks naming).

    Uses the Databricks SDK shape:
    - run.iterations: list of RunTask per iteration (when populated for for-each runs).
    - For-each task: task.for_each_task.stats.task_run_stats (ForEachTaskTaskRunStats) with
      total_iterations, succeeded_iterations, failed_iterations, active_iterations, etc.
    When run.iterations is not populated, builds a synthetic list from task_run_stats.
    """
    try:
        client = get_workspace_client()
        run = client.jobs.get_run(run_id=run_id)
        tasks = getattr(run, "tasks", None) or []
        run_state = getattr(run, "state", None)
        run_lc = _state_value(getattr(run_state, "life_cycle_state", None)) if run_state else None
        logger.info(
            "transfer_iterations: get_run run_id=%s run.tasks(len)=%s run.state.life_cycle_state=%s",
            run_id,
            len(tasks),
            run_lc,
        )
        out: List[Dict[str, Any]] = []

        # 1) run.iterations is the documented list of iteration runs (RunTask) for for-each jobs.
        # API order is not guaranteed (e.g. completion order). Sort by start_time ascending so
        # position 0 = earliest started = batch 0 (first manifest chunk).
        run_iterations = getattr(run, "iterations", None) or []
        if run_iterations:
            logger.info("transfer_iterations: run_id=%s using run.iterations(len)=%s", run_id, len(run_iterations))
            statuses_with_start = [_task_state_to_status(it) for it in run_iterations]
            # Sort by start_time (earliest = batch 0); use run_id as tie-breaker when start_times equal.
            statuses_with_start.sort(
                key=lambda s: (
                    s.get("start_time") is None,
                    s.get("start_time") or 0,
                    s.get("run_id") if s.get("run_id") is not None else 0,
                )
            )
            out = statuses_with_start
            logger.info(
                "transfer_iterations: run_id=%s collected %s iteration(s) (sorted by start_time) states=%s",
                run_id,
                len(out),
                [(it.get("life_cycle_state"), it.get("result_state")) for it in out],
            )
            return out

        # 2) From the for_each task: for_each_task.stats.task_run_stats (ForEachTaskTaskRunStats)
        task_run_stats = _task_run_stats_from_for_each_task(tasks)
        if task_run_stats is not None:
            out = _synthetic_iterations_from_task_run_stats(task_run_stats)
            if out:
                total = getattr(task_run_stats, "total_iterations", None) or (task_run_stats.get("total_iterations") if isinstance(task_run_stats, dict) else None)
                logger.info(
                    "transfer_iterations: run_id=%s using for_each_task.stats.task_run_stats total=%s synthetic(len)=%s",
                    run_id,
                    total,
                    len(out),
                )
                return out

        # 3) Fallbacks
        _collect_transfer_task_runs(tasks, out, 0)
        if len(out) == 0:
            run_task_runs = getattr(run, "task_runs", None) or []
            if run_task_runs:
                logger.info("transfer_iterations: run_id=%s trying run.task_runs(len)=%s", run_id, len(run_task_runs))
            for tr in run_task_runs:
                key = getattr(tr, "task_key", None) or getattr(tr, "task_key", "")
                if key == "transfer":
                    out.append(_task_state_to_status(tr))
        if len(out) == 0:
            _inspect_run_tasks(run_id, run, tasks)
        logger.info(
            "transfer_iterations: run_id=%s collected %s iteration(s) states=%s",
            run_id,
            len(out),
            [(it.get("life_cycle_state"), it.get("result_state")) for it in out],
        )
        return out
    except Exception as e:
        logger.warning("Failed to get run for_each iterations run_id=%s: %s", run_id, e)
        return []


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

"""
SQLite-backed persistence for transfer metadata.

Stores the core fields needed to list and resume transfers across backend restarts.
Transient state (per-file results, task_iterations, run_id_to_file) stays in-memory
and is rebuilt from Databricks job runs on demand.
"""
import json
import os
import sqlite3
import threading
from typing import List, Optional

from common.logger import get_logger
from models.transfer import TransferState, TransferStatus, TransferSummary

logger = get_logger(__name__)

_DB_PATH = os.environ.get(
    "TRANSFER_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "transfers.db"),
)

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Return a per-thread SQLite connection (created once, reused)."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
        conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        _local.conn = conn
    return conn


_initialized = False


def init_db() -> None:
    """Create the transfers table if it doesn't exist."""
    global _initialized
    if _initialized:
        return
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            transfer_id     TEXT PRIMARY KEY,
            status          TEXT NOT NULL DEFAULT 'pending',
            total           INTEGER NOT NULL DEFAULT 0,
            completed       INTEGER NOT NULL DEFAULT 0,
            failed          INTEGER NOT NULL DEFAULT 0,
            catalog         TEXT,
            schema_name     TEXT,
            volume          TEXT,
            run_ids_json    TEXT,
            job_run_url     TEXT,
            catalog_explorer_url TEXT,
            started_at      REAL,
            duration_seconds REAL
        )
    """)
    conn.commit()
    _initialized = True
    logger.info("Transfer store initialized at %s", _DB_PATH)


def save(state: TransferState) -> None:
    """Insert or update a transfer's persisted metadata."""
    init_db()
    run_ids_json = json.dumps(state.run_ids) if state.run_ids else None
    job_run_url = state.job_run_url or (
        state.job_run_urls[0] if state.job_run_urls else None
    )
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO transfers
            (transfer_id, status, total, completed, failed,
             catalog, schema_name, volume, run_ids_json,
             job_run_url, catalog_explorer_url, started_at, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transfer_id) DO UPDATE SET
            status=excluded.status,
            total=excluded.total,
            completed=excluded.completed,
            failed=excluded.failed,
            run_ids_json=excluded.run_ids_json,
            job_run_url=excluded.job_run_url,
            started_at=excluded.started_at,
            duration_seconds=excluded.duration_seconds
        """,
        (
            state.transfer_id,
            state.status.value,
            state.total,
            state.completed,
            state.failed,
            state.catalog,
            state.schema_name,
            state.volume,
            run_ids_json,
            job_run_url,
            state.catalog_explorer_url,
            state.started_at,
            state.duration_seconds,
        ),
    )
    conn.commit()


def get(transfer_id: str) -> Optional[TransferSummary]:
    """Load a transfer summary from the DB. Returns None if not found."""
    init_db()
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM transfers WHERE transfer_id = ?", (transfer_id,)
    ).fetchone()
    if not row:
        return None
    return _row_to_summary(row)


def find_by_run_id(run_id: int) -> Optional[TransferSummary]:
    """Find a transfer whose run_ids_json contains the given run_id."""
    init_db()
    conn = _get_conn()
    pattern = f"%{run_id}%"
    rows = conn.execute(
        "SELECT * FROM transfers WHERE run_ids_json LIKE ?", (pattern,)
    ).fetchall()
    for row in rows:
        run_ids = _parse_run_ids(row["run_ids_json"])
        if run_ids and run_id in run_ids:
            return _row_to_summary(row)
    return None


def list_all() -> List[TransferSummary]:
    """Return all transfers, most recent first."""
    init_db()
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM transfers ORDER BY started_at DESC"
    ).fetchall()
    return [_row_to_summary(r) for r in rows]


def _parse_run_ids(raw: Optional[str]) -> Optional[List[int]]:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _row_to_summary(row: sqlite3.Row) -> TransferSummary:
    return TransferSummary(
        transfer_id=row["transfer_id"],
        status=TransferStatus(row["status"]),
        total=row["total"],
        completed=row["completed"],
        failed=row["failed"],
        run_ids=_parse_run_ids(row["run_ids_json"]),
        started_at=row["started_at"],
        duration_seconds=row["duration_seconds"],
        job_run_url=row["job_run_url"],
    )

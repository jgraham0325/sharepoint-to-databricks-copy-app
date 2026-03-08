from typing import List, Optional
from enum import Enum
from pydantic import BaseModel


class TransferStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class FileTransferItem(BaseModel):
    drive_id: str
    item_id: str
    name: str
    size: int = 0
    relative_path: str = ""  # Path prefix under the copied folder (for hierarchy)


class FolderTransferItem(BaseModel):
    """A folder to copy recursively; hierarchy is preserved under folder_name."""
    drive_id: str
    folder_item_id: str
    folder_name: str


class TransferRequest(BaseModel):
    files: List[FileTransferItem]
    catalog: str
    schema_name: str
    volume: str
    subfolder: str = ""
    folders: List[FolderTransferItem] = []  # optional; expanded server-side with hierarchy


class FolderTransferRequest(BaseModel):
    """Request to copy an entire SharePoint folder (recursively) to a volume. Agent-friendly."""
    drive_id: str
    folder_item_id: Optional[str] = None  # None = copy from drive root
    catalog: str
    schema_name: str
    volume: str
    subfolder: str = ""


class FileResult(BaseModel):
    name: str
    status: TransferStatus
    error: Optional[str] = None


class JobRunStatus(BaseModel):
    """Per-job-run status for UI: queued, running, success, or failed."""
    run_id: int
    url: Optional[str] = None
    status: str  # "queued" | "running" | "success" | "failed"
    file_names: List[str] = []
    error: Optional[str] = None


class TaskIterationStatus(BaseModel):
    """Status of one For Each iteration (transfer task). Uses Databricks naming."""
    index: int  # 0-based iteration index
    life_cycle_state: str  # PENDING, QUEUED, RUNNING, TERMINATED, SKIPPED, INTERNAL_ERROR, etc.
    result_state: str = ""  # SUCCESS, FAILED, etc. (when terminated)
    state_message: str = ""


class TransferBatch(BaseModel):
    """One For Each batch: status and the files in that batch (iteration-centric, not run-level)."""
    index: int  # 0-based batch index
    life_cycle_state: str
    result_state: str = ""
    state_message: str = ""
    file_count: int  # len(file_names)
    file_names: List[str] = []  # files in this batch only


class TransferState(BaseModel):
    transfer_id: str
    status: TransferStatus
    total: int
    completed: int
    failed: int
    results: List[FileResult]
    # Destination and link to open in Databricks Catalog Explorer (set when transfer is started)
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    volume: Optional[str] = None
    catalog_explorer_url: Optional[str] = None
    # When set, transfer is offloaded to Databricks job run(s); poll these for completion
    run_ids: Optional[List[int]] = None
    # Map run_id (as str) -> file name or list of file names (batch) for result reporting
    run_id_to_file: Optional[dict] = None
    # Links to Databricks job run UI (set when job runs are submitted; kept for display after completion)
    job_run_urls: Optional[List[str]] = None
    # Single job run URL when using one run with For Each (preferred for display)
    job_run_url: Optional[str] = None
    # Per-job-run status for incremental UI updates (running / success / failed)
    job_run_statuses: Optional[List[JobRunStatus]] = None
    # For Each task iteration statuses (transfer task per manifest); Databricks life_cycle_state / result_state
    task_iterations: Optional[List[TaskIterationStatus]] = None
    # Number of For Each iterations (manifest chunks) for progress estimation; set when job is submitted
    total_iterations: Optional[int] = None
    # File count per batch (same order as task_iterations / manifest_paths); set when job is submitted
    batch_file_counts: Optional[List[int]] = None
    # Batches: status + file list per For Each iteration (canonical for iteration-level status; built for response)
    batches: Optional[List[TransferBatch]] = None
    # Timing: set when transfer starts and when it finishes
    started_at: Optional[float] = None  # Unix timestamp
    duration_seconds: Optional[float] = None  # Elapsed time when finished
    # True when results were capped (e.g. only first N kept in memory)
    results_truncated: bool = False

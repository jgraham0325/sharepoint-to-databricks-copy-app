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

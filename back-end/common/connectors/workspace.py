from typing import List

import io
from databricks.sdk import WorkspaceClient

from common.logger import get_logger

logger = get_logger(__name__)


def upload_to_volume(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    volume: str,
    file_path: str,
    data: bytes,
) -> str:
    """Upload a file to a Unity Catalog Volume. Returns the full volume path."""
    full_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_path}"
    logger.info("Uploading %s (%d bytes)", full_path, len(data))
    client.files.upload(full_path, io.BytesIO(data), overwrite=True)
    return full_path


def list_volume_contents(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    volume: str,
    path: str = "",
) -> List[dict]:
    """List files/directories at a path inside a Volume."""
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    full_path = f"{base}/{path}" if path else base
    items = []
    for f in client.files.list_directory_contents(full_path):
        items.append(
            {
                "name": f.name,
                "path": f.path,
                "is_directory": f.is_directory,
                "file_size": f.file_size,
            }
        )
    return items

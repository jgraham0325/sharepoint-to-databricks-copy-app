from typing import Optional
from databricks.sdk import WorkspaceClient
from common.logger import get_logger

logger = get_logger(__name__)

_client: Optional[WorkspaceClient] = None


def get_workspace_client() -> WorkspaceClient:
    """Return a singleton WorkspaceClient.

    In Databricks Apps the SDK auto-detects credentials from injected env vars.
    For local dev, DATABRICKS_HOST and DATABRICKS_TOKEN must be set.
    """
    global _client
    if _client is None:
        logger.info("Initialising WorkspaceClient")
        _client = WorkspaceClient()
    return _client

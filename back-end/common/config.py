import os
from dotenv import load_dotenv

load_dotenv()

MS_CLIENT_ID: str = os.environ.get("MS_CLIENT_ID", "")
MS_CLIENT_SECRET: str = os.environ.get("MS_CLIENT_SECRET", "")
MS_TENANT_ID: str = os.environ.get("MS_TENANT_ID", "")

DATABRICKS_HOST: str = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN: str = os.environ.get("DATABRICKS_TOKEN", "")
# Model/endpoint for agent chat (Foundation Model API). Example: databricks-meta-llama-3-3-70b-instruct
DATABRICKS_CHAT_MODEL: str = os.environ.get(
    "DATABRICKS_CHAT_MODEL", "databricks-meta-llama-3-3-70b-instruct"
)

APP_URL: str = os.environ.get("APP_URL", "http://localhost:8000")

# Files larger than this (bytes) are sent to the Databricks job; below this they may run on app server if batch is small
LARGE_FILE_THRESHOLD_BYTES: int = int(os.environ.get("LARGE_FILE_THRESHOLD_BYTES", "104857600"))  # 100 MiB default
# If transfer has fewer than this many files and all are small, run on app server to avoid job startup time
MAX_FILES_ON_SERVER: int = int(os.environ.get("MAX_FILES_ON_SERVER", "20"))
# Files per manifest chunk when using the job (one job run per chunk)
FILES_PER_MANIFEST_CHUNK: int = int(os.environ.get("FILES_PER_MANIFEST_CHUNK", "50"))
# Max result entries kept in memory and returned by GET transfer (avoids huge payloads)
MAX_TRANSFER_RESULTS_IN_MEMORY: int = int(os.environ.get("MAX_TRANSFER_RESULTS_IN_MEMORY", "500"))
# Optional: job ID of the sharepoint-transfer job (from bundle). If unset, resolved by job name.
_raw = os.environ.get("SHAREPOINT_TRANSFER_JOB_ID", "")
SHAREPOINT_TRANSFER_JOB_ID: int = int(_raw) if _raw.isdigit() else 0

MS_AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
MS_SCOPES = ["https://graph.microsoft.com/.default"]
MS_REDIRECT_PATH = "/api/v1/auth/callback"
MS_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

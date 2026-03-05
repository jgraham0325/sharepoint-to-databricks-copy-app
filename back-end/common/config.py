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

MS_AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
MS_SCOPES = ["https://graph.microsoft.com/.default"]
MS_REDIRECT_PATH = "/api/v1/auth/callback"
MS_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

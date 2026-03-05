from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routes.v1 import healthcheck, auth, sharepoint, transfer, volumes, agent

app = FastAPI(title="SharePoint Upload App")

# --- API routes ---
prefix = "/api/v1"
app.include_router(healthcheck.router, prefix=prefix)
app.include_router(auth.router, prefix=prefix)
app.include_router(sharepoint.router, prefix=prefix)
app.include_router(transfer.router, prefix=prefix)
app.include_router(volumes.router, prefix=prefix)
app.include_router(agent.router, prefix=prefix)

# --- Static files (React build) ---
static_dir = Path(__file__).parent / "static"
if static_dir.exists() and (static_dir / "index.html").exists():
    app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the React SPA for any non-API route."""
        file = static_dir / full_path
        if file.is_file():
            return FileResponse(file)
        return FileResponse(static_dir / "index.html")

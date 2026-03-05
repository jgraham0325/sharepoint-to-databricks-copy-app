from __future__ import annotations

from fastapi import APIRouter, HTTPException

from common.authentication.workspace import get_workspace_client
from common.connectors.workspace import list_volume_contents
from common.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/volumes")


@router.get("/catalogs")
async def catalogs():
    """List catalogs the service principal can see."""
    ws = get_workspace_client()
    items = []
    for c in ws.catalogs.list():
        items.append({"name": c.name})
    return items


@router.get("/catalogs/{catalog}/schemas")
async def schemas(catalog: str):
    ws = get_workspace_client()
    items = []
    for s in ws.schemas.list(catalog_name=catalog):
        items.append({"name": s.name})
    return items


@router.get("/catalogs/{catalog}/schemas/{schema}/volumes")
async def volumes(catalog: str, schema: str):
    ws = get_workspace_client()
    items = []
    for v in ws.volumes.list(catalog_name=catalog, schema_name=schema):
        items.append({"name": v.name, "volume_type": v.volume_type.value if v.volume_type else None})
    return items


@router.get("/browse/{catalog}/{schema}/{volume}")
async def browse(catalog: str, schema: str, volume: str, path: str = ""):
    try:
        ws = get_workspace_client()
        items = list_volume_contents(ws, catalog, schema, volume, path)
        return items
    except Exception as exc:
        logger.error("Error listing volume contents: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))

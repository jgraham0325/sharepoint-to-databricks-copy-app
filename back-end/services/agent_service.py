"""
Agent service: runs a chat loop with Databricks Foundation Model API and tools
to browse SharePoint, copy folders to volumes, and monitor transfers.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from openai import OpenAI

from common import config
from common.logger import get_logger
from services import sharepoint_service
from services import transfer_service

logger = get_logger(__name__)

# OpenAI-compatible base URL for Databricks Foundation Model API (see Databricks docs)
def _client() -> OpenAI:
    base = (config.DATABRICKS_HOST or "").rstrip("/")
    if not base or not config.DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set for the agent")
    base_url = f"{base}/serving-endpoints/"
    return OpenAI(
        api_key=config.DATABRICKS_TOKEN,
        base_url=base_url,
    )


AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_sites",
            "description": "Search or list SharePoint sites the user can access. Use query to filter by name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (optional). Use empty string to list all."},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_drives",
            "description": "List document libraries (drives) for a SharePoint site.",
            "parameters": {
                "type": "object",
                "properties": {
                    "site_id": {"type": "string", "description": "SharePoint site ID from list_sites."},
                },
                "required": ["site_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_children",
            "description": "List files and folders inside a drive or a folder. Use item_id to list a specific folder; omit for drive root.",
            "parameters": {
                "type": "object",
                "properties": {
                    "drive_id": {"type": "string", "description": "Drive ID from list_drives."},
                    "item_id": {"type": "string", "description": "Folder item ID (optional). Omit to list drive root."},
                },
                "required": ["drive_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "copy_folder_to_volume",
            "description": "Copy an entire SharePoint folder (and all files inside it, recursively) to a Unity Catalog volume. Use folder_item_id for a specific folder, or omit to copy the whole drive root.",
            "parameters": {
                "type": "object",
                "properties": {
                    "drive_id": {"type": "string", "description": "Drive ID from list_drives."},
                    "folder_item_id": {"type": "string", "description": "Folder ID from list_children (optional). Omit to copy from drive root."},
                    "catalog": {"type": "string", "description": "Unity Catalog catalog name."},
                    "schema_name": {"type": "string", "description": "Schema name."},
                    "volume": {"type": "string", "description": "Volume name."},
                    "subfolder": {"type": "string", "description": "Path inside the volume (optional). e.g. 'imports/2025'."},
                },
                "required": ["drive_id", "catalog", "schema_name", "volume"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_transfers",
            "description": "List all transfers (running, completed, failed, pending). Returns a summary of each transfer including status, progress, timing, and Databricks job run URL. Use this when the user asks about running transfers, recent transfers, or transfer history.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_transfer_status",
            "description": "Get detailed status for a specific transfer by its transfer_id, including per-file results. Use list_transfers first to find transfer IDs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "transfer_id": {"type": "string", "description": "Transfer ID from list_transfers or copy_folder_to_volume."},
                },
                "required": ["transfer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_catalogs",
            "description": "List Unity Catalog catalogs (for choosing destination volume).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_schemas",
            "description": "List schemas in a catalog.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "description": "Catalog name from list_catalogs."},
                },
                "required": ["catalog"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_volumes",
            "description": "List volumes in a catalog and schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "description": "Catalog name."},
                    "schema_name": {"type": "string", "description": "Schema name from list_schemas."},
                },
                "required": ["catalog", "schema_name"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are an assistant that helps users transfer files from SharePoint to Databricks Unity Catalog volumes, and monitor those transfers.

**Tools available:**

Browse SharePoint:
- list_sites: find SharePoint sites (use query to search by name)
- list_drives: get document libraries for a site
- list_children: list files/folders in a drive or folder (use item_id for a subfolder)

Transfer files:
- copy_folder_to_volume: copy an entire SharePoint folder (recursively) to a volume

Monitor transfers:
- list_transfers: list ALL transfers with status, progress, timing, and job URLs. Use this when the user asks about running transfers, recent transfers, or wants an overview.
- get_transfer_status: get detailed status for a specific transfer including per-file results. Use list_transfers first to find transfer IDs if the user hasn't provided one.

Discover Databricks destinations:
- list_catalogs, list_schemas, list_volumes: browse Unity Catalog to find destination volumes

**Workflows:**

When the user asks to copy something:
1. Find the site (list_sites), then list_drives for that site.
2. If they named a folder, use list_children to find that folder's ID.
3. Find the destination volume using list_catalogs → list_schemas → list_volumes, or use what the user specified.
4. Call copy_folder_to_volume.
5. Report what was started (transfer_id, file count).

When the user asks about transfers (e.g. "what's running?", "show my transfers", "any transfers still going?"):
1. Call list_transfers to get all transfers.
2. Summarize clearly: group by status (in_progress, completed, failed), show progress and timing.
3. If the user wants details on a specific transfer, use get_transfer_status.

Respond in clear, short sentences. Use markdown formatting for readability (tables, bold, bullet points)."""


async def _run_tool(name: str, arguments: dict, ms_token: str) -> str:
    """Execute one tool and return a JSON string result."""
    try:
        if name == "list_sites":
            query = arguments.get("query", "") or ""
            sites = await sharepoint_service.search_sites(ms_token, query)
            return json.dumps([{"id": s.id, "name": s.name, "display_name": s.display_name} for s in sites])

        if name == "list_drives":
            site_id = arguments["site_id"]
            drives = await sharepoint_service.list_drives(ms_token, site_id)
            return json.dumps([{"id": d.id, "name": d.name} for d in drives])

        if name == "list_children":
            drive_id = arguments["drive_id"]
            item_id = arguments.get("item_id") or None
            items = await sharepoint_service.list_children(ms_token, drive_id, item_id)
            return json.dumps([
                {"id": i.id, "name": i.name, "is_folder": i.is_folder, "size": i.size}
                for i in items
            ])

        if name == "copy_folder_to_volume":
            drive_id = arguments["drive_id"]
            folder_item_id = arguments.get("folder_item_id") or None
            catalog = arguments["catalog"]
            schema_name = arguments["schema_name"]
            volume = arguments["volume"]
            subfolder = arguments.get("subfolder") or ""
            state = await transfer_service.start_folder_transfer(
                drive_id=drive_id,
                folder_item_id=folder_item_id,
                catalog=catalog,
                schema_name=schema_name,
                volume=volume,
                subfolder=subfolder,
                ms_token=ms_token,
            )
            return json.dumps({
                "transfer_id": state.transfer_id,
                "status": state.status.value,
                "total": state.total,
                "message": f"Copy started. {state.total} file(s). Poll get_transfer_status with transfer_id to check progress.",
            })

        if name == "list_transfers":
            from datetime import datetime, timezone
            summaries = transfer_service.list_transfers()
            return json.dumps([
                {
                    "transfer_id": s.transfer_id,
                    "status": s.status.value,
                    "total": s.total,
                    "completed": s.completed,
                    "failed": s.failed,
                    "started_at": datetime.fromtimestamp(s.started_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if s.started_at else None,
                    "duration_seconds": round(s.duration_seconds, 1) if s.duration_seconds else None,
                    "job_run_url": s.job_run_url,
                }
                for s in summaries
            ])

        if name == "get_transfer_status":
            transfer_id = arguments["transfer_id"]
            state = transfer_service.get_transfer(transfer_id)
            if state is None:
                return json.dumps({"error": "Transfer not found"})
            return json.dumps({
                "transfer_id": state.transfer_id,
                "status": state.status.value,
                "total": state.total,
                "completed": state.completed,
                "failed": state.failed,
                "catalog": state.catalog,
                "schema_name": state.schema_name,
                "volume": state.volume,
                "results": [{"name": r.name, "status": r.status.value, "error": r.error} for r in state.results],
            })

        if name == "list_catalogs":
            from common.authentication.workspace import get_workspace_client
            ws = get_workspace_client()
            items = [{"name": c.name} for c in ws.catalogs.list()]
            return json.dumps(items)

        if name == "list_schemas":
            from common.authentication.workspace import get_workspace_client
            ws = get_workspace_client()
            catalog = arguments["catalog"]
            items = [{"name": s.name} for s in ws.schemas.list(catalog_name=catalog)]
            return json.dumps(items)

        if name == "list_volumes":
            from common.authentication.workspace import get_workspace_client
            ws = get_workspace_client()
            catalog = arguments["catalog"]
            schema_name = arguments["schema_name"]
            items = [{"name": v.name} for v in ws.volumes.list(catalog_name=catalog, schema_name=schema_name)]
            return json.dumps(items)

        return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as e:
        logger.exception("Tool %s failed: %s", name, e)
        return json.dumps({"error": str(e)})


def _messages_for_api(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert to OpenAI API format (tool_calls and tool role)."""
    out = []
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if role == "assistant" and m.get("tool_calls"):
            out.append({
                "role": "assistant",
                "content": content or None,
                "tool_calls": [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {"name": tc["name"], "arguments": tc.get("arguments", "{}")},
                    }
                    for tc in m["tool_calls"]
                ],
            })
        elif role == "tool":
            out.append({"role": "tool", "tool_call_id": m["tool_call_id"], "content": m["content"]})
        else:
            out.append({"role": role, "content": content or ""})
    return out


async def chat(
    user_message: str,
    history: list[dict[str, Any]],
    ms_token: str,
    max_tool_rounds: int = 10,
) -> str:
    """
    Run one agent turn: send user message + history to the model with tools,
    execute any tool calls, repeat until the model returns a final reply.
    Returns the assistant's final text.
    """
    client = _client()
    messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": h["role"], "content": h.get("content", "")})
    messages.append({"role": "user", "content": user_message})

    for _ in range(max_tool_rounds):
        api_messages = _messages_for_api(messages)
        response = client.chat.completions.create(
            model=config.DATABRICKS_CHAT_MODEL,
            messages=api_messages,
            tools=AGENT_TOOLS,
            tool_choice="auto",
            max_tokens=1024,
        )
        choice = response.choices[0] if response.choices else None
        if not choice or not choice.message:
            return "No response from the model."

        msg = choice.message
        if not getattr(msg, "tool_calls", None) or len(msg.tool_calls) == 0:
            return (msg.content or "").strip()

        # Append assistant message with tool_calls
        tool_calls = [
            {"id": tc.id, "name": tc.function.name, "arguments": tc.function.arguments or "{}"}
            for tc in msg.tool_calls
        ]
        messages.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": tool_calls,
        })

        # Run each tool and append tool results
        for tc in msg.tool_calls:
            name = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            result = await _run_tool(name, args, ms_token)
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    return "Reached maximum tool rounds. Please try a shorter request."

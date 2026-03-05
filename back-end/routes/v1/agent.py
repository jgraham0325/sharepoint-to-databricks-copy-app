from typing import Optional

from fastapi import APIRouter, HTTPException, Header

from models.agent import AgentChatRequest, AgentChatResponse
from services.agent_service import chat

router = APIRouter(prefix="/agent")


@router.post("/chat", response_model=AgentChatResponse)
async def agent_chat(body: AgentChatRequest, x_ms_token: Optional[str] = Header(None)):
    """
    Send a message to the agent. The agent can list SharePoint sites/drives/folders,
    list Unity Catalog volumes, and copy a folder from SharePoint to a volume.
    Requires X-MS-Token (Microsoft access token) for SharePoint access.
    """
    if not x_ms_token:
        raise HTTPException(status_code=401, detail="Missing X-MS-Token header")
    history = []
    if body.history:
        history = [{"role": m.role, "content": m.content} for m in body.history]
    try:
        reply = await chat(
            user_message=body.message,
            history=history,
            ms_token=x_ms_token,
        )
        return AgentChatResponse(reply=reply)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}") from e

from typing import Any, List, Optional
from pydantic import BaseModel


class AgentMessage(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class AgentChatRequest(BaseModel):
    message: str
    history: Optional[List[AgentMessage]] = None


class AgentChatResponse(BaseModel):
    reply: str

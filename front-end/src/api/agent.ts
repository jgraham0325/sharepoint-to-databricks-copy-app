import { apiFetch } from "./client";

export interface AgentMessage {
  role: "user" | "assistant";
  content: string;
}

export interface AgentChatRequest {
  message: string;
  history?: AgentMessage[];
}

export interface AgentChatResponse {
  reply: string;
}

export function agentChat(req: AgentChatRequest): Promise<AgentChatResponse> {
  return apiFetch("/api/v1/agent/chat", {
    method: "POST",
    body: JSON.stringify(req),
  });
}

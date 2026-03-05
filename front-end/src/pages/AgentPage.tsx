import { useState, useRef, useEffect } from "react";
import { Container, Card, Form, Button } from "react-bootstrap";
import { Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useAuth } from "../context/AuthContext";
import LoginButton from "../components/auth/LoginButton";
import { agentChat, type AgentMessage } from "../api/agent";
import toast from "react-hot-toast";

export default function AgentPage() {
  const { isAuthenticated } = useAuth();
  const [messages, setMessages] = useState<AgentMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  useEffect(() => scrollToBottom(), [messages]);

  const send = async () => {
    const text = input.trim();
    if (!text || loading || !isAuthenticated) return;
    setInput("");
    setMessages((prev) => [...prev, { role: "user", content: text }]);
    setLoading(true);
    try {
      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const res = await agentChat({ message: text, history });
      setMessages((prev) => [...prev, { role: "assistant", content: res.reply }]);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Agent request failed");
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Sorry, something went wrong. Please try again." },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Container className="py-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h4 className="mb-0">SharePoint → Volume Agent</h4>
        <div className="d-flex align-items-center gap-2">
          <Link to="/" className="btn btn-outline-secondary btn-sm">
            Browse
          </Link>
          <Link to="/transfer" className="btn btn-outline-secondary btn-sm">
            Transfer
          </Link>
          <LoginButton />
        </div>
      </div>

      {!isAuthenticated ? (
        <Card body className="text-center py-5">
          <i className="bi bi-robot" style={{ fontSize: "3rem" }}></i>
          <p className="mt-3">
            Sign in with Microsoft to use the agent. You can ask it to copy folders from SharePoint into a Databricks volume.
          </p>
          <LoginButton />
        </Card>
      ) : (
        <Card body className="p-0 overflow-hidden">
          <div
            className="p-3 overflow-auto"
            style={{ minHeight: "320px", maxHeight: "60vh" }}
          >
            {messages.length === 0 && (
              <p className="text-muted small mb-0">
                Ask in plain language, e.g.: “Copy the Reports folder from the Marketing site to catalog main, schema my_schema, volume sharepoint_imports” or “List my SharePoint sites” or “What volumes do I have?”
              </p>
            )}
            {messages.map((m, i) => (
              <div
                key={i}
                className={`d-flex mb-3 ${m.role === "user" ? "justify-content-end" : ""}`}
              >
                <div
                  className={`rounded p-2 px-3 ${
                    m.role === "user"
                      ? "bg-primary text-white"
                      : "bg-light border"
                  }`}
                  style={{ maxWidth: "85%" }}
                >
                  {m.role === "assistant" && (
                    <i className="bi bi-robot me-2 text-secondary small"></i>
                  )}
                  {m.role === "assistant" ? (
                    <div className="agent-markdown">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>
                        {m.content}
                      </ReactMarkdown>
                    </div>
                  ) : (
                    <span style={{ whiteSpace: "pre-wrap" }}>{m.content}</span>
                  )}
                </div>
              </div>
            ))}
            {loading && (
              <div className="d-flex mb-3">
                <div className="bg-light border rounded p-2 px-3">
                  <span className="spinner-border spinner-border-sm me-2"></span>
                  Thinking...
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
          <div className="p-3 border-top bg-light">
            <Form
              onSubmit={(e) => {
                e.preventDefault();
                send();
              }}
              className="d-flex gap-2"
            >
              <Form.Control
                placeholder="Ask to copy a folder or list sites/volumes..."
                value={input}
                onChange={(e) => setInput(e.target.value)}
                disabled={loading}
              />
              <Button type="submit" variant="primary" disabled={loading || !input.trim()}>
                Send
              </Button>
            </Form>
          </div>
        </Card>
      )}
    </Container>
  );
}

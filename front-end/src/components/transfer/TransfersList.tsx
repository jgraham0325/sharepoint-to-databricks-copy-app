import { useState, useEffect, useCallback } from "react";
import { Card, Table, Badge, Button, Spinner } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { listTransfers, type TransferSummary, type TransferStatus } from "../../api/transfer";

const LIST_POLL_INTERVAL_MS = 4000;

function statusBadge(status: TransferStatus) {
  switch (status) {
    case "completed":
      return <Badge bg="success">Completed</Badge>;
    case "failed":
      return <Badge bg="danger">Failed</Badge>;
    case "in_progress":
      return <Badge bg="primary">In progress</Badge>;
    default:
      return <Badge bg="secondary">Pending</Badge>;
  }
}

function formatTime(ts: number | null | undefined): string {
  if (ts == null) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleString();
}

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null) return "—";
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

interface Props {
  onStartTransfer?: () => void;
}

export default function TransfersList({ onStartTransfer }: Props) {
  const [summaries, setSummaries] = useState<TransferSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const fetchList = useCallback(async () => {
    try {
      const data = await listTransfers();
      setSummaries(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load transfers");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchList();
  }, [fetchList]);

  useEffect(() => {
    const interval = setInterval(fetchList, LIST_POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchList]);

  const openDetail = (transferId: string) => {
    navigate(`/transfer?transfer_id=${encodeURIComponent(transferId)}`);
  };

  if (loading) {
    return (
      <Card body className="text-center py-5">
        <Spinner animation="border" size="sm" className="me-2" />
        Loading transfers…
      </Card>
    );
  }

  if (error) {
    return (
      <Card body className="border-danger">
        <p className="text-danger mb-0">{error}</p>
      </Card>
    );
  }

  return (
    <Card body>
      {summaries.length === 0 ? (
        <div className="text-center py-4">
          <i className="bi bi-arrow-left-right text-muted" style={{ fontSize: "2rem" }}></i>
          <p className="text-muted mt-2 mb-2">No transfers yet.</p>
          {onStartTransfer && (
            <Button variant="primary" size="sm" onClick={onStartTransfer}>
              <i className="bi bi-plus-lg me-1"></i>
              Browse files to start your first transfer
            </Button>
          )}
        </div>
      ) : (
        <Table responsive size="sm" className="mb-0">
          <thead>
            <tr>
              <th>ID</th>
              <th>Status</th>
              <th>Progress</th>
              <th>Started</th>
              <th>Duration</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {summaries.map((s) => (
              <tr
                key={s.transfer_id}
                style={{ cursor: "pointer" }}
                onClick={() => openDetail(s.transfer_id)}
              >
                <td>
                  <code className="small">{s.transfer_id}</code>
                </td>
                <td>{statusBadge(s.status)}</td>
                <td>
                  {s.completed + s.failed}/{s.total}
                  {s.failed > 0 && (
                    <span className="text-danger ms-1">({s.failed} failed)</span>
                  )}
                </td>
                <td className="text-muted small">{formatTime(s.started_at)}</td>
                <td className="text-muted small">
                  {s.duration_seconds != null
                    ? formatDuration(s.duration_seconds)
                    : s.status === "in_progress"
                      ? "…"
                      : "—"}
                </td>
                <td onClick={(e) => e.stopPropagation()}>
                  {s.job_run_url && (
                    <a
                      href={s.job_run_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="small"
                    >
                      Job run
                    </a>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </Card>
  );
}

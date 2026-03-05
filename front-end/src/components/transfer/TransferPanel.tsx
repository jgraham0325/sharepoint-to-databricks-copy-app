import { useState } from "react";
import { ProgressBar, ListGroup, Badge, Alert, Button } from "react-bootstrap";
import type { TransferState } from "../../api/transfer";

interface Props {
  state: TransferState | null;
}

function statusBadge(status: string) {
  switch (status) {
    case "completed":
      return <Badge bg="success">Completed</Badge>;
    case "failed":
      return <Badge bg="danger">Failed</Badge>;
    case "in_progress":
      return <Badge bg="primary">In Progress</Badge>;
    default:
      return <Badge bg="secondary">Pending</Badge>;
  }
}

/** Dummy transform & analyse for demo: simulates a short processing step. */
function runDummyTransformAndAnalyse(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 2200));
}

export default function TransferPanel({ state }: Props) {
  const [isAnalysing, setIsAnalysing] = useState(false);
  const [analysisComplete, setAnalysisComplete] = useState(false);

  if (!state) return null;

  const pct =
    state.total > 0
      ? Math.round(((state.completed + state.failed) / state.total) * 100)
      : 0;

  const transferFinished =
    state.status === "completed" || state.status === "failed";
  const hasCompletedFiles = state.completed > 0;

  const handleTransformAndAnalyse = async () => {
    setIsAnalysing(true);
    setAnalysisComplete(false);
    try {
      await runDummyTransformAndAnalyse();
      setAnalysisComplete(true);
    } finally {
      setIsAnalysing(false);
    }
  };

  return (
    <div className="mt-3">
      <div className="d-flex justify-content-between align-items-center mb-2">
        <h6 className="mb-0">Transfer Progress</h6>
        {statusBadge(state.status)}
      </div>

      <ProgressBar
        now={pct}
        label={`${pct}%`}
        variant={state.failed > 0 ? "warning" : "primary"}
        className="mb-3"
      />

      {state.failed > 0 && (
        <Alert variant="warning" className="py-2">
          {state.failed} file(s) failed to transfer
        </Alert>
      )}

      {state.results.length > 0 && (
        <ListGroup>
          {state.results.map((r, i) => (
            <ListGroup.Item
              key={i}
              className="d-flex justify-content-between align-items-center py-2"
            >
              <span>
                <i
                  className={`bi ${r.status === "completed" ? "bi-check-circle text-success" : "bi-x-circle text-danger"} me-2`}
                ></i>
                {r.name}
              </span>
              {r.error && <small className="text-danger">{r.error}</small>}
            </ListGroup.Item>
          ))}
        </ListGroup>
      )}

      {transferFinished && hasCompletedFiles && (
        <div className="mt-3">
          <Button
            variant="primary"
            className="w-100"
            disabled={isAnalysing}
            onClick={handleTransformAndAnalyse}
          >
            {isAnalysing ? (
              <>
                <span className="spinner-border spinner-border-sm me-2"></span>
                Transforming & analysing...
              </>
            ) : (
              <>
                <i className="bi bi-gear-wide-connected me-2"></i>
                Transform & Analyse
              </>
            )}
          </Button>
          {analysisComplete && (
            <Alert variant="info" className="mt-2 py-2 mb-0">
              <i className="bi bi-check-circle me-2"></i>
              Analysis complete (demo). In production this would run your
              pipeline or notebook.
            </Alert>
          )}
        </div>
      )}

      {(state.status === "completed" || state.status === "failed") &&
        state.catalog_explorer_url && (
          <Alert variant="success" className="mt-3 py-2 d-flex align-items-center">
            <i className="bi bi-box-arrow-up-right me-2"></i>
            <span className="me-2">Open destination in Databricks:</span>
            <a
              href={state.catalog_explorer_url}
              target="_blank"
              rel="noopener noreferrer"
              className="alert-link"
            >
              Catalog Explorer
            </a>
          </Alert>
        )}
    </div>
  );
}

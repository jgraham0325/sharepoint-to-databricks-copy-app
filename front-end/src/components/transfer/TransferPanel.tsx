import { useState, useMemo, useRef, useEffect } from "react";
import { ProgressBar, ListGroup, Badge, Alert, Button } from "react-bootstrap";
import type { TransferState, JobRunStatus, FileResult, TaskIterationStatus, TransferBatch } from "../../api/transfer";

const INITIAL_VISIBLE_FILES = 10;
const MAX_VISIBLE_FILES_EXPANDED = 500;

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

function JobRunStatusBadge({
  status,
  className,
}: {
  status: "queued" | "running" | "success" | "failed";
  className?: string;
}) {
  const badge = (() => {
    switch (status) {
      case "success":
        return <Badge bg="success">Success</Badge>;
      case "failed":
        return <Badge bg="danger">Failed</Badge>;
      case "queued":
        return <Badge bg="secondary">Queued</Badge>;
      case "running":
        return <Badge bg="primary">Running</Badge>;
      default:
        return <Badge bg="secondary">{status}</Badge>;
    }
  })();
  return className ? <span className={className}>{badge}</span> : badge;
}

/** Databricks naming: life_cycle_state (PENDING, QUEUED, RUNNING, TERMINATED, …) and result_state (SUCCESS, FAILED). */
function TaskIterationStatusBadge({ iteration }: { iteration: TaskIterationStatus }) {
  const { life_cycle_state, result_state } = iteration;
  const terminal = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"].includes(life_cycle_state);
  const display = terminal && result_state ? result_state : life_cycle_state;
  const variant =
    display === "SUCCESS"
      ? "success"
      : display === "FAILED" || display === "INTERNAL_ERROR"
        ? "danger"
        : display === "RUNNING" || display === "TERMINATING"
          ? "primary"
          : "secondary";
  return <Badge bg={variant}>{display}</Badge>;
}

/** Badge for a batch (same semantics as TaskIterationStatus). */
function BatchStatusBadge({ batch }: { batch: TransferBatch }) {
  return (
    <TaskIterationStatusBadge
      iteration={{
        index: batch.index,
        life_cycle_state: batch.life_cycle_state,
        result_state: batch.result_state,
        state_message: batch.state_message,
      }}
    />
  );
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

/** Dummy transform & analyse for demo: simulates a short processing step. */
function runDummyTransformAndAnalyse(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 2200));
}

function FileResultRow({ r }: { r: FileResult }) {
  const isError = r.status === "failed" || r.error;
  return (
    <ListGroup.Item
      className={`d-flex justify-content-between align-items-center py-2 ${isError ? "list-group-item-danger" : ""}`}
    >
      <span>
        <i
          className={`bi ${r.status === "completed" ? "bi-check-circle text-success" : "bi-x-circle text-danger"} me-2`}
        ></i>
        {r.name}
      </span>
      {r.error && <small className="text-danger fw-medium">{r.error}</small>}
    </ListGroup.Item>
  );
}

export default function TransferPanel({ state }: Props) {
  const [isAnalysing, setIsAnalysing] = useState(false);
  const [analysisComplete, setAnalysisComplete] = useState(false);
  const [resultsExpanded, setResultsExpanded] = useState(false);
  const [failedExpanded, setFailedExpanded] = useState(false);
  const [fileNamesExpanded, setFileNamesExpanded] = useState(false);
  const [, setTick] = useState(0);
  const startTimeRef = useRef<{ transferId: string; startTime: number } | null>(null);

  const inProgress = state?.status === "in_progress";
  useEffect(() => {
    if (!inProgress || !state?.transfer_id) return;
    if (startTimeRef.current?.transferId !== state.transfer_id) {
      startTimeRef.current = { transferId: state.transfer_id, startTime: Date.now() };
    }
    const interval = setInterval(() => setTick((t) => t + 1), 200);
    return () => clearInterval(interval);
  }, [inProgress, state?.transfer_id]);

  const { failedResults, successResults, fileNamesFromBatchesOrJob } = useMemo(() => {
    const results = state?.results ?? [];
    const failed = results.filter((r) => r.status === "failed" || r.error);
    const success = results.filter((r) => r.status !== "failed" && !r.error);
    const fileNames =
      (state?.batches?.length
        ? state.batches.flatMap((b) => b.file_names)
        : state?.job_run_statuses?.[0]?.file_names) ?? [];
    return { failedResults: failed, successResults: success, fileNamesFromBatchesOrJob: fileNames };
  }, [state?.results, state?.batches, state?.job_run_statuses]);

  const visibleFailedCount = failedExpanded
    ? Math.min(MAX_VISIBLE_FILES_EXPANDED, failedResults.length)
    : Math.min(INITIAL_VISIBLE_FILES, failedResults.length);
  const canExpandOrCollapseFailed = failedResults.length > INITIAL_VISIBLE_FILES;

  const visibleSuccessCount = resultsExpanded
    ? Math.min(MAX_VISIBLE_FILES_EXPANDED, successResults.length)
    : Math.min(INITIAL_VISIBLE_FILES, successResults.length);
  const canExpandOrCollapse = successResults.length > INITIAL_VISIBLE_FILES;

  const visibleFileNamesCount = fileNamesExpanded
    ? Math.min(MAX_VISIBLE_FILES_EXPANDED, fileNamesFromBatchesOrJob.length)
    : Math.min(INITIAL_VISIBLE_FILES, fileNamesFromBatchesOrJob.length);
  const canExpandOrCollapseFileNames = fileNamesFromBatchesOrJob.length > INITIAL_VISIBLE_FILES;
  const moreFileNamesCount = Math.min(MAX_VISIBLE_FILES_EXPANDED, fileNamesFromBatchesOrJob.length) - visibleFileNamesCount;
  const moreSuccessCount = Math.min(MAX_VISIBLE_FILES_EXPANDED, successResults.length) - visibleSuccessCount;
  const moreFailedCount = Math.min(MAX_VISIBLE_FILES_EXPANDED, failedResults.length) - visibleFailedCount;

  if (!state) return null;

  const pct =
    state.total > 0
      ? Math.round(((state.completed + state.failed) / state.total) * 100)
      : 0;

  const transferFinished =
    state.status === "completed" || state.status === "failed";
  const hasCompletedFiles = state.completed > 0;

  const elapsedSeconds =
    transferFinished && state.duration_seconds != null
      ? state.duration_seconds
      : inProgress && startTimeRef.current?.transferId === state.transfer_id
        ? (Date.now() - startTimeRef.current.startTime) / 1000
        : null;

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
        <div className="d-flex align-items-center gap-2">
          {elapsedSeconds != null && (
            <span className="text-muted small">
              <i className="bi bi-clock me-1"></i>
              {formatDuration(elapsedSeconds)}
            </span>
          )}
          {statusBadge(state.status)}
        </div>
      </div>

      <div className="d-flex align-items-center gap-2 mb-3">
        <ProgressBar
          now={pct}
          variant={state.failed > 0 ? "warning" : "primary"}
          className="flex-grow-1"
          style={{ minWidth: 0 }}
        />
        <span className="text-body-emphasis fw-medium" style={{ minWidth: "3rem" }}>
          {pct}%
        </span>
      </div>

      {state.total > 0 && (
        <p className="text-muted small mb-2">
          {state.completed} completed, {state.failed} failed
          {state.total > state.completed + state.failed && (
            <span>, {state.total - state.completed - state.failed} in progress</span>
          )}
        </p>
      )}

      {state.failed > 0 && (
        <Alert variant="warning" className="py-2">
          {state.failed} file(s) failed to transfer
        </Alert>
      )}

      {(state.results.length > 0 || fileNamesFromBatchesOrJob.length > 0) && (
        <>
          {failedResults.length > 0 && (
            <div className="mb-2">
              <p className="text-danger small fw-medium mb-1">
                <i className="bi bi-exclamation-triangle me-1"></i>
                Failed ({failedResults.length})
              </p>
              <ListGroup className="mb-1">
                {failedResults
                  .slice(0, visibleFailedCount)
                  .map((r, i) => (
                    <FileResultRow key={`failed-${i}`} r={r} />
                  ))}
              </ListGroup>
              {canExpandOrCollapseFailed && (
                <Button
                  variant="outline-danger"
                  size="sm"
                  className="mt-1"
                  onClick={() => setFailedExpanded((e) => !e)}
                >
                  {failedExpanded
                    ? "Show less"
                    : `Show ${moreFailedCount} more`}
                </Button>
              )}
            </div>
          )}
          {successResults.length > 0 && (
            <div className="mb-1">
              <p className="text-muted small mb-1">
                {failedResults.length > 0 ? "Succeeded" : "Files"}{" "}
                ({successResults.length})
              </p>
              <ListGroup className="mb-1">
                {successResults
                  .slice(0, visibleSuccessCount)
                  .map((r, i) => (
                    <FileResultRow key={`success-${i}`} r={r} />
                  ))}
              </ListGroup>
              {canExpandOrCollapse && (
                <Button
                  variant="outline-secondary"
                  size="sm"
                  className="mt-1"
                  onClick={() => setResultsExpanded((e) => !e)}
                >
                  {resultsExpanded
                    ? "Show less"
                    : `Show ${moreSuccessCount} more`}
                </Button>
              )}
            </div>
          )}
          {state.results.length === 0 && fileNamesFromBatchesOrJob.length > 0 && (
            <div className="mb-1">
              <p className="text-muted small mb-1">
                Files in this transfer ({fileNamesFromBatchesOrJob.length})
              </p>
              <ListGroup className="mb-1">
                {fileNamesFromBatchesOrJob
                  .slice(0, visibleFileNamesCount)
                  .map((name, i) => (
                    <ListGroup.Item key={`file-${i}`} className="py-1 small">
                      <i className="bi bi-file-earmark me-2 text-muted"></i>
                      {name}
                    </ListGroup.Item>
                  ))}
              </ListGroup>
              {canExpandOrCollapseFileNames && (
                <Button
                  variant="outline-secondary"
                  size="sm"
                  className="mt-1"
                  onClick={() => setFileNamesExpanded((e) => !e)}
                >
                  {fileNamesExpanded
                    ? "Show less"
                    : `Show ${moreFileNamesCount} more`}
                </Button>
              )}
            </div>
          )}
          {state.results_truncated && (
            <p className="text-muted small mb-0">
              Showing first {state.results.length} results; more files were transferred.
            </p>
          )}
        </>
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

      {(!!state.job_run_url || (state.job_run_statuses?.length ?? 0) > 0) && (
        <Alert variant="light" className="mt-3 py-2">
          <i className="bi bi-diagram-3 me-2"></i>
          <strong className="me-2">Databricks job:</strong>
          {state.job_run_statuses?.[0]?.status != null && (
            <JobRunStatusBadge status={state.job_run_statuses[0].status} className="me-2" />
          )}
          <span className="d-flex align-items-center gap-2 flex-wrap">
            {(state.job_run_url ?? state.job_run_statuses?.[0]?.url) && (
              <a
                href={state.job_run_url ?? state.job_run_statuses![0].url ?? "#"}
                target="_blank"
                rel="noopener noreferrer"
                className="alert-link small"
              >
                Open run in Databricks
              </a>
            )}
            {(() => {
              const n =
                (state.batches?.length ? state.batches.reduce((s, b) => s + b.file_count, 0) : null) ??
                state.job_run_statuses?.[0]?.file_names?.length ??
                0;
              return n > 0 ? (
                <span className="small text-muted">
                  ({n} file{n !== 1 ? "s" : ""})
                </span>
              ) : null;
            })()}
          </span>
          {(state.batches?.length ?? 0) > 0 ? (
            <>
              <div className="small text-muted mt-2 mb-1">Batches</div>
              <ListGroup variant="flush" className="mt-1">
                {state.batches!.map((batch, displayIndex) => (
                  <ListGroup.Item
                    key={batch.index}
                    className="d-flex justify-content-between align-items-center px-0 py-1 border-0"
                  >
                    <span className="d-flex align-items-center gap-2 flex-wrap">
                      <BatchStatusBadge batch={batch} />
                      <span className="small text-muted">
                        Batch {displayIndex + 1}{" "}
                        ({batch.file_count} file{batch.file_count !== 1 ? "s" : ""})
                      </span>
                      {batch.state_message && (
                        <small className="text-danger text-truncate" style={{ maxWidth: "20rem" }} title={batch.state_message}>
                          {batch.state_message}
                        </small>
                      )}
                    </span>
                  </ListGroup.Item>
                ))}
              </ListGroup>
            </>
          ) : (state.task_iterations?.length ?? 0) > 0 ? (
            <>
              <div className="small text-muted mt-2 mb-1">Batches</div>
              <ListGroup variant="flush" className="mt-1">
                {state.task_iterations!.map((it, displayIndex) => {
                  const fileCount = state.batch_file_counts?.[it.index];
                  const filesLabel =
                    fileCount != null ? ` (${fileCount} file${fileCount !== 1 ? "s" : ""})` : "";
                  return (
                    <ListGroup.Item
                      key={it.index}
                      className="d-flex justify-content-between align-items-center px-0 py-1 border-0"
                    >
                      <span className="d-flex align-items-center gap-2 flex-wrap">
                        <TaskIterationStatusBadge iteration={it} />
                        <span className="small text-muted">
                          Batch {displayIndex + 1}
                          {filesLabel}
                        </span>
                        {it.state_message && (
                          <small className="text-danger text-truncate" style={{ maxWidth: "20rem" }} title={it.state_message}>
                            {it.state_message}
                          </small>
                        )}
                      </span>
                    </ListGroup.Item>
                  );
                })}
              </ListGroup>
            </>
          ) : (state.total_iterations != null && state.total_iterations > 0) ? (
            <>
              <div className="small text-muted mt-2 mb-1">Batches</div>
              <ListGroup variant="flush" className="mt-1">
                {Array.from({ length: state.total_iterations }, (_, batchIndex) => batchIndex).map(
                  (batchIndex, displayIndex) => {
                    const fileCount = state.batch_file_counts?.[batchIndex];
                    const filesLabel =
                      fileCount != null ? ` (${fileCount} file${fileCount !== 1 ? "s" : ""})` : "";
                    return (
                      <ListGroup.Item
                        key={batchIndex}
                        className="d-flex justify-content-between align-items-center px-0 py-1 border-0"
                      >
                        <span className="d-flex align-items-center gap-2 flex-wrap">
                          <Badge bg="secondary">PENDING</Badge>
                          <span className="small text-muted">
                            Batch {displayIndex + 1}
                            {filesLabel}
                          </span>
                        </span>
                      </ListGroup.Item>
                    );
                  })}
              </ListGroup>
            </>
          ) : null}
          {(!state.task_iterations?.length && state.job_run_statuses?.length && !state.total_iterations) ? (
            <ListGroup variant="flush" className="mt-2">
              {state.job_run_statuses!.map((job: JobRunStatus) => (
                <ListGroup.Item
                  key={job.run_id}
                  className="d-flex justify-content-between align-items-center px-0 py-1 border-0"
                >
                  <span className="d-flex align-items-center gap-2 flex-wrap">
                    <JobRunStatusBadge status={job.status} />
                    {job.error && <small className="text-danger">{job.error}</small>}
                  </span>
                </ListGroup.Item>
              ))}
            </ListGroup>
          ) : null}
        </Alert>
      )}
    </div>
  );
}

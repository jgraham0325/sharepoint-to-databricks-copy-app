import { apiFetch } from "./client";

export interface FileTransferItem {
  drive_id: string;
  item_id: string;
  name: string;
  size: number;
}

export interface FolderTransferItem {
  drive_id: string;
  folder_item_id: string;
  folder_name: string;
}

export interface TransferRequest {
  files: FileTransferItem[];
  catalog: string;
  schema_name: string;
  volume: string;
  subfolder: string;
  folders?: FolderTransferItem[];
}

export type TransferStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed";

export interface FileResult {
  name: string;
  status: TransferStatus;
  error: string | null;
}

export interface JobRunStatus {
  run_id: number;
  url?: string | null;
  status: "queued" | "running" | "success" | "failed";
  file_names: string[];
  error?: string | null;
}

/** One For Each iteration (transfer task). Databricks life_cycle_state / result_state naming. */
export interface TaskIterationStatus {
  index: number;
  life_cycle_state: string;
  result_state: string;
  state_message: string;
}

/** One For Each batch: status and the files in that batch (iteration-centric). */
export interface TransferBatch {
  index: number;
  life_cycle_state: string;
  result_state: string;
  state_message: string;
  file_count: number;
  file_names: string[];
}

export interface TransferState {
  transfer_id: string;
  status: TransferStatus;
  total: number;
  completed: number;
  failed: number;
  results: FileResult[];
  catalog?: string;
  schema_name?: string;
  volume?: string;
  catalog_explorer_url?: string | null;
  /** Links to Databricks job run UI when transfer uses job run(s) for large files */
  job_run_urls?: string[] | null;
  /** Single job run URL (one run with For Each) */
  job_run_url?: string | null;
  /** Per-job-run status for incremental UI updates */
  job_run_statuses?: JobRunStatus[] | null;
  /** For Each task iteration statuses (Databricks naming) */
  task_iterations?: TaskIterationStatus[] | null;
  /** Number of For Each iterations (for progress estimation) */
  total_iterations?: number | null;
  /** File count per batch (same order as task_iterations); set when job is submitted */
  batch_file_counts?: number[] | null;
  /** Batches: status + file list per For Each iteration (canonical for iteration-level status) */
  batches?: TransferBatch[] | null;
  /** Elapsed time in seconds when transfer has finished */
  duration_seconds?: number | null;
  /** True when results were capped (only first N kept in memory) */
  results_truncated?: boolean;
}

export function startTransfer(req: TransferRequest): Promise<TransferState> {
  return apiFetch("/api/v1/transfer/start", {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export function getTransferStatus(transferId: string): Promise<TransferState> {
  return apiFetch(`/api/v1/transfer/status/${encodeURIComponent(transferId)}`);
}

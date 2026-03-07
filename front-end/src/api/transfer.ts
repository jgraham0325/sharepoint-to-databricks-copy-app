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
  /** Per-job-run status for incremental UI updates */
  job_run_statuses?: JobRunStatus[] | null;
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

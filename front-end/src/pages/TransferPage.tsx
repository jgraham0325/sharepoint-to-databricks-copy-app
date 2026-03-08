import { useState, useCallback, useEffect } from "react";
import { Container, Row, Col, Card, Form, Button } from "react-bootstrap";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import type { DriveItem } from "../api/sharepoint";
import type { FileTransferItem, FolderTransferItem } from "../api/transfer";
import VolumeSelector from "../components/transfer/VolumeSelector";
import TransferPanel from "../components/transfer/TransferPanel";
import TransfersList from "../components/transfer/TransfersList";
import { useTransfer } from "../hooks/useTransfer";

const ACTIVE_TRANSFER_KEY = "sharepoint_upload_active_transfer";

interface LocationState {
  files: DriveItem[];
  folders: DriveItem[];
  driveId: string;
}

export default function TransferPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const state = location.state as LocationState | null;

  const hasSelection = !!(
    state &&
    ((state.files?.length ?? 0) > 0 || (state.folders?.length ?? 0) > 0)
  );

  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("");
  const [subfolder, setSubfolder] = useState("");

  // Prefer transfer_id from URL; sync from sessionStorage to URL on mount so URL is source of truth
  const transferIdFromUrl = searchParams.get("transfer_id");
  const [syncedTransferId, setSyncedTransferId] = useState<string | null>(
    () => transferIdFromUrl
  );

  useEffect(() => {
    // New file selection takes priority — don't restore a stale transfer
    if (hasSelection) {
      setSyncedTransferId(null);
      try {
        sessionStorage.removeItem(ACTIVE_TRANSFER_KEY);
      } catch {
        /* ignore */
      }
      return;
    }
    if (transferIdFromUrl) {
      setSyncedTransferId(transferIdFromUrl);
      return;
    }
    try {
      const fromStorage = sessionStorage.getItem(ACTIVE_TRANSFER_KEY);
      if (fromStorage) {
        setSearchParams(
          (prev) => {
            const next = new URLSearchParams(prev);
            next.set("transfer_id", fromStorage);
            return next;
          },
          { replace: true }
        );
        setSyncedTransferId(fromStorage);
      } else {
        setSyncedTransferId(null);
      }
    } catch {
      setSyncedTransferId(null);
    }
  }, [transferIdFromUrl, setSearchParams, hasSelection]);

  const clearPersistedTransferId = useCallback(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete("transfer_id");
        return next;
      },
      { replace: true }
    );
    setSyncedTransferId(null);
    try {
      sessionStorage.removeItem(ACTIVE_TRANSFER_KEY);
    } catch {
      /* ignore */
    }
  }, [setSearchParams]);

  const syncTransferIdToUrl = useCallback(
    (transferId: string) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          next.set("transfer_id", transferId);
          return next;
        },
        { replace: true }
      );
      try {
        sessionStorage.setItem(ACTIVE_TRANSFER_KEY, transferId);
      } catch {
        /* ignore */
      }
    },
    [setSearchParams]
  );

  const { transferState, isTransferring, beginTransfer } = useTransfer(
    syncedTransferId,
    clearPersistedTransferId,
    syncTransferIdToUrl
  );

  // Clear sessionStorage when transfer reaches terminal state (keep URL so panel still shows)
  useEffect(() => {
    if (
      transferState?.status === "completed" ||
      transferState?.status === "failed"
    ) {
      try {
        sessionStorage.removeItem(ACTIVE_TRANSFER_KEY);
      } catch {
        /* ignore */
      }
    }
  }, [transferState?.status]);

  const handleVolumeSelect = useCallback(
    (c: string, s: string, v: string) => {
      setCatalog(c);
      setSchema(s);
      setVolume(v);
    },
    []
  );

  // Use URL as source of truth for list vs detail: no transfer_id in URL => list view
  const showListView = !hasSelection && !transferIdFromUrl;
  const showTransferOnly = transferIdFromUrl && !hasSelection;

  if (showListView) {
    return (
      <Container className="page-content">
        <div className="d-flex justify-content-between align-items-center mb-3">
          <h2 className="mb-0" style={{ fontSize: "1.35rem", fontWeight: 600 }}>
            All Transfers
          </h2>
          <Button variant="primary" onClick={() => navigate("/")}>
            <i className="bi bi-plus-lg me-1"></i>
            New Transfer
          </Button>
        </div>
        <TransfersList onStartTransfer={() => navigate("/")} />
      </Container>
    );
  }

  if (showTransferOnly) {
    return (
      <Container className="page-content">
        <div className="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-3">
          <h2 className="mb-0" style={{ fontSize: "1.35rem", fontWeight: 600 }}>
            Transfer Detail
          </h2>
          <div className="d-flex align-items-center gap-2">
            <Button
              variant="outline-secondary"
              onClick={() => clearPersistedTransferId()}
            >
              <i className="bi bi-arrow-left me-1"></i>All Transfers
            </Button>
            <Button variant="primary" onClick={() => navigate("/")}>
              <i className="bi bi-plus-lg me-1"></i>New Transfer
            </Button>
          </div>
        </div>
        <TransferPanel state={transferState} />
      </Container>
    );
  }

  const files: FileTransferItem[] = (state!.files || []).map((f) => ({
    drive_id: state!.driveId,
    item_id: f.id,
    name: f.name,
    size: f.size,
  }));

  const folders: FolderTransferItem[] = (state!.folders || []).map((f) => ({
    drive_id: state!.driveId,
    folder_item_id: f.id,
    folder_name: f.name,
  }));

  const transferStarted = !!transferState;
  const canStart = catalog && schema && volume && !isTransferring && !transferStarted;

  const startTransferHandler = () => {
    if (!canStart) return;
    beginTransfer({
      files,
      folders: folders.length > 0 ? folders : undefined,
      catalog,
      schema_name: schema,
      volume,
      subfolder,
    });
  };

  return (
    <Container className="page-content">
      <div className="d-flex flex-wrap justify-content-between align-items-center gap-2 mb-4">
        <h2 className="mb-0" style={{ fontSize: "1.35rem", fontWeight: 600 }}>
          {transferStarted ? "Transfer in Progress" : "New Transfer"}
        </h2>
        <div className="d-flex align-items-center gap-2">
          {!transferStarted && (
            <Button
              variant="success"
              disabled={!canStart}
              onClick={startTransferHandler}
            >
              <i className="bi bi-cloud-upload me-2"></i>
              Start Transfer
            </Button>
          )}
          <Button
            variant="outline-secondary"
            onClick={() => {
              clearPersistedTransferId();
              navigate("/transfer", { replace: true });
            }}
          >
            <i className="bi bi-arrow-left me-1"></i>All Transfers
          </Button>
          <Button variant="outline-secondary" onClick={() => navigate("/")}>
            <i className="bi bi-folder2-open me-1"></i>Browse SharePoint
          </Button>
        </div>
      </div>

      <Row>
        <Col md={6}>
          <Card body className="mb-3">
            <h6 className="mb-3" style={{ fontWeight: 600 }}>
              <i className="bi bi-microsoft text-primary me-2"></i>
              SharePoint Source
            </h6>
            <ul className="list-unstyled mb-0">
              {folders.map((f) => (
                <li key={f.folder_item_id} className="py-1">
                  <i className="bi bi-folder-fill text-warning me-2"></i>
                  {f.folder_name} <small className="text-muted">(folder + contents)</small>
                </li>
              ))}
              {files.map((f) => (
                <li key={f.item_id} className="py-1">
                  <i className="bi bi-file-earmark me-2"></i>
                  {f.name}
                </li>
              ))}
            </ul>
          </Card>
        </Col>

        <Col md={6}>
          {!transferStarted && (
            <Card body className="mb-3">
              <h6 className="mb-3" style={{ fontWeight: 600 }}>
                <i className="bi bi-database me-2" style={{ color: "var(--color-accent)" }}></i>
                Databricks Destination
              </h6>
              <VolumeSelector onSelect={handleVolumeSelect} />

              <Form.Group className="mt-3">
                <Form.Label className="small">
                  Subfolder (optional)
                </Form.Label>
                <Form.Control
                  placeholder="e.g. imports/2024"
                  value={subfolder}
                  onChange={(e) => setSubfolder(e.target.value)}
                />
              </Form.Group>
            </Card>
          )}

          {transferStarted && (
            <Card body className="mb-3">
              <h6 className="mb-3" style={{ fontWeight: 600 }}>
                <i className="bi bi-database me-2" style={{ color: "var(--color-accent)" }}></i>
                Databricks Destination
              </h6>
              <p className="small text-muted mb-0">
                <code>{catalog}.{schema}.{volume}</code>
                {subfolder && <span> / {subfolder}</span>}
              </p>
            </Card>
          )}

          <TransferPanel state={transferState} />

          {transferStarted && (
            <Card body className="mt-3 text-center">
              <p className="text-muted small mb-3">
                This transfer is running in the background. You can safely navigate away.
              </p>
              <div className="d-flex justify-content-center gap-2">
                <Button variant="primary" onClick={() => navigate("/")}>
                  <i className="bi bi-plus-lg me-1"></i>Transfer More Files
                </Button>
                <Button
                  variant="outline-secondary"
                  onClick={() => {
                    clearPersistedTransferId();
                    navigate("/transfer", { replace: true });
                  }}
                >
                  <i className="bi bi-list-ul me-1"></i>View All Transfers
                </Button>
              </div>
            </Card>
          )}
        </Col>
      </Row>
    </Container>
  );
}

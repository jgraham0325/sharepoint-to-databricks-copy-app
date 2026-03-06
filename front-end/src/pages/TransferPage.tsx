import { useState, useCallback, useEffect } from "react";
import { Container, Row, Col, Card, Form, Button, Alert } from "react-bootstrap";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import type { DriveItem } from "../api/sharepoint";
import type { FileTransferItem, FolderTransferItem } from "../api/transfer";
import VolumeSelector from "../components/transfer/VolumeSelector";
import TransferPanel from "../components/transfer/TransferPanel";
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
  }, [transferIdFromUrl, setSearchParams]);

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

  const { transferState, isTransferring, beginTransfer } = useTransfer(
    syncedTransferId,
    clearPersistedTransferId
  );

  // Persist transfer_id to URL and sessionStorage when a transfer is active
  useEffect(() => {
    const id = transferState?.transfer_id;
    if (!id) return;
    if (searchParams.get("transfer_id") !== id) {
      const next = new URLSearchParams(searchParams);
      next.set("transfer_id", id);
      setSearchParams(next, { replace: true });
    }
    try {
      sessionStorage.setItem(ACTIVE_TRANSFER_KEY, id);
    } catch {
      /* ignore */
    }
  }, [transferState?.transfer_id, searchParams, setSearchParams]);

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

  const hasFiles = (state?.files?.length ?? 0) > 0;
  const hasFolders = (state?.folders?.length ?? 0) > 0;
  const hasSelection = state && (hasFiles || hasFolders);
  const showTransferOnly = syncedTransferId && !hasSelection;

  if (!hasSelection && !syncedTransferId) {
    return (
      <Container className="page-content">
        <Alert variant="info" className="mb-0">
          No files or folders selected.{" "}
          <Alert.Link onClick={() => navigate("/")}>
            Go back to browse.
          </Alert.Link>
        </Alert>
      </Container>
    );
  }

  if (showTransferOnly) {
    return (
      <Container className="page-content">
        <div className="d-flex justify-content-between align-items-center mb-3">
          <h2 className="mb-0" style={{ fontSize: "1.35rem", fontWeight: 600 }}>
            Transfer to volume
          </h2>
          <Button variant="outline-secondary" onClick={() => navigate("/")}>
            <i className="bi bi-arrow-left me-1"></i>Back
          </Button>
        </div>
        <Alert variant="secondary" className="mb-3">
          Transfer in progress (list of files not available after refresh).
        </Alert>
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

  const canStart = catalog && schema && volume && !isTransferring;

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
          Transfer to volume
        </h2>
        <div className="d-flex align-items-center gap-2">
          <Button
            variant="success"
            disabled={!canStart}
            onClick={startTransferHandler}
          >
            {isTransferring ? (
              <>
                <span className="spinner-border spinner-border-sm me-2"></span>
                Transferring...
              </>
            ) : (
              <>
                <i className="bi bi-cloud-upload me-2"></i>
                Start Transfer
              </>
            )}
          </Button>
          <Button variant="outline-secondary" onClick={() => navigate("/")}>
            <i className="bi bi-arrow-left me-1"></i>Back
          </Button>
        </div>
      </div>

      <Row>
        <Col md={6}>
          <Card body className="mb-3">
            <h6 className="mb-3" style={{ fontWeight: 600 }}>To transfer</h6>
            <ul className="list-unstyled mb-0">
              {folders.map((f) => (
                <li key={f.folder_item_id} className="py-1">
                  <i className="bi bi-folder-fill text-warning me-2"></i>
                  {f.folder_name} <small className="text-muted">(folder + hierarchy)</small>
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
          <Card body className="mb-3">
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

          <TransferPanel state={transferState} />
        </Col>
      </Row>
    </Container>
  );
}

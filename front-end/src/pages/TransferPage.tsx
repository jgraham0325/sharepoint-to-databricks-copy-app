import { useState, useCallback } from "react";
import { Container, Row, Col, Card, Form, Button, Alert } from "react-bootstrap";
import { useLocation, useNavigate } from "react-router-dom";
import type { DriveItem } from "../api/sharepoint";
import type { FileTransferItem, FolderTransferItem } from "../api/transfer";
import VolumeSelector from "../components/transfer/VolumeSelector";
import TransferPanel from "../components/transfer/TransferPanel";
import { useTransfer } from "../hooks/useTransfer";

interface LocationState {
  files: DriveItem[];
  folders: DriveItem[];
  driveId: string;
}

export default function TransferPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const state = location.state as LocationState | null;

  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [volume, setVolume] = useState("");
  const [subfolder, setSubfolder] = useState("");

  const { transferState, isTransferring, beginTransfer } = useTransfer();

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
  if (!state || (!hasFiles && !hasFolders)) {
    return (
      <Container className="py-4">
        <Alert variant="info">
          No files or folders selected.{" "}
          <Alert.Link onClick={() => navigate("/")}>
            Go back to browse.
          </Alert.Link>
        </Alert>
      </Container>
    );
  }

  const files: FileTransferItem[] = (state.files || []).map((f) => ({
    drive_id: state.driveId,
    item_id: f.id,
    name: f.name,
    size: f.size,
  }));

  const folders: FolderTransferItem[] = (state.folders || []).map((f) => ({
    drive_id: state.driveId,
    folder_item_id: f.id,
    folder_name: f.name,
  }));

  const canStart = catalog && schema && volume && !isTransferring;

  return (
    <Container className="py-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h4 className="mb-0">Transfer Files to Volume</h4>
        <Button variant="outline-secondary" onClick={() => navigate("/")}>
          <i className="bi bi-arrow-left me-1"></i>Back
        </Button>
      </div>

      <Row>
        <Col md={6}>
          <Card body className="mb-3">
            <h6>To transfer</h6>
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

            <Button
              className="w-100 mt-3"
              variant="success"
              disabled={!canStart}
              onClick={() =>
                beginTransfer({
                  files,
                  folders: folders.length > 0 ? folders : undefined,
                  catalog,
                  schema_name: schema,
                  volume,
                  subfolder,
                })
              }
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
          </Card>

          <TransferPanel state={transferState} />
        </Col>
      </Row>
    </Container>
  );
}

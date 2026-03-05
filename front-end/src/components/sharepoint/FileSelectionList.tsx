import { ListGroup, Button, Badge } from "react-bootstrap";
import type { DriveItem } from "../../api/sharepoint";

interface Props {
  selectedFiles: DriveItem[];
  selectedFolders: DriveItem[];
  onRemoveFile: (item: DriveItem) => void;
  onRemoveFolder: (item: DriveItem) => void;
  onClear: () => void;
}

export default function FileSelectionList({
  selectedFiles,
  selectedFolders,
  onRemoveFile,
  onRemoveFolder,
  onClear,
}: Props) {
  const total = selectedFiles.length + selectedFolders.length;
  if (total === 0) return null;

  return (
    <div className="mt-3">
      <div className="d-flex justify-content-between align-items-center mb-2">
        <h6 className="mb-0">
          Selected <Badge bg="primary">{total}</Badge>
        </h6>
        <Button variant="outline-danger" size="sm" onClick={onClear}>
          Clear all
        </Button>
      </div>
      <ListGroup>
        {selectedFolders.map((folder) => (
          <ListGroup.Item
            key={folder.id}
            className="d-flex justify-content-between align-items-center py-2"
          >
            <span>
              <i className="bi bi-folder-fill me-2 text-warning"></i>
              {folder.name}
              <small className="text-muted ms-1">(folder + subfolders)</small>
            </span>
            <Button
              variant="outline-danger"
              size="sm"
              onClick={() => onRemoveFolder(folder)}
            >
              <i className="bi bi-x"></i>
            </Button>
          </ListGroup.Item>
        ))}
        {selectedFiles.map((file) => (
          <ListGroup.Item
            key={file.id}
            className="d-flex justify-content-between align-items-center py-2"
          >
            <span>
              <i className="bi bi-file-earmark-check me-2 text-success"></i>
              {file.name}
            </span>
            <Button
              variant="outline-danger"
              size="sm"
              onClick={() => onRemoveFile(file)}
            >
              <i className="bi bi-x"></i>
            </Button>
          </ListGroup.Item>
        ))}
      </ListGroup>
    </div>
  );
}

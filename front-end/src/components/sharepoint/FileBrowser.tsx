import { ListGroup, Spinner, Form } from "react-bootstrap";
import type { DriveItem } from "../../api/sharepoint";

interface Props {
  items: DriveItem[];
  loading: boolean;
  selectedFiles: DriveItem[];
  selectedFolders: DriveItem[];
  onOpenFolder: (item: DriveItem) => void;
  onToggleFile: (item: DriveItem) => void;
  onToggleFolder: (item: DriveItem) => void;
  onSelectAll?: () => void;
  onDeselectAll?: () => void;
}

function formatSize(bytes: number): string {
  if (bytes === 0) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function FileBrowser({
  items,
  loading,
  selectedFiles,
  selectedFolders,
  onOpenFolder,
  onToggleFile,
  onToggleFolder,
  onSelectAll,
  onDeselectAll,
}: Props) {
  if (loading) {
    return (
      <div className="text-center py-4">
        <Spinner />
      </div>
    );
  }

  const isFileSelected = (item: DriveItem) =>
    selectedFiles.some((f) => f.id === item.id);
  const isFolderSelected = (item: DriveItem) =>
    selectedFolders.some((f) => f.id === item.id);

  const fileItems = items.filter((i) => !i.is_folder);
  const folderItems = items.filter((i) => i.is_folder);
  const allFilesSelected =
    fileItems.length === 0 ||
    fileItems.every((item) => selectedFiles.some((f) => f.id === item.id));
  const allFoldersSelected =
    folderItems.length === 0 ||
    folderItems.every((item) => selectedFolders.some((f) => f.id === item.id));
  const allSelected = allFilesSelected && allFoldersSelected;
  const hasItems = fileItems.length > 0 || folderItems.length > 0;

  return (
    <>
      {hasItems && onSelectAll && onDeselectAll && (
        <div className="d-flex align-items-center mb-2">
          <Form.Check
            type="checkbox"
            id="select-all"
            label="Select all"
            checked={allSelected}
            onChange={() => (allSelected ? onDeselectAll() : onSelectAll())}
            className="small"
          />
        </div>
      )}
      <ListGroup>
      {items.map((item) => (
        <ListGroup.Item
          key={item.id}
          action={item.is_folder}
          onClick={() => (item.is_folder ? onOpenFolder(item) : undefined)}
          className="d-flex align-items-center"
        >
          <Form.Check
            type="checkbox"
            checked={item.is_folder ? isFolderSelected(item) : isFileSelected(item)}
            onChange={() =>
              item.is_folder ? onToggleFolder(item) : onToggleFile(item)
            }
            className="me-2"
            onClick={(e) => e.stopPropagation()}
          />
          <i
            className={`bi ${item.is_folder ? "bi-folder-fill text-warning" : "bi-file-earmark"} me-2`}
          ></i>
          <span className="flex-grow-1">{item.name}</span>
          {!item.is_folder && (
            <small className="text-muted">{formatSize(item.size)}</small>
          )}
          {item.is_folder && <i className="bi bi-chevron-right"></i>}
        </ListGroup.Item>
      ))}
      {items.length === 0 && (
        <ListGroup.Item className="text-muted text-center">
          This folder is empty
        </ListGroup.Item>
      )}
    </ListGroup>
    </>
  );
}

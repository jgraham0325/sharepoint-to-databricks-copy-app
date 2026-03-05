import { ListGroup, Spinner } from "react-bootstrap";
import type { Drive } from "../../api/sharepoint";

interface Props {
  drives: Drive[];
  loading: boolean;
  onSelect: (drive: Drive) => void;
}

export default function DriveList({ drives, loading, onSelect }: Props) {
  if (loading) {
    return (
      <div className="text-center py-4">
        <Spinner />
      </div>
    );
  }

  return (
    <ListGroup>
      {drives.map((drive) => (
        <ListGroup.Item
          key={drive.id}
          action
          onClick={() => onSelect(drive)}
          className="d-flex justify-content-between align-items-center"
        >
          <div>
            <i className="bi bi-hdd me-2"></i>
            <strong>{drive.name}</strong>
            <small className="text-muted ms-2">{drive.drive_type}</small>
          </div>
          <i className="bi bi-chevron-right"></i>
        </ListGroup.Item>
      ))}
      {drives.length === 0 && (
        <ListGroup.Item className="text-muted text-center">
          No document libraries found
        </ListGroup.Item>
      )}
    </ListGroup>
  );
}

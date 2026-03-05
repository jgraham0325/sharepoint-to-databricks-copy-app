import { useState, useEffect } from "react";
import { Form, InputGroup, ListGroup, Spinner, Alert } from "react-bootstrap";
import type { Site } from "../../api/sharepoint";

interface Props {
  sites: Site[];
  loading: boolean;
  onSearch: (query: string) => void;
  onSelect: (site: Site) => void;
}

export default function SiteSearch({ sites, loading, onSearch, onSelect }: Props) {
  const [query, setQuery] = useState("");

  // Filter sites as user types
  useEffect(() => {
    onSearch(query);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  return (
    <div>
      <Form className="mb-3">
        <InputGroup>
          <InputGroup.Text>
            <i className="bi bi-search"></i>
          </InputGroup.Text>
          <Form.Control
            placeholder="Search or filter sites..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={loading}
          />
        </InputGroup>
        <Form.Text className="text-muted">
          {query ? (
            <>Showing {sites.length} site{sites.length !== 1 ? "s" : ""} matching "{query}"</>
          ) : (
            <>Showing all {sites.length} site{sites.length !== 1 ? "s" : ""} you have access to</>
          )}
        </Form.Text>
      </Form>

      {loading ? (
        <div className="text-center py-4">
          <Spinner animation="border" role="status">
            <span className="visually-hidden">Loading sites...</span>
          </Spinner>
        </div>
      ) : sites.length === 0 ? (
        <Alert variant="info">
          {query ? (
            <>No sites found matching "{query}". Try a different search term.</>
          ) : (
            <>
              <strong>No SharePoint sites found.</strong>
              <br />
              <small>
                This could mean:
                <ul className="mb-0 mt-2">
                  <li>You don't have access to any SharePoint sites</li>
                  <li>Your organization hasn't set up SharePoint sites yet</li>
                  <li>Permissions need to be configured by your administrator</li>
                </ul>
                Contact your SharePoint administrator if you believe you should have access to sites.
              </small>
            </>
          )}
        </Alert>
      ) : (
        <ListGroup>
          {sites.map((site) => (
            <ListGroup.Item
              key={site.id}
              action
              onClick={() => onSelect(site)}
              className="d-flex justify-content-between align-items-center"
            >
              <div>
                <i className="bi bi-globe me-2"></i>
                <strong>{site.display_name}</strong>
                <small className="text-muted ms-2">{site.name}</small>
              </div>
              <i className="bi bi-chevron-right"></i>
            </ListGroup.Item>
          ))}
        </ListGroup>
      )}
    </div>
  );
}

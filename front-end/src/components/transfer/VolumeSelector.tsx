import { useState, useEffect, useRef } from "react";
import { Form, Spinner } from "react-bootstrap";
import {
  listCatalogs,
  listSchemas,
  listVolumes,
  type CatalogItem,
  type SchemaItem,
  type VolumeItem,
} from "../../api/volumes";
import toast from "react-hot-toast";

const STORAGE_KEY = "sharepoint_upload_app_volume";

function loadSavedVolume(): { catalog: string; schema: string; volume: string } | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { catalog?: string; schema_name?: string; volume?: string };
    if (parsed.catalog && parsed.schema_name && parsed.volume) {
      return { catalog: parsed.catalog, schema: parsed.schema_name, volume: parsed.volume };
    }
  } catch {
    // ignore
  }
  return null;
}

function saveVolume(catalog: string, schema: string, volume: string) {
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ catalog, schema_name: schema, volume })
    );
  } catch {
    // ignore
  }
}

interface Props {
  onSelect: (catalog: string, schema: string, volume: string) => void;
}

export default function VolumeSelector({ onSelect }: Props) {
  const saved = loadSavedVolume();
  const [catalogs, setCatalogs] = useState<CatalogItem[]>([]);
  const [schemas, setSchemas] = useState<SchemaItem[]>([]);
  const [volumes, setVolumes] = useState<VolumeItem[]>([]);

  const [selectedCatalog, setSelectedCatalog] = useState(saved?.catalog ?? "");
  const [selectedSchema, setSelectedSchema] = useState(saved?.schema ?? "");
  const [selectedVolume, setSelectedVolume] = useState(saved?.volume ?? "");

  const [loading, setLoading] = useState(false);
  const isRestoring = useRef(!!saved);

  useEffect(() => {
    setLoading(true);
    listCatalogs()
      .then(setCatalogs)
      .catch((e) => toast.error(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selectedCatalog) {
      setSchemas([]);
      return;
    }
    if (!isRestoring.current) {
      setSelectedSchema("");
      setSelectedVolume("");
    }
    setLoading(true);
    listSchemas(selectedCatalog)
      .then(setSchemas)
      .catch((e) => toast.error(e.message))
      .finally(() => setLoading(false));
  }, [selectedCatalog]);

  useEffect(() => {
    if (!selectedCatalog || !selectedSchema) {
      setVolumes([]);
      return;
    }
    if (!isRestoring.current) {
      setSelectedVolume("");
    }
    setLoading(true);
    listVolumes(selectedCatalog, selectedSchema)
      .then(setVolumes)
      .catch((e) => toast.error(e.message))
      .finally(() => setLoading(false));
  }, [selectedCatalog, selectedSchema]);

  // After first cascade of loads, clear "restoring" so future catalog/schema changes clear downstream
  useEffect(() => {
    if (isRestoring.current && catalogs.length > 0) {
      isRestoring.current = false;
    }
  }, [catalogs.length]);

  // When lists load, clear selection if saved value is no longer valid
  useEffect(() => {
    if (catalogs.length === 0) return;
    const catalogNames = new Set(catalogs.map((c) => c.name));
    if (selectedCatalog && !catalogNames.has(selectedCatalog)) {
      setSelectedCatalog("");
      setSelectedSchema("");
      setSelectedVolume("");
    }
  }, [catalogs]);

  useEffect(() => {
    if (schemas.length === 0) return;
    const schemaNames = new Set(schemas.map((s) => s.name));
    if (selectedSchema && !schemaNames.has(selectedSchema)) {
      setSelectedSchema("");
      setSelectedVolume("");
    }
  }, [schemas]);

  useEffect(() => {
    if (volumes.length === 0) return;
    const volumeNames = new Set(volumes.map((v) => v.name));
    if (selectedVolume && !volumeNames.has(selectedVolume)) {
      setSelectedVolume("");
    }
  }, [volumes]);

  useEffect(() => {
    if (selectedCatalog && selectedSchema && selectedVolume) {
      onSelect(selectedCatalog, selectedSchema, selectedVolume);
      saveVolume(selectedCatalog, selectedSchema, selectedVolume);
    }
  }, [selectedCatalog, selectedSchema, selectedVolume, onSelect]);

  return (
    <div>
      <h6>
        Target Volume {loading && <Spinner size="sm" className="ms-2" />}
      </h6>

      <Form.Group className="mb-2">
        <Form.Label className="small">Catalog</Form.Label>
        <Form.Select
          value={selectedCatalog}
          onChange={(e) => setSelectedCatalog(e.target.value)}
        >
          <option value="">Select catalog...</option>
          {catalogs.map((c) => (
            <option key={c.name} value={c.name}>
              {c.name}
            </option>
          ))}
        </Form.Select>
      </Form.Group>

      <Form.Group className="mb-2">
        <Form.Label className="small">Schema</Form.Label>
        <Form.Select
          value={selectedSchema}
          onChange={(e) => setSelectedSchema(e.target.value)}
          disabled={!selectedCatalog}
        >
          <option value="">Select schema...</option>
          {schemas.map((s) => (
            <option key={s.name} value={s.name}>
              {s.name}
            </option>
          ))}
        </Form.Select>
      </Form.Group>

      <Form.Group className="mb-2">
        <Form.Label className="small">Volume</Form.Label>
        <Form.Select
          value={selectedVolume}
          onChange={(e) => setSelectedVolume(e.target.value)}
          disabled={!selectedSchema}
        >
          <option value="">Select volume...</option>
          {volumes.map((v) => (
            <option key={v.name} value={v.name}>
              {v.name}
            </option>
          ))}
        </Form.Select>
      </Form.Group>
    </div>
  );
}

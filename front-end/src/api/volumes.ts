import { apiFetch } from "./client";

export interface CatalogItem {
  name: string;
}

export interface SchemaItem {
  name: string;
}

export interface VolumeItem {
  name: string;
  volume_type: string | null;
}

export function listCatalogs(): Promise<CatalogItem[]> {
  return apiFetch("/api/v1/volumes/catalogs");
}

export function listSchemas(catalog: string): Promise<SchemaItem[]> {
  return apiFetch(
    `/api/v1/volumes/catalogs/${encodeURIComponent(catalog)}/schemas`
  );
}

export function listVolumes(
  catalog: string,
  schema: string
): Promise<VolumeItem[]> {
  return apiFetch(
    `/api/v1/volumes/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/volumes`
  );
}

import { apiFetch } from "./client";

export interface Site {
  id: string;
  name: string;
  display_name: string;
  web_url: string;
}

export interface Drive {
  id: string;
  name: string;
  drive_type: string;
  web_url: string;
}

export interface DriveItem {
  id: string;
  name: string;
  size: number;
  is_folder: boolean;
  web_url: string;
  mime_type: string | null;
  download_url: string | null;
  parent_path: string | null;
}

export function searchSites(query: string): Promise<Site[]> {
  return apiFetch(`/api/v1/sharepoint/sites?query=${encodeURIComponent(query)}`);
}

export function listDrives(siteId: string): Promise<Drive[]> {
  return apiFetch(`/api/v1/sharepoint/sites/${encodeURIComponent(siteId)}/drives`);
}

export function listChildren(
  driveId: string,
  itemId?: string
): Promise<DriveItem[]> {
  const params = itemId ? `?item_id=${encodeURIComponent(itemId)}` : "";
  return apiFetch(
    `/api/v1/sharepoint/drives/${encodeURIComponent(driveId)}/children${params}`
  );
}

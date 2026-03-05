import { useState, useCallback, useEffect } from "react";
import {
  searchSites,
  listDrives,
  listChildren,
  type Site,
  type Drive,
  type DriveItem,
} from "../api/sharepoint";
import toast from "react-hot-toast";
import { useAuth } from "../context/AuthContext";

export type BrowseLevel = "sites" | "drives" | "files";

interface BreadcrumbItem {
  label: string;
  level: BrowseLevel;
  driveId?: string;
  itemId?: string;
}

export function useSharePointBrowser() {
  const { isAuthenticated } = useAuth();
  const [level, setLevel] = useState<BrowseLevel>("sites");
  const [loading, setLoading] = useState(false);

  const [allSites, setAllSites] = useState<Site[]>([]); // All sites from API
  const [sites, setSites] = useState<Site[]>([]); // Filtered sites for display
  const [drives, setDrives] = useState<Drive[]>([]);
  const [items, setItems] = useState<DriveItem[]>([]);

  const [selectedSite, setSelectedSite] = useState<Site | null>(null);
  const [selectedDrive, setSelectedDrive] = useState<Drive | null>(null);
  const [currentDriveId, setCurrentDriveId] = useState<string>("");

  const [breadcrumbs, setBreadcrumbs] = useState<BreadcrumbItem[]>([]);
  const [selectedFiles, setSelectedFiles] = useState<DriveItem[]>([]);
  const [selectedFolders, setSelectedFolders] = useState<DriveItem[]>([]);

  // Load all sites when authenticated
  useEffect(() => {
    if (isAuthenticated) {
      loadAllSites();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAuthenticated]);

  const loadAllSites = useCallback(async () => {
    setLoading(true);
    try {
      // Load all sites with empty query to get everything
      const result = await searchSites("");
      setAllSites(result);
      setSites(result); // Show all by default
      setLevel("sites");
      setBreadcrumbs([]);
    } catch (err: any) {
      toast.error(err.message || "Failed to load sites");
    } finally {
      setLoading(false);
    }
  }, []);

  const doSearchSites = useCallback((query: string) => {
    // Filter sites client-side
    if (!query.trim()) {
      setSites(allSites);
    } else {
      const queryLower = query.toLowerCase();
      const filtered = allSites.filter(
        (site) =>
          site.name.toLowerCase().includes(queryLower) ||
          site.display_name.toLowerCase().includes(queryLower)
      );
      setSites(filtered);
    }
    setLevel("sites");
    setBreadcrumbs([]);
  }, [allSites]);

  const selectSite = useCallback(async (site: Site) => {
    setLoading(true);
    try {
      setSelectedSite(site);
      const result = await listDrives(site.id);
      setDrives(result);
      setLevel("drives");
      setBreadcrumbs([{ label: site.display_name, level: "sites" }]);
    } catch (err: any) {
      toast.error(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  const selectDrive = useCallback(
    async (drive: Drive) => {
      setLoading(true);
      try {
        setSelectedDrive(drive);
        setCurrentDriveId(drive.id);
        const result = await listChildren(drive.id);
        setItems(result);
        setLevel("files");
        setBreadcrumbs([
          { label: selectedSite?.display_name || "", level: "sites" },
          { label: drive.name, level: "drives" },
        ]);
      } catch (err: any) {
        toast.error(err.message);
      } finally {
        setLoading(false);
      }
    },
    [selectedSite]
  );

  const openFolder = useCallback(
    async (folder: DriveItem) => {
      setLoading(true);
      try {
        const result = await listChildren(currentDriveId, folder.id);
        setItems(result);
        setBreadcrumbs((prev) => [
          ...prev,
          {
            label: folder.name,
            level: "files",
            driveId: currentDriveId,
            itemId: folder.id,
          },
        ]);
      } catch (err: any) {
        toast.error(err.message);
      } finally {
        setLoading(false);
      }
    },
    [currentDriveId]
  );

  const navigateBreadcrumb = useCallback(
    async (index: number) => {
      const crumb = breadcrumbs[index];
      if (!crumb) return;

      if (crumb.level === "sites") {
        // Go back to sites list - show all sites
        setLevel("sites");
        setSites(allSites);
        setBreadcrumbs([]);
      } else if (crumb.level === "drives") {
        // Go back to site's drives
        if (selectedSite) await selectSite(selectedSite);
      } else if (crumb.level === "files" && crumb.driveId && crumb.itemId) {
        setLoading(true);
        try {
          const result = await listChildren(crumb.driveId, crumb.itemId);
          setItems(result);
          setBreadcrumbs(breadcrumbs.slice(0, index + 1));
        } catch (err: any) {
          toast.error(err.message);
        } finally {
          setLoading(false);
        }
      }
    },
    [breadcrumbs, selectedSite, selectedDrive, selectSite, selectDrive, allSites]
  );

  const toggleFileSelection = useCallback((item: DriveItem) => {
    setSelectedFiles((prev) => {
      const exists = prev.find((f) => f.id === item.id);
      if (exists) return prev.filter((f) => f.id !== item.id);
      return [...prev, item];
    });
  }, []);

  const toggleFolderSelection = useCallback((item: DriveItem) => {
    setSelectedFolders((prev) => {
      const exists = prev.find((f) => f.id === item.id);
      if (exists) return prev.filter((f) => f.id !== item.id);
      return [...prev, item];
    });
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedFiles([]);
    setSelectedFolders([]);
  }, []);

  const selectAllInCurrentFolder = useCallback(() => {
    setSelectedFiles((prev) => {
      const fileItems = items.filter((i) => !i.is_folder);
      const existingIds = new Set(prev.map((f) => f.id));
      const toAdd = fileItems.filter((f) => !existingIds.has(f.id));
      return [...prev, ...toAdd];
    });
  }, [items]);

  const deselectAllInCurrentFolder = useCallback(() => {
    setSelectedFiles((prev) => {
      const currentIds = new Set(items.filter((i) => !i.is_folder).map((i) => i.id));
      return prev.filter((f) => !currentIds.has(f.id));
    });
  }, [items]);

  return {
    level,
    loading,
    sites,
    drives,
    items,
    selectedSite,
    selectedDrive,
    breadcrumbs,
    selectedFiles,
    selectedFolders,
    doSearchSites,
    selectSite,
    selectDrive,
    openFolder,
    navigateBreadcrumb,
    toggleFileSelection,
    toggleFolderSelection,
    clearSelection,
    selectAllInCurrentFolder,
    deselectAllInCurrentFolder,
  };
}

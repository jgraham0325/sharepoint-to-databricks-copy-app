import { Container, Row, Col, Breadcrumb, Card, Button } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import LoginButton from "../components/auth/LoginButton";
import SiteSearch from "../components/sharepoint/SiteSearch";
import DriveList from "../components/sharepoint/DriveList";
import FileBrowser from "../components/sharepoint/FileBrowser";
import FileSelectionList from "../components/sharepoint/FileSelectionList";
import { useSharePointBrowser } from "../hooks/useSharePointBrowser";

export default function BrowsePage() {
  const { isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const browser = useSharePointBrowser();

  const hasSelection =
    browser.selectedFiles.length > 0 || browser.selectedFolders.length > 0;
  const transferLabel = hasSelection
    ? `Transfer${browser.selectedFiles.length > 0 ? ` ${browser.selectedFiles.length} file(s)` : ""}${browser.selectedFiles.length > 0 && browser.selectedFolders.length > 0 ? " +" : ""}${browser.selectedFolders.length > 0 ? ` ${browser.selectedFolders.length} folder(s)` : ""}`
    : "Transfer";

  return (
    <Container className="page-content">
      {!isAuthenticated ? (
        <Card body className="card-empty-state">
          <i
            className="bi bi-shield-lock"
            style={{ fontSize: "3rem" }}
          ></i>
          <p className="mt-3">
            Sign in with your Microsoft account to browse SharePoint files.
          </p>
          <LoginButton />
        </Card>
      ) : (
        <>
          <h2 className="mb-4" style={{ fontSize: "1.35rem", fontWeight: 600 }}>
            Browse SharePoint
          </h2>
          <Row>
            <Col md={8}>
              {browser.breadcrumbs.length > 0 && (
              <Breadcrumb className="mb-3">
                <Breadcrumb.Item
                  onClick={() => browser.doSearchSites("")}
                >
                  Sites
                </Breadcrumb.Item>
                {browser.breadcrumbs.map((crumb, i) => (
                  <Breadcrumb.Item
                    key={i}
                    active={i === browser.breadcrumbs.length - 1}
                    onClick={() =>
                      i < browser.breadcrumbs.length - 1 &&
                      browser.navigateBreadcrumb(i)
                    }
                  >
                    {crumb.label}
                  </Breadcrumb.Item>
                ))}
              </Breadcrumb>
            )}

            {browser.level === "sites" && (
              <SiteSearch
                sites={browser.sites}
                loading={browser.loading}
                onSearch={browser.doSearchSites}
                onSelect={browser.selectSite}
              />
            )}

            {browser.level === "drives" && (
              <DriveList
                drives={browser.drives}
                loading={browser.loading}
                onSelect={browser.selectDrive}
              />
            )}

            {browser.level === "files" && (
              <FileBrowser
                items={browser.items}
                loading={browser.loading}
                selectedFiles={browser.selectedFiles}
                selectedFolders={browser.selectedFolders}
                onOpenFolder={browser.openFolder}
                onToggleFile={browser.toggleFileSelection}
                onToggleFolder={browser.toggleFolderSelection}
                onSelectAll={browser.selectAllInCurrentFolder}
                onDeselectAll={browser.deselectAllInCurrentFolder}
              />
            )}
          </Col>

          <Col md={4}>
            {hasSelection && (
              <Button
                variant="success"
                className="w-100 mb-3"
                onClick={() =>
                  navigate("/transfer", {
                    state: {
                      files: browser.selectedFiles,
                      folders: browser.selectedFolders,
                      driveId: browser.selectedDrive?.id || "",
                    },
                  })
                }
              >
                <i className="bi bi-cloud-upload me-2"></i>
                {transferLabel}
              </Button>
            )}
            <FileSelectionList
              selectedFiles={browser.selectedFiles}
              selectedFolders={browser.selectedFolders}
              onRemoveFile={browser.toggleFileSelection}
              onRemoveFolder={browser.toggleFolderSelection}
              onClear={browser.clearSelection}
            />
          </Col>
          </Row>
        </>
      )}
    </Container>
  );
}

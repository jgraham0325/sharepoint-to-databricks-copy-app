import { Container, Row, Col, Breadcrumb, Card } from "react-bootstrap";
import { Link, useNavigate } from "react-router-dom";
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

  return (
    <Container className="py-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h4 className="mb-0">SharePoint File Browser</h4>
        <div className="d-flex align-items-center gap-2">
          <Link to="/agent" className="btn btn-outline-primary btn-sm">
            <i className="bi bi-robot me-1"></i>Agent
          </Link>
          <LoginButton />
        </div>
      </div>

      {!isAuthenticated ? (
        <Card body className="text-center py-5">
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
            <FileSelectionList
              selectedFiles={browser.selectedFiles}
              selectedFolders={browser.selectedFolders}
              onRemoveFile={browser.toggleFileSelection}
              onRemoveFolder={browser.toggleFolderSelection}
              onClear={browser.clearSelection}
            />
            {(browser.selectedFiles.length > 0 || browser.selectedFolders.length > 0) && (
              <button
                className="btn btn-success w-100 mt-3"
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
                Transfer{" "}
                {browser.selectedFiles.length > 0 &&
                  `${browser.selectedFiles.length} file(s)`}
                {browser.selectedFiles.length > 0 &&
                  browser.selectedFolders.length > 0 &&
                  " + "}
                {browser.selectedFolders.length > 0 &&
                  `${browser.selectedFolders.length} folder(s)`}
              </button>
            )}
          </Col>
        </Row>
      )}
    </Container>
  );
}

import { HashRouter, Routes, Route, Navigate, NavLink } from "react-router-dom";
import { Toaster } from "react-hot-toast";
import { AuthProvider } from "./context/AuthContext";
import LoginButton from "./components/auth/LoginButton";
import BrowsePage from "./pages/BrowsePage";
import TransferPage from "./pages/TransferPage";
import AgentPage from "./pages/AgentPage";

function AppBar() {
  return (
    <header className="app-bar">
      <NavLink to="/" className="app-bar__brand">
        <span className="app-bar__brand-icon" aria-hidden>
          <i className="bi bi-cloud-arrow-up-fill" style={{ fontSize: "0.9rem" }}></i>
        </span>
        SharePoint Upload
      </NavLink>
      <nav className="app-bar__nav">
        <NavLink
          to="/"
          end
          className={({ isActive }) => `app-bar__link ${isActive ? "app-bar__link--active" : ""}`}
        >
          Browse
        </NavLink>
        <NavLink
          to="/transfer"
          className={({ isActive }) => `app-bar__link ${isActive ? "app-bar__link--active" : ""}`}
        >
          Transfer
        </NavLink>
        <NavLink
          to="/agent"
          className={({ isActive }) => `app-bar__link ${isActive ? "app-bar__link--active" : ""}`}
        >
          <i className="bi bi-robot me-1"></i>
          Agent
        </NavLink>
      </nav>
      <div className="app-bar__actions">
        <LoginButton />
      </div>
    </header>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <HashRouter>
        <Toaster position="top-right" />
        <div className="app-shell">
          <AppBar />
          <main className="app-main">
            <Routes>
              <Route path="/" element={<BrowsePage />} />
              <Route path="/transfer" element={<TransferPage />} />
              <Route path="/agent" element={<AgentPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </main>
        </div>
      </HashRouter>
    </AuthProvider>
  );
}

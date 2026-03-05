import { HashRouter, Routes, Route, Navigate } from "react-router-dom";
import { Toaster } from "react-hot-toast";
import { AuthProvider } from "./context/AuthContext";
import BrowsePage from "./pages/BrowsePage";
import TransferPage from "./pages/TransferPage";
import AgentPage from "./pages/AgentPage";

export default function App() {
  return (
    <AuthProvider>
      <HashRouter>
        <Toaster position="top-right" />
        <Routes>
          <Route path="/" element={<BrowsePage />} />
          <Route path="/transfer" element={<TransferPage />} />
          <Route path="/agent" element={<AgentPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </HashRouter>
    </AuthProvider>
  );
}

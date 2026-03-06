import { Button } from "react-bootstrap";
import { useAuth } from "../../context/AuthContext";

export default function LoginButton() {
  const { isAuthenticated, user, login, logout } = useAuth();

  if (isAuthenticated) {
    return (
      <div className="d-flex align-items-center gap-2">
        <span className="text-muted small" title={user?.userPrincipalName ?? undefined}>
          <i className="bi bi-person-circle me-1"></i>
          Logged in as <strong>{user?.displayName ?? "…"}</strong>
        </span>
        <Button variant="outline-secondary" onClick={logout}>
          <i className="bi bi-box-arrow-right me-1"></i>Sign out
        </Button>
      </div>
    );
  }

  return (
    <Button variant="primary" onClick={login}>
      <i className="bi bi-microsoft me-2"></i>Sign in with Microsoft
    </Button>
  );
}

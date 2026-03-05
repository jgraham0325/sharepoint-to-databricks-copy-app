import { Button } from "react-bootstrap";
import { useAuth } from "../../context/AuthContext";

export default function LoginButton() {
  const { isAuthenticated, login, logout } = useAuth();

  if (isAuthenticated) {
    return (
      <Button variant="outline-secondary" size="sm" onClick={logout}>
        <i className="bi bi-box-arrow-right me-1"></i>Sign out of Microsoft
      </Button>
    );
  }

  return (
    <Button variant="primary" onClick={login}>
      <i className="bi bi-microsoft me-2"></i>Sign in with Microsoft
    </Button>
  );
}

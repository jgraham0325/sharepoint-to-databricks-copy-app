import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import toast from "react-hot-toast";
import { setMsToken } from "../api/client";
import { refreshToken as apiRefreshToken } from "../api/auth";

interface AuthState {
  accessToken: string | null;
  refreshTokenValue: string | null;
  isAuthenticated: boolean;
  login: () => void;
  logout: () => void;
}

const AuthContext = createContext<AuthState>({
  accessToken: null,
  refreshTokenValue: null,
  isAuthenticated: false,
  login: () => {},
  logout: () => {},
});

export function useAuth() {
  return useContext(AuthContext);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [refreshTokenValue, setRefreshTokenValue] = useState<string | null>(
    null
  );

  // Listen for postMessage from the OAuth popup
  useEffect(() => {
    function handler(e: MessageEvent) {
      if (e.origin !== window.location.origin) return;
      if (e.data?.type === "ms-auth-callback") {
        setAccessToken(e.data.access_token);
        setRefreshTokenValue(e.data.refresh_token);
        setMsToken(e.data.access_token);
      } else if (e.data?.type === "ms-auth-error") {
        // Handle authentication errors
        console.error("Authentication error:", e.data.error);
        toast.error(`Authentication failed: ${e.data.error}`);
      }
    }
    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);

  // Auto-refresh token 5 minutes before expiry
  useEffect(() => {
    if (!refreshTokenValue) return;
    const interval = setInterval(
      async () => {
        try {
          const res = await apiRefreshToken(refreshTokenValue);
          setAccessToken(res.access_token);
          setRefreshTokenValue(res.refresh_token);
          setMsToken(res.access_token);
        } catch {
          // Refresh failed — user will need to re-login
          setAccessToken(null);
          setRefreshTokenValue(null);
          setMsToken(null);
        }
      },
      50 * 60 * 1000 // refresh every 50 min
    );
    return () => clearInterval(interval);
  }, [refreshTokenValue]);

  const login = useCallback(async () => {
    try {
      const resp = await fetch("/api/v1/auth/login");
      if (!resp.ok) {
        throw new Error(`Failed to get login URL: ${resp.statusText}`);
      }
      const data = await resp.json();
      
      // Open popup and track it
      const popup = window.open(
        data.login_url, 
        "ms-login", 
        "width=600,height=700,scrollbars=yes,resizable=yes"
      );
      
      if (!popup) {
        toast.error("Popup blocked. Please allow popups for this site and try again.");
        return;
      }
      
      // Check if popup was closed manually (user cancelled)
      const checkClosed = setInterval(() => {
        if (popup.closed) {
          clearInterval(checkClosed);
          // Don't show error if user just closed the window
        }
      }, 500);
      
      // Timeout after 5 minutes
      setTimeout(() => {
        if (!popup.closed) {
          popup.close();
          clearInterval(checkClosed);
          toast.error("Login timed out. Please try again.");
        }
      }, 5 * 60 * 1000);
    } catch (error) {
      console.error("Login error:", error);
      toast.error(`Failed to start login: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }, []);

  const logout = useCallback(() => {
    setAccessToken(null);
    setRefreshTokenValue(null);
    setMsToken(null);
  }, []);

  return (
    <AuthContext.Provider
      value={{
        accessToken,
        refreshTokenValue,
        isAuthenticated: !!accessToken,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

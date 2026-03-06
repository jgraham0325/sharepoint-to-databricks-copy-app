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
import { refreshToken as apiRefreshToken, getMe } from "../api/auth";

const STORAGE_KEY = "ms-auth";

export interface MsUser {
  displayName: string;
  userPrincipalName: string;
}

interface AuthState {
  accessToken: string | null;
  refreshTokenValue: string | null;
  isAuthenticated: boolean;
  user: MsUser | null;
  login: () => void;
  logout: () => void;
}

const AuthContext = createContext<AuthState>({
  accessToken: null,
  refreshTokenValue: null,
  isAuthenticated: false,
  user: null,
  login: () => {},
  logout: () => {},
});

export function useAuth() {
  return useContext(AuthContext);
}

function loadStoredAuth(): {
  accessToken: string | null;
  refreshToken: string | null;
} {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return { accessToken: null, refreshToken: null };
    const data = JSON.parse(raw) as {
      access_token?: string;
      refresh_token?: string;
    };
    return {
      accessToken: data.access_token ?? null,
      refreshToken: data.refresh_token ?? null,
    };
  } catch {
    return { accessToken: null, refreshToken: null };
  }
}

function saveAuth(accessToken: string, refreshToken: string) {
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ access_token: accessToken, refresh_token: refreshToken })
    );
  } catch (e) {
    console.warn("Could not persist auth", e);
  }
}

function clearStoredAuth() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {}
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [refreshTokenValue, setRefreshTokenValue] = useState<string | null>(
    null
  );
  const [user, setUser] = useState<MsUser | null>(null);

  // Restore session from localStorage on mount; refresh if we only have refresh token
  useEffect(() => {
    const { accessToken: storedAccess, refreshToken: storedRefresh } =
      loadStoredAuth();
    if (storedRefresh) {
      // Prefer refreshing to get a valid access token
      apiRefreshToken(storedRefresh)
        .then((res) => {
          setAccessToken(res.access_token);
          setRefreshTokenValue(res.refresh_token);
          setMsToken(res.access_token);
          saveAuth(res.access_token, res.refresh_token);
        })
        .catch(() => {
          clearStoredAuth();
        });
    } else if (storedAccess) {
      setAccessToken(storedAccess);
      setRefreshTokenValue(null);
      setMsToken(storedAccess);
    }
  }, []);

  // Fetch current user when we have an access token
  useEffect(() => {
    if (!accessToken) {
      setUser(null);
      return;
    }
    getMe()
      .then((me) =>
        setUser({
          displayName: me.display_name,
          userPrincipalName: me.user_principal_name,
        })
      )
      .catch(() => setUser(null));
  }, [accessToken]);

  // Listen for postMessage from the OAuth popup (callback runs on backend, so in dev accept that origin)
  const allowedCallbackOrigin =
    typeof import.meta.env.VITE_APP_URL === "string" && import.meta.env.VITE_APP_URL
      ? import.meta.env.VITE_APP_URL.replace(/\/$/, "")
      : import.meta.env.DEV
        ? "http://localhost:8000"
        : null;
  useEffect(() => {
    function handler(e: MessageEvent) {
      const ok =
        e.origin === window.location.origin ||
        (allowedCallbackOrigin !== null && e.origin === allowedCallbackOrigin);
      if (!ok) return;
      if (e.data?.type === "ms-auth-callback") {
        const at = e.data.access_token;
        const rt = e.data.refresh_token;
        setAccessToken(at);
        setRefreshTokenValue(rt);
        setMsToken(at);
        saveAuth(at, rt);
      } else if (e.data?.type === "ms-auth-error") {
        console.error("Authentication error:", e.data.error);
        toast.error(`Authentication failed: ${e.data.error}`);
      }
    }
    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, [allowedCallbackOrigin]);

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
          saveAuth(res.access_token, res.refresh_token);
        } catch {
          setAccessToken(null);
          setRefreshTokenValue(null);
          setMsToken(null);
          clearStoredAuth();
        }
      },
      50 * 60 * 1000 // refresh every 50 min
    );
    return () => clearInterval(interval);
  }, [refreshTokenValue]);

  const login = useCallback(async () => {
    try {
      const resp = await fetch(
        typeof window !== "undefined" && window.location.origin
          ? `/api/v1/auth/login?origin=${encodeURIComponent(window.location.origin)}`
          : "/api/v1/auth/login"
      );
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
    setUser(null);
    setMsToken(null);
    clearStoredAuth();
  }, []);

  return (
    <AuthContext.Provider
      value={{
        accessToken,
        refreshTokenValue,
        isAuthenticated: !!accessToken,
        user,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

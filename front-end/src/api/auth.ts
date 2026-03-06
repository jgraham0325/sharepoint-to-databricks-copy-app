import { apiFetch } from "./client";

export interface LoginUrlResponse {
  login_url: string;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

export interface MeResponse {
  display_name: string;
  user_principal_name: string;
}

/** Pass origin when frontend runs on a different port (e.g. Vite dev on 5173) so the OAuth callback can postMessage back. */
export function getLoginUrl(frontendOrigin?: string): Promise<LoginUrlResponse> {
  const url =
    typeof frontendOrigin === "string" && frontendOrigin
      ? `/api/v1/auth/login?origin=${encodeURIComponent(frontendOrigin)}`
      : "/api/v1/auth/login";
  return apiFetch(url);
}

export function refreshToken(refresh_token: string): Promise<TokenResponse> {
  return apiFetch("/api/v1/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token }),
  });
}

export function getMe(): Promise<MeResponse> {
  return apiFetch("/api/v1/auth/me");
}

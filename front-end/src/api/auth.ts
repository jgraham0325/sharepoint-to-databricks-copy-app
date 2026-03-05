import { apiFetch } from "./client";

export interface LoginUrlResponse {
  login_url: string;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

export function getLoginUrl(): Promise<LoginUrlResponse> {
  return apiFetch("/api/v1/auth/login");
}

export function refreshToken(refresh_token: string): Promise<TokenResponse> {
  return apiFetch("/api/v1/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token }),
  });
}

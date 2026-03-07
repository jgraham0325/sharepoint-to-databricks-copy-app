let _msToken: string | null = null;
let _msRefreshToken: string | null = null;

export function setMsToken(token: string | null) {
  _msToken = token;
}

export function getMsToken(): string | null {
  return _msToken;
}

export function setMsRefreshToken(token: string | null) {
  _msRefreshToken = token;
}

export function getMsRefreshToken(): string | null {
  return _msRefreshToken;
}

export async function apiFetch<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };
  if (_msToken) {
    headers["X-MS-Token"] = _msToken;
  }
  if (_msRefreshToken) {
    headers["X-MS-Refresh-Token"] = _msRefreshToken;
  }

  const resp = await fetch(path, { ...options, headers });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed: ${resp.status}`);
  }
  return resp.json();
}

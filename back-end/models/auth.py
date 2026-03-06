from pydantic import BaseModel


class LoginUrlResponse(BaseModel):
    login_url: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int


class RefreshRequest(BaseModel):
    refresh_token: str


class MeResponse(BaseModel):
    display_name: str
    user_principal_name: str

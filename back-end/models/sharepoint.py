from typing import Optional
from pydantic import BaseModel


class Site(BaseModel):
    id: str
    name: str
    display_name: str
    web_url: str


class Drive(BaseModel):
    id: str
    name: str
    drive_type: str
    web_url: str


class DriveItem(BaseModel):
    id: str
    name: str
    size: int = 0
    is_folder: bool
    web_url: str
    mime_type: Optional[str] = None
    download_url: Optional[str] = None
    parent_path: Optional[str] = None

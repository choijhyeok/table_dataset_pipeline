from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import mimetypes
from pathlib import Path

scopes = ["https://www.googleapis.com/auth/drive.file"]

class gdrive_handler:
  def __init__(self, sa_json):
      self.creds = service_account.Credentials.from_service_account_file(sa_json, scopes=scopes)
  
  def upload_to_gdrive_folder(drive, local_path: Path, folder_id: str, target_name: str | None = None):
        """
        local_path 파일을 지정한 GDrive 폴더에 업로드.
        """
        
        target_name = local_path.name
        mime_type, _ = mimetypes.guess_type(str(local_path))
        media = MediaFileUpload(str(local_path), mimetype=mime_type or "application/octet-stream", resumable=True)

        meta = {"name": target_name, "parents": [folder_id]}
        created = drive.files().create(
            body=meta,
            media_body=media,
            fields="id,name,webViewLink,parents,driveId",
            supportsAllDrives=True,
        ).execute()
        
        return created
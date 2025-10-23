from __future__ import annotations
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]  # 업로드에 충분

def get_drive_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds)

def upload_file(file_path: str, folder_id: str, mime_type: str | None = None, new_name: str | None = None):
    """
    공유된 폴더/공유 드라이브에 파일 업로드.
    - folder_id: 업로드 대상 폴더 ID
    - mime_type: 지정 시 MIME 타입 고정(예: 'application/pdf'); 생략 가능
    - new_name: 드라이브에 보일 파일명(미지정 시 로컬 파일명)
    """
    service = get_drive_service()
    file_name = new_name or os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)

    file_metadata = {
        "name": file_name,
        "parents": [folder_id],
    }

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name, mimeType, parents",
        supportsAllDrives=True,  # 공유 드라이브 대응
    ).execute()

    print(f"Uploaded: {created['name']} (id={created['id']})")
    return created

if __name__ == "__main__":
    TARGET_FOLDER_ID = "1XHUPS-mBu5DH4Dr1BivcyuJw1TevO9CV"  # 예: "1AbCDeFgHiJk..."
    upload_file("hello.txt", TARGET_FOLDER_ID)
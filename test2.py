from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SERVICE_ACCOUNT_FILE = "choi.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = "1XHUPS-mBu5DH4Dr1BivcyuJw1TevO9CV"  # 방금 접근 OK 나온 폴더 ID

# 인증/클라이언트
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive = build("drive", "v3", credentials=creds)

# 테스트 파일 생성
local_path = "upload_test.txt"
with open(local_path, "w", encoding="utf-8") as f:
    f.write("업로드 테스트 - 서비스 계정\n")

# 업로드
meta = {"name": "shared_or_mydrive_upload_test.txt", "parents": [FOLDER_ID]}
media = MediaFileUpload(local_path, mimetype="text/plain", resumable=True)

created = drive.files().create(
    body=meta,
    media_body=media,
    fields="id,name,webViewLink,parents",
    supportsAllDrives=True,  # 공유 문서함이든 My Drive든 안전하게 True 권장
).execute()

print("✅ 업로드 완료:", created["name"])
print("🔗 링크:", created["webViewLink"])
print("📁 부모:", created.get("parents"))
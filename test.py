from __future__ import print_function
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
# 최소 권한(파일 생성/소유): drive.file
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_drive():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def upload_to_my_drive(local_path, target_name=None, parent_folder_id=None):
    drive = get_drive()
    target_name = target_name or os.path.basename(local_path)
    media = MediaFileUpload(local_path, resumable=True)
    meta = {'name': target_name}
    if parent_folder_id:
        meta['parents'] = [parent_folder_id]  # 내 드라이브의 특정 폴더에 넣고 싶을 때
    file = drive.files().create(
        body=meta, media_body=media, fields='id, name, webViewLink'
    ).execute()
    print('✅ Uploaded:', file['name'], file['id'], file.get('webViewLink', ''))
    return file

if __name__ == '__main__':
    # 샘플 파일 만들기
    with open('hello.txt', 'w', encoding='utf-8') as f:
        f.write('안녕하세요, 업로드 테스트입니다.\n')

    upload_to_my_drive('hello.txt', target_name='upload-test-hello.txt')
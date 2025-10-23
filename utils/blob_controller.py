import os
import io
import mimetypes
import tempfile
from pathlib import Path

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient,
    generate_blob_sas,
    BlobSasPermissions,
    ContentSettings,
)

class blob_controller:
    def __init__(self, conn: str, container: str):
        if not conn:
            raise ValueError("Azure connection string이 비어 있습니다.")
        if not container or container.lower() != container:
            raise ValueError("컨테이너 이름은 소문자여야 합니다. (예: 'meritz-data')")

        self.conn = conn
        self.container = container
        self.blob_service = BlobServiceClient.from_connection_string(conn)
        self.container_client = self.blob_service.get_container_client(container)

        # 컨테이너 없으면 생성, 있으면 무시
        self.ensure_container_exists()

    def ensure_container_exists(self):
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass 


    def upload_pdf_to_blob(self, file_path: str) -> str:
        """
        로컬 PDF 파일을 업로드. blob_name을 넘기지 않으면  basename으로 업로드.
        """
        name = os.path.basename(file_path)
        blob_client: BlobClient = self.container_client.get_blob_client(name)
        with open(file_path, "rb") as f:
            blob_client.upload_blob(
                f,
                overwrite=True,
                max_concurrency=3,
                timeout=600,
                content_settings=ContentSettings(content_type="application/pdf"),
            )
        return name
    
    
    def list_files(self):
        """
        특정 prefix 하위의 blob 목록 반환
        """
        blobs = self.container_client.list_blobs()
        file_list = [b.name for b in blobs]
        return file_list
    
    def download_to_temp(self, blob_name: str, tmp_dir: Path) -> Path:
        """
        blob_name(컨테이너 내 경로)을 tmp_dir에 파일로 저장하고 그 경로 반환
        """
        tmp_dir.mkdir(parents=True, exist_ok=True)           
        local_path = tmp_dir / Path(blob_name).name

        bc: BlobClient = self.container_client.get_blob_client(blob_name)  
        try:
            stream = bc.download_blob(max_concurrency=4)    
            with open(local_path, "wb") as f:
                for chunk in stream.chunks():
                    f.write(chunk)
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Blob not found: {blob_name}")

        return local_path
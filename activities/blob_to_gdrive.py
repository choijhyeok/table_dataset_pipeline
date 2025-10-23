
import os
import io
import mimetypes
import tempfile
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

from application.app import app
from utils.blob_controller import blob_controller
from utils.logging_utils import gdrive_handler


@app.function_name(name="blob_to_gdrive")
@app.activity_trigger(input_name="activity_payload")
def blob_to_gdrive(activity_payload):
    try:
        prefix = activity_payload.get("blob_prefix")
        gdrive_folder_id = activity_payload.get("gdrive_folder_id")
        
        blob_client = blob_controller(
            conn = os.getenv("azure-blob-connection-string"),
            container = os.getenv("azure-blob-container-name"),
            prefix = prefix
        )
        
        gdrive_client = gdrive_handler(
            credentials_json=os.getenv("gdrive-credentials-json")
        )


        blob_to_gdrive_main(    
            prefix=prefix,
            gdrive_folder_id=gdrive_folder_id,
            blob_client=blob_client,
            gdrive_client=gdrive_client,
        )
        
        return {"success": True}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    
    
def blob_to_gdrive_main(prefix: str, gdrive_folder_id: str, blob_client: blob_controller, gdrive_client: gdrive_handler):
    try:
        with tempfile.TemporaryDirectory(prefix="blob2gdrive_") as td:
            tmp_dir = Path(td)

            blob_names = blob_client.list_blob_names()
            
            results = []
            for name in blob_names:

                local_path = blob.download_to_temp(name, tmp_dir)

                created = upload_to_gdrive_folder(
                    drive=drive,
                    local_path=local_path,
                    folder_id=GDRIVE_FOLDER_ID,
                    target_name=local_path.name,
                )
                results.append(created)
                
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

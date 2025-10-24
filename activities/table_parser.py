from pathlib import Path
import csv
import io
import os
import time
from datetime import datetime
from tempfile import TemporaryDirectory
import tempfile
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceNotFoundError
from application.app import app
from utils.blob_controller import blob_controller
import fitz

import logging


# Blob에서 PDF를 임시파일 없이 스트림으로 열기
def open_pdf_from_blob_stream(bc: blob_controller, blob_name: str):
    """
    디스크 임시파일 없이 Blob을 스트리밍으로 읽어 메모리/스풀 버퍼에 적재한 뒤,
    fitz.open(stream=..., filetype="pdf")로 바로 연다.
    - 작은/중간 사이즈 PDF: 메모리에서 처리
    - 큰 PDF: SpooledTemporaryFile 이 자동으로 디스크로 스필오버
    """

    spooled = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)  # 64MB까지 메모리, 초과 시 디스크 스필오버
    try:
        blob_client = bc.container_client.get_blob_client(blob_name)
        stream = blob_client.download_blob(max_concurrency=4)
        for chunk in stream.chunks():
            if chunk:
                spooled.write(chunk)
        spooled.seek(0)
        data = spooled.read()
        return fitz.open(stream=data, filetype="pdf")
    finally:
        try:
            spooled.close()
        except Exception:
            pass


@app.function_name(name="pdf_list")
@app.activity_trigger(input_name="payload")
def pdf_list(payload):
    conn = os.getenv("azure-blob-connection-string")
    container = payload["container"]
    bc = blob_controller(conn=conn, container=container)
    return [n for n in bc.list_files() if n.lower().endswith(".pdf")]


@app.function_name(name="pdf_to_png_md")
@app.activity_trigger(input_name="activity_payload")
def pdf_to_png_md(activity_payload):
    try:
        url = activity_payload.get("url")  
        container = activity_payload.get("container")                    
        image_container = activity_payload.get("image_container") 
        markdown_container = activity_payload.get("markdown_container") 
        history_container = activity_payload.get("history_container")
        category = activity_payload.get("category")

        conn = os.getenv("azure-blob-connection-string")

        data_bc = blob_controller(conn=conn, container=container)                 # PDF 
        img_bc = blob_controller(conn=conn, container=image_container)            # PNG
        md_bc = blob_controller(conn=conn, container=markdown_container)          # MD
        hist_bc = blob_controller(conn=conn, container=history_container)         # history.csv (나중에 gradio 에서 쓸거)

        processed = pdf_parser(data_bc, img_bc, md_bc, hist_bc, category)

        return {"success": True, "processed": processed}
    except Exception as e:
        return {"success": False, "error": str(e)}
    


def pdf_parser(data_bc: blob_controller, img_bc: blob_controller, md_bc: blob_controller, hist_bc: blob_controller, category: str = "") -> int:
    """
    - Blob 컨테이너에 있는 모든 PDF를 temp로 받아서 페이지 순회
    - 각 페이지에서 표가 있으면 표 영역 전체를 하나로 합쳐 crop → PNG 저장
    - 같은 페이지의 표들을 Markdown으로 합쳐 저장
    - 두 파일(PNG, MD) 모두 생성/업로드 성공 시에만 history.csv에 한 줄 append
    - 반환: 처리(히스토리에 기록된) 건수
    """
    processed = 0  # md/png 페어 성공 건수
    with TemporaryDirectory() as download_dir_str:
        download_dir = Path(download_dir_str)
        blobs = data_bc.list_files()

        for blob_name in blobs:
            if not blob_name.lower().endswith(".pdf"):
                continue
            try:
                doc = open_pdf_from_blob_stream(data_bc, blob_name)
            except Exception:
                continue
            base = os.path.splitext(os.path.basename(blob_name))[0]

            try:
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)

                    # 표 탐지
                    try:
                        table_finder = page.find_tables()
                        tables = table_finder.tables if hasattr(table_finder, "tables") else []
                    except Exception:
                        tables = []

                    if not tables:
                        continue  # 표 없으면 pass


                    for idx, t in enumerate(tables, start=1):

                        table_rect = fitz.Rect(t.bbox)

                        png_name = f"{base}_p{page_num + 1}_t{idx}.png"
                        md_name  = f"{base}_p{page_num + 1}_t{idx}.md"
                        png_path = download_dir / png_name
                        md_path  = download_dir / md_name

                        # PNG 생성 (표 bbox만 clip)
                        try:
                            pix = page.get_pixmap(dpi=300, clip=table_rect)
                            pix.save(str(png_path))
                            png_ok = True
                        except Exception as e:
                            png_ok = False

                        # MD 생성 (해당 표만)
                        try:
                            md_text = t.to_markdown()
                            md_path.write_text(md_text, encoding="utf-8")
                            md_ok = True
                        except Exception as e:
                            md_ok = False

                        # 둘 다 성공시에만 업로드 + history 반영
                        if png_ok and md_ok:
                            try:
                                # PNG 업로드 (image 컨테이너)
                                png_bc = img_bc.container_client.get_blob_client(png_name)
                                with open(png_path, "rb") as f:
                                    png_bc.upload_blob(
                                        f,
                                        overwrite=True,
                                        max_concurrency=3,
                                        timeout=600,
                                        content_settings=ContentSettings(content_type="image/png"),
                                    )

                                # MD 업로드 (markdown 컨테이너)
                                md_blob = md_bc.container_client.get_blob_client(md_name)
                                with open(md_path, "rb") as f:
                                    md_blob.upload_blob(
                                        f,
                                        overwrite=True,
                                        max_concurrency=3,
                                        timeout=600,
                                        content_settings=ContentSettings(content_type="text/markdown"),
                                    )
                                # history.csv 업데이트 (없으면 생성)
                                history_blob = "history.csv"
                                hist_client = hist_bc.container_client.get_blob_client(history_blob)

                                header = ["original_pdf", "png_name", "md_name", "page", "category", "table_index"]
                                rows = []
                                try:
                                    existing = hist_client.download_blob(max_concurrency=2).readall().decode("utf-8")
                                    reader = csv.reader(existing.splitlines())
                                    rows.extend(list(reader))
                                    if not rows or rows[0] != header:
                                        rows.insert(0, header)  # 헤더 보정
                                except ResourceNotFoundError:
                                    rows.append(header)

                                rows.append([blob_name, png_name, md_name, str(page_num + 1), category, str(idx)])

                                # CSV 재생성 후 업로드(덮어쓰기)
                                byte_buf = io.BytesIO()
                                text_wr = io.TextIOWrapper(byte_buf, encoding="utf-8", newline="")
                                writer = csv.writer(text_wr, quoting=csv.QUOTE_ALL)
                                for r in rows:
                                    writer.writerow(r)
                                text_wr.flush()
                                byte_buf.seek(0)

                                hist_client.upload_blob(
                                    byte_buf.getvalue(),
                                    overwrite=True,
                                    max_concurrency=2,
                                    timeout=600,
                                    content_settings=ContentSettings(content_type="text/csv"),
                                )
                                text_wr.close()
                                byte_buf.close()

                                processed += 1
                            finally:
                                # temp 정리
                                try: png_path.unlink(missing_ok=True)
                                except Exception: pass
                                try: md_path.unlink(missing_ok=True)
                                except Exception: pass
                        else:
                            # 실패했으면 생성된 파일이 있다면 정리
                            try: png_path.unlink(missing_ok=True)
                            except Exception: pass
                            try: md_path.unlink(missing_ok=True)
                            except Exception: pass

            finally:
                try: doc.close()
                except Exception: pass


    return processed
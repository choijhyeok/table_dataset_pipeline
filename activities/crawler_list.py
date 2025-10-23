import os
import time
from datetime import datetime
from tempfile import TemporaryDirectory

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

from application.app import app
from utils.blob_controller import blob_controller

import logging



@app.function_name(name="get_cnt")
@app.activity_trigger(input_name="activity_payload")
def get_cnt(activity_payload):
    try:
        url = activity_payload.get("url")

        count = meritz_cnt(url)
        return {"success": True, "count": count}  # 임시로 42 반환
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.function_name(name="meritz_crawler")
@app.activity_trigger(input_name="activity_payload")
def meritz_crawler(activity_payload):
    try:
        url = activity_payload.get("url")
        container = activity_payload.get("container")
        cnt = activity_payload.get("cnt")

        blob_client = blob_controller(
            conn = os.getenv("azure-blob-connection-string"),
            container = container
        )
        meritz_crawler_main(url, cnt, blob_client)
        
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}
    



def meritz_cnt(url: str) -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chrome")  
        ctx = browser.new_context(accept_downloads=True)
        page: Page = ctx.new_page()
        page.set_default_timeout(25_000)

        total_first = 0
        try:
            page.goto(url, wait_until="load")
            page.wait_for_load_state()

            for sel in ["xpath=/html/body/div[3]", "div.prod_category", "div.jspPane"]:
                try:
                    page.wait_for_selector(sel, state="visible", timeout=10_000)
                except Exception:
                    pass

            first_category = page.locator(
                "xpath=/html/body/div[3] >> div.prod_category >> "
                "div.prod_list_inner.txtarea >> div.jspPane >> li"
            )
            try:
                total_first = first_category.count()
            except Exception:
                total_first = 0

        except Exception:
            total_first = 0

        finally:
            try: ctx.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    return total_first




def meritz_crawler_main(url: str, cnt: int, blob_client: blob_controller) -> int:
    """
    Playwright로 PDF 다운로드 → temp 보관 → Blob 업로드
    return: 업로드한 파일 개수
    """
    uploaded = 0

    with TemporaryDirectory() as tmpdir:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, channel="chrome")
            ctx = browser.new_context(
                accept_downloads=True,
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
            )
            page: Page = ctx.new_page()
            page.set_default_timeout(25_000)

            try:
                page.goto(url, wait_until="load")
                page.wait_for_load_state()

                # 1차 카테고리
                first_category = page.locator(
                    "xpath=/html/body/div[3] >> div.prod_category >> "
                    "div.prod_list_inner.txtarea >> div.jspPane >> li"
                )
                try:
                    total_first = first_category.count()
                except Exception:
                    total_first = 0

                # 인덱스 범위 체크: 잘못되면 바로 종료(continue 금지)
                i = int(cnt)
                if i < 0 or i >= max(0, total_first):
                    return 0

                # i번째 클릭
                item = first_category.nth(i)
                try: item.scroll_into_view_if_needed()
                except Exception: pass
                item.click(timeout=8_000)

                # 2차 카테고리
                second_category = page.locator(
                    "xpath=/html/body/div[3] >> div.prod_detail >> "
                    "div.prod_list_inner.txtarea >> div.jspPane >> li"
                )
                try:
                    total_second = second_category.count()
                except Exception:
                    total_second = 0

                for j in range(total_second):
                    try:
                        elem = second_category.nth(j)
                        try: elem.scroll_into_view_if_needed()
                        except Exception: pass
                        elem.click(timeout=8_000)

                        # 다운로드 버튼
                        download_btn = page.locator(
                            "xpath=/html/body/div[3]/div[3]/div[4]/table/tbody/tr[1]/td[4]/a"
                        )
                        download_btn.wait_for(state="visible", timeout=8_000)

                        # 다운로드 → temp 저장
                        with page.expect_download(timeout=15_000) as dl_info:
                            download_btn.click()
                        download = dl_info.value

                        suggested = download.suggested_filename or "file.pdf"
                        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fname = f"{stamp}_{suggested}"
                        temp_save_path = os.path.join(tmpdir, fname)
                        download.save_as(temp_save_path)

                        # Blob 업로드
                        try:
                            logging.info(f"Uploading to blob: {temp_save_path}")
                            blob_client.upload_pdf_to_blob(temp_save_path)
                            uploaded += 1
                        finally:
                            try: os.remove(temp_save_path)
                            except Exception: pass

                        time.sleep(0.1)

                    except Exception as e:
                        logging.warning(f"second_category[{j}] error: {e}")
                        # 계속 진행

            except Exception as e:
                # 여기선 continue 사용 금지. 함수 종료/로그만.
                logging.exception(f"meritz_crawler_main outer error: {e}")
                return uploaded

            finally:
                try: ctx.close()
                except Exception: pass
                try: browser.close()
                except Exception: pass

    return uploaded
import os
import time
import urllib.parse
from datetime import datetime
from typing import List, Dict, Optional

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

def meritz_pdf_main(
    url: str = "https://www.meritzfire.com/disclosure/product-announcement/product-list.do?vMode=PC#!/",
    download_dir: str = "./downloads",
):
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, channel="chrome")
        # ✅ 임시 디렉터리에만 두지 말고 다운로드 이벤트를 받을 수 있도록
        ctx = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        )

        page: Page = ctx.new_page()
        page.set_default_timeout(100_000)

        page.goto(url)
        page.wait_for_load_state()

        # 초기 영역 로딩 대기
        page.locator("xpath=/html/body/div[3]").wait_for()

        # 1차 카테고리
        first_category = page.locator(
            "xpath=/html/body/div[3] >> div.prod_category >> div.prod_list_inner.txtarea >> div.jspPane >> li"
        )
        total_first = first_category.count()
        print(f"Total first category items: {total_first}")

        for i in range(total_first):
            first_item = first_category.nth(i)
            first_item.click()

            # 2차 카테고리
            second_category = page.locator(
                "xpath=/html/body/div[3] >> div.prod_detail >> div.prod_list_inner.txtarea >> div.jspPane >> li"
            )
            total_second = second_category.count()
            print(f"Total second category items: {total_second}")

            for j in range(total_second):
                second_category.nth(j).click()

                # ▼ ‘다운로드’로 예상되는 버튼/링크
                download_btn = page.locator("xpath=/html/body/div[3]/div[3]/div[4]/table/tbody/tr[1]/td[4]/a")
                download_btn.wait_for()

                # --- 1) 정상적인 'download' 이벤트 시나리오
                try:
                    with page.expect_download(timeout=30_000) as dl_info:
                        download_btn.click()  # 이 클릭이 진짜 다운로드를 트리거해야 함
                    download = dl_info.value

                    # 파일명 제안 얻기
                    suggested = download.suggested_filename
                    # 저장 경로 지정
                    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    fname = f"{stamp}_{suggested}" if suggested else f"{stamp}.pdf"
                    save_path = os.path.join(download_dir, fname)
                    download.save_as(save_path)

                    print(f"✅ Saved by download event: {save_path}")
                    # 다음 아이템으로
                    continue

                except PlaywrightTimeoutError:
                    # 'download' 이벤트가 발생하지 않은 경우 (blob/inline/pdf-viewer/팝업 등)
                    print("⚠️ No 'download' event. Trying popup/response fallback...")

                # --- 2) 팝업 창으로 열리는 경우
                try:
                    with page.expect_popup(timeout=5_000) as pop_info:
                        download_btn.click()
                    pdf_page = pop_info.value
                    pdf_page.wait_for_load_state()

                    # 주소가 .pdf면 바로 요청해서 저장
                    pdf_url = pdf_page.url
                    if pdf_url.lower().endswith(".pdf"):
                        # 응답 스니핑으로 바디 저장
                        # 팝업 페이지에서 네트워크 응답을 기다렸다가 pdf content-type이면 저장
                        resp = pdf_page.wait_for_event(
                            "response",
                            predicate=lambda r: r.url == pdf_url,
                            timeout=10_000
                        )
                        if "application/pdf" in (resp.headers.get("content-type", "").lower()):
                            body = resp.body()
                            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            save_path = os.path.join(download_dir, f"{stamp}_popup.pdf")
                            with open(save_path, "wb") as f:
                                f.write(body)
                            print(f"✅ Saved via popup response: {save_path}")
                            pdf_page.close()
                            continue

                    # blob/inline 뷰어면 response 방식으로 잡기
                    resp = pdf_page.wait_for_event(
                        "response",
                        predicate=lambda r: "application/pdf" in r.headers.get("content-type", "").lower(),
                        timeout=10_000
                    )
                    body = resp.body()
                    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    save_path = os.path.join(download_dir, f"{stamp}_popup_inline.pdf")
                    with open(save_path, "wb") as f:
                        f.write(body)
                    print(f"✅ Saved via popup inline PDF: {save_path}")
                    pdf_page.close()
                    continue

                except PlaywrightTimeoutError:
                    pass
                except Exception as e:
                    print(f"popup fallback error: {e}")

                # --- 3) 같은 탭에서 inline/stream으로 열리는 경우 (response sniff)
                try:
                    # 클릭 후 'application/pdf' 응답을 대기
                    download_btn.click()
                    resp = page.wait_for_event(
                        "response",
                        predicate=lambda r: "application/pdf" in r.headers.get("content-type", "").lower(),
                        timeout=10_000
                    )
                    body = resp.body()
                    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    save_path = os.path.join(download_dir, f"{stamp}_inline.pdf")
                    with open(save_path, "wb") as f:
                        f.write(body)
                    print(f"✅ Saved via inline response: {save_path}")
                    continue

                except PlaywrightTimeoutError:
                    print("❌ Could not capture as download/popup/inline. Skipping this item.")
                except Exception as e:
                    print(f"inline fallback error: {e}")

            # 데모용 살짝 대기
            time.sleep(1.0)

        ctx.close()
        browser.close()


if __name__ == "__main__":
    meritz_pdf_main()
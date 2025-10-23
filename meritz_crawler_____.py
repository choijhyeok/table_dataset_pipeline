import os
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def meritz_pdf_main(
    url: str = "https://www.meritzfire.com/disclosure/product-announcement/product-list.do?vMode=PC#!/",
    download_dir: str = "./downloads",
):
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chrome")
        ctx = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        )
        page: Page = ctx.new_page()
        page.set_default_timeout(25_000)  # 기본 타임아웃

        try:
            page.goto(url, wait_until="load")
            page.wait_for_load_state()

            for sel in ["xpath=/html/body/div[3]", "div.prod_category", "div.jspPane"]:
                try:
                    page.wait_for_selector(sel, state="visible", timeout=10_000)
                except Exception as e:
                    print(f"{ts()} WARN init wait failed: {sel} -> {e}")

            # 1차 카테고리
            first_category = page.locator(
                "xpath=/html/body/div[3] >> div.prod_category >> div.prod_list_inner.txtarea >> div.jspPane >> li"
            )
            try:
                total_first = first_category.count()
            except Exception as e:
                print(f"{ts()} ERROR count first_category: {e}")
                total_first = 0

            print(f"{ts()} Total first category items: {total_first}")

            for i in range(total_first):
                abort_second = False  # ❗이 i에서 j 중 하나라도 실패하면 이걸 True로 바꿈
                try:
                    print(f"{ts()} >>> First category index {i}")
                    item = first_category.nth(i)
                    try:
                        item.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    try:
                        item.click(timeout=8_000)  # 짧게 시도
                    except Exception as e:
                        print(f"{ts()} WARN first click failed idx={i}: {e}")
                        continue  # 다음 i로

                    # 2차 카테고리
                    second_category = page.locator(
                        "xpath=/html/body/div[3] >> div.prod_detail >> div.prod_list_inner.txtarea >> div.jspPane >> li"
                    )
                    try:
                        total_second = second_category.count()
                    except Exception as e:
                        print(f"{ts()} ERROR count second_category: {e}")
                        total_second = 0

                    print(f"{ts()}   Total second category items: {total_second}")

                    for j in range(total_second):
                        print(f"{ts()}   >>> Second category index {j}")
                        try:
                            elem = second_category.nth(j)
                            try:
                                elem.scroll_into_view_if_needed()
                            except Exception:
                                pass

                            # ⬇️ 여기서 실패 한 번이면 바로 abort
                            elem.click(timeout=8_000)

                            # 다운로드 버튼 대기 (실패하면 abort → 다음 i)
                            download_btn = page.locator("xpath=/html/body/div[3]/div[3]/div[4]/table/tbody/tr[1]/td[4]/a")
                            download_btn.wait_for(state="visible", timeout=8_000)

                            # 다운로드 시도 (실패해도 abort)
                            try:
                                with page.expect_download(timeout=15_000) as dl_info:
                                    download_btn.click()
                                download = dl_info.value
                                suggested = download.suggested_filename
                                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                fname = f"{stamp}_{suggested}" if suggested else f"{stamp}.pdf"
                                save_path = os.path.join(download_dir, fname)
                                download.save_as(save_path)
                                print(f"{ts()}   ✅ Saved: {save_path}")
                            except PlaywrightTimeoutError as e:
                                print(f"{ts()}   INFO no download event (blob/viewer?) → abort this i. {e}")
                                abort_second = True
                                break
                            except Exception as e:
                                print(f"{ts()}   ERROR download failed → abort this i. {e}")
                                abort_second = True
                                break

                            time.sleep(0.15)

                        except Exception as e:
                            # j에서 **한 번이라도** 실패 → 남은 j 패스하고 다음 i로
                            print(f"{ts()}   WARN second step failed i={i}, j={j}: {e} → abort this i")
                            abort_second = True
                            break

                    # j 루프 끝나고 abort면 다음 i로
                    if abort_second:
                        continue

                    time.sleep(0.4)

                except Exception as e:
                    print(f"{ts()} ERROR in first loop i={i}: {e}")
                    continue

        finally:
            try:
                ctx.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


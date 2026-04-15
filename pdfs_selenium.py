import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

HEADLESS = True
BASE_URL = "https://www.saudiexchange.sa"


def start_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


def click_financial_statements_tab(driver):
    """Click the 'Financial Statements and Reports' tab, searching iframes too."""
    wait = WebDriverWait(driver, 15)
    driver.switch_to.default_content()

    def search_frames(depth=0, max_depth=8):
        if depth > max_depth:
            return False
        # Try clicking the tab in current frame context
        for xp in [
            "//*[@id='finacialStatementAndReports']",
            "//*[normalize-space()='FINANCIAL STATEMENTS AND REPORTS']",
            "//*[contains(normalize-space(), 'FINANCIAL STATEMENTS')]",
        ]:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                try:
                    wait.until(EC.element_to_be_clickable(els[0])).click()
                    return True
                except Exception:
                    try:
                        driver.execute_script("arguments[0].click();", els[0])
                        return True
                    except Exception:
                        pass
        # Recurse into iframes
        for f in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                driver.switch_to.frame(f)
                if search_frames(depth + 1, max_depth):
                    return True
                driver.switch_to.parent_frame()
            except Exception:
                try:
                    driver.switch_to.parent_frame()
                except Exception:
                    pass
        return False

    return search_frames()


def scrape_pdf_links(input_url: str) -> list[str]:
    driver = start_driver()
    try:
        print("[step] Loading page...")
        driver.get(input_url)
        time.sleep(3)

        print("[step] Clicking 'Financial Statements and Reports' tab...")
        clicked = click_financial_statements_tab(driver)
        if not clicked:
            print("[warn] Could not find the tab — scraping whatever is loaded.")
        else:
            print("[ok] Tab clicked.")

        # Wait for .inner_tab_sub to appear after the click
        print("[step] Waiting for content to load...")
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".inner_tab_sub"))
            )
        except Exception:
            print("[warn] Timed out waiting for .inner_tab_sub — trying anyway.")

        time.sleep(2)  # extra buffer for dynamic content

        # Search main doc + iframes for .inner_tab_sub
        def extract_links_from_current_frame():
            soup = BeautifulSoup(driver.page_source, "lxml")
            container = soup.find(class_="inner_tab_sub")
            if not container:
                return []
            return [
                BASE_URL + a["href"]
                for a in container.find_all("a", class_="btn-pdf")
                if a.get("href")
            ]

        driver.switch_to.default_content()
        links = extract_links_from_current_frame()

        # If nothing found, search iframes
        if not links:
            print("[debug] Nothing in main doc, searching iframes...")
            for frame in driver.find_elements(By.TAG_NAME, "iframe"):
                try:
                    driver.switch_to.frame(frame)
                    links = extract_links_from_current_frame()
                    driver.switch_to.parent_frame()
                    if links:
                        print(f"[ok] Found links in iframe.")
                        break
                except Exception:
                    try:
                        driver.switch_to.parent_frame()
                    except Exception:
                        pass

        print(f"[ok] Found {len(links)} PDF links.")
        return links

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    url = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/!ut/p/z1/jZBLb4JQEIV_iwuXdUYUuHZHNcUHCEhahY250imQgtdeLpL01xftphr7mMxmZr6TnDkQwwbiPT_mKVe52POinaPY2OqWgdqUoccmkzEGjws2naOnoWHC-hLA0NdbwHcHDq7QRgPi_-jxh7Lwb318hbi2gcHSCjzN1BFD7Rq4YfEM_OJhDnFaiN1XHtZ-N2ApxJJeSZLs1bJdZ0odqvsudrFpml4qRFpQLxFlF29JMlEp2FyS5yTM7cy3h_0Z0xa2FY7RCA1z9TCyEBnCWlIlapkQrFJSLs_3S1HWicvlG6kJKZ4XFQQJTzJy6EiFz1OC8PRdXjVcJZmTVyqkghJFLxAtvdOpaichfS55SYokRKflFqK-qRuMDYdMHw3PESq-K-g5p-Yb2z8dJL3XVClHJLwgiLiEQ_m0wdwv10zdRePdB7UdWZ3OJ3E73tY!/dz/d5/L0lHSklKQ1NDbENsQ1FvS1VRb2dwUkNpQ2xFaVEvWU9ZRUFBSU1FQUFBRUVNQ01LR0lNQU9FT0JFQkVKRk5GTkpGRERMRExISU1FRFBQQXZBblBDS0EvNEpDaWpLMWJHTGppRUVwTWhTVFVVMXUybHNacVdhM2JTMjFWRktxaXBBISEvWjdfNUE2MDJIODBPMFZDNDA2ME80R01MODFHNTUvWjZfNUE2MDJIODBPR0YyRTBRRjlCUURFRzEwSzQvdmlldy9ub3JtYWwvbGFuZy9lbi9odHRwOiUwJTB0YWRhd3VsJTAvY29tcGFueVN5bWJvbC8yMDMw/?locale=en"
    pdf_links = scrape_pdf_links(url)
    for link in pdf_links:
        print(link)
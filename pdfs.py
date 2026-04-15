import asyncio
from playwright.async_api import async_playwright


async def scrape_document_links(input_url: str) -> list[str]:
    base_url = "https://www.saudiexchange.sa"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Step 1: Load the page
        await page.goto(input_url, wait_until="networkidle", timeout=60000)

        # Step 2: Take a screenshot to verify what loaded
        await page.screenshot(path="debug.png", full_page=True)
        print("Screenshot saved to debug.png")

        # Step 3: Search all frames for the tab element
        target_frame = None
        for f in page.frames:
            el = await f.query_selector("#finacialStatementAndReports")
            if el:
                target_frame = f
                print(f"Found tab in frame: {f.url}")
                break

        if target_frame is None:
            # Print all frame URLs to help debug
            print("Available frames:")
            for f in page.frames:
                print(f"  - {f.url}")
            raise Exception("Could not find #finacialStatementAndReports in any frame")

        # Step 4: Click the tab and wait for content
        await target_frame.click("#finacialStatementAndReports")
        await target_frame.wait_for_selector(".inner_tab_sub", timeout=15000)

        # Step 5: Extract links
        links = await target_frame.eval_on_selector_all(
            ".inner_tab_sub td a.btn-pdf",
            "elements => elements.map(el => el.getAttribute('href'))"
        )

        await browser.close()

        full_urls = [
            base_url + href
            for href in links
            if href is not None
        ]

        return full_urls


def scrape_links(input_url: str) -> list[str]:
    return asyncio.run(scrape_document_links(input_url))


if __name__ == "__main__":
    url = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/!ut/p/z1/jZBLb4JQEIV_iwuXdUYUuHZHNcUHCEhahY250imQgtdeLpL01xftphr7mMxmZr6TnDkQwwbiPT_mKVe52POinaPY2OqWgdqUoccmkzEGjws2naOnoWHC-hLA0NdbwHcHDq7QRgPi_-jxh7Lwb318hbi2gcHSCjzN1BFD7Rq4YfEM_OJhDnFaiN1XHtZ-N2ApxJJeSZLs1bJdZ0odqvsudrFpml4qRFpQLxFlF29JMlEp2FyS5yTM7cy3h_0Z0xa2FY7RCA1z9TCyEBnCWlIlapkQrFJSLs_3S1HWicvlG6kJKZ4XFQQJTzJy6EiFz1OC8PRdXjVcJZmTVyqkghJFLxAtvdOpaichfS55SYokRKflFqK-qRuMDYdMHw3PESq-K-g5p-Yb2z8dJL3XVClHJLwgiLiEQ_m0wdwv10zdRePdB7UdWZ3OJ3E73tY!/dz/d5/L0lHSklKQ1NDbENsQ1FvS1VRb2dwUkNpQ2xFaVEvWU9ZRUFBSU1FQUFBRUVNQ01LR0lNQU9FT0JFQkVKRk5GTkpGRERMRExISU1FRFBQQXZBblBDS0EvNEpDaWpLMWJHTGppRUVwTWhTVFVVMXUybHNacVdhM2JTMjFWRktxaXBBISEvWjdfNUE2MDJIODBPMFZDNDA2ME80R01MODFHNTUvWjZfNUE2MDJIODBPR0YyRTBRRjlCUURFRzEwSzQvdmlldy9ub3JtYWwvbGFuZy9lbi9odHRwOiUwJTB0YWRhd3VsJTAvY29tcGFueVN5bWJvbC8yMDMw/?locale=en"
    document_links = scrape_links(url)
    for link in document_links:
        print(link)
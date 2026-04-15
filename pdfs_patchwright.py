import asyncio
import random
from patchright.async_api import async_playwright


def random_delay(a=0.5, b=2.0):
    return asyncio.sleep(random.uniform(a, b))


async def scrape_document_links(input_url: str) -> list[str]:
    base_url = "https://www.saudiexchange.sa"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="Australia/Sydney",
        )

        page = await context.new_page()

        # --- STEALTH JS PATCHES ---
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            window.chrome = {
                runtime: {}
            };
        """)

        await page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        })

        await page.goto(input_url, wait_until="domcontentloaded", timeout=60000)
        await random_delay(1, 3)

        await page.mouse.wheel(0, random.randint(300, 800))
        await random_delay()

        # screenshot
        await page.screenshot(path="debug.png", full_page=True)

        target_frame = None
        for frame in page.frames:
            try:
                el = await frame.query_selector("#finacialStatementAndReports")
                if el:
                    target_frame = frame
                    break
            except:
                continue

        if target_frame is None:
            print("Frames:")
            for f in page.frames:
                print(f.url)
            raise Exception("Target element not found")

        tab = await target_frame.query_selector("#finacialStatementAndReports")
        await tab.hover()
        await random_delay(0.5, 1.5)
        await tab.click()

        await target_frame.wait_for_selector(".inner_tab_sub", timeout=15000)
        await random_delay()

        links = await target_frame.eval_on_selector_all(
            ".inner_tab_sub td a.btn-pdf",
            "els => els.map(el => el.getAttribute('href'))"
        )

        await browser.close()

        return [
            base_url + href
            for href in links if href
        ]


def scrape_links(input_url: str) -> list[str]:
    return asyncio.run(scrape_document_links(input_url))


if __name__ == "__main__":
    url = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/!ut/p/z1/jZBLb4JQEIV_iwuXdUYUuHZHNcUHCEhahY250imQgtdeLpL01xftphr7mMxmZr6TnDkQwwbiPT_mKVe52POinaPY2OqWgdqUoccmkzEGjws2naOnoWHC-hLA0NdbwHcHDq7QRgPi_-jxh7Lwb318hbi2gcHSCjzN1BFD7Rq4YfEM_OJhDnFaiN1XHtZ-N2ApxJJeSZLs1bJdZ0odqvsudrFpml4qRFpQLxFlF29JMlEp2FyS5yTM7cy3h_0Z0xa2FY7RCA1z9TCyEBnCWlIlapkQrFJSLs_3S1HWicvlG6kJKZ4XFQQJTzJy6EiFz1OC8PRdXjVcJZmTVyqkghJFLxAtvdOpaichfS55SYokRKflFqK-qRuMDYdMHw3PESq-K-g5p-Yb2z8dJL3XVClHJLwgiLiEQ_m0wdwv10zdRePdB7UdWZ3OJ3E73tY!/dz/d5/L0lHSklKQ1NDbENsQ1FvS1VRb2dwUkNpQ2xFaVEvWU9ZRUFBSU1FQUFBRUVNQ01LR0lNQU9FT0JFQkVKRk5GTkpGRERMRExISU1FRFBQQXZBblBDS0EvNEpDaWpLMWJHTGppRUVwTWhTVFVVMXUybHNacVdhM2JTMjFWRktxaXBBISEvWjdfNUE2MDJIODBPMFZDNDA2ME80R01MODFHNTUvWjZfNUE2MDJIODBPR0YyRTBRRjlCUURFRzEwSzQvdmlldy9ub3JtYWwvbGFuZy9lbi9odHRwOiUwJTB0YWRhd3VsJTAvY29tcGFueVN5bWJvbC8yMDMw/?locale=en"
    links = scrape_links(url)
    for l in links:
        print(l)
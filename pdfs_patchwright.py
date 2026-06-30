import asyncio
import csv
import random
from pathlib import Path

from patchright.async_api import async_playwright


BASE_URL = "https://www.saudiexchange.sa"


async def random_delay(a=1.0, b=3.0):
    await asyncio.sleep(random.uniform(a, b))


# ------------------------------------------------------------------
# Browser setup
# ------------------------------------------------------------------

async def create_browser():
    p = await async_playwright().start()

    browser = await p.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
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

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });

        Object.defineProperty(navigator, 'plugins', {
            get: () => [1,2,3,4,5]
        });

        window.chrome = {
            runtime: {}
        };
    """)

    return p, browser, context


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

async def scrape_document_links(context, company_url):
    page = await context.new_page()

    try:
        await page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        })

        await page.goto(
            company_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )

        await random_delay(1, 2)

        await page.mouse.wheel(
            0,
            random.randint(300, 800)
        )

        await random_delay(0.5, 1.5)

        target_frame = None

        for frame in page.frames:
            try:
                el = await frame.query_selector(
                    "#finacialStatementAndReports"
                )

                if el:
                    target_frame = frame
                    break

            except Exception:
                pass

        if target_frame is None:
            raise Exception(
                "Financial statements tab not found"
            )

        tab = await target_frame.query_selector(
            "#finacialStatementAndReports"
        )

        await tab.hover()
        await random_delay(0.5, 1.5)

        await tab.click()

        await target_frame.wait_for_selector(
            ".inner_tab_sub",
            timeout=15000,
        )

        await random_delay()

        links = await target_frame.eval_on_selector_all(
            ".inner_tab_sub td a.btn-pdf",
            "els => els.map(el => el.getAttribute('href'))"
        )

        return [
            BASE_URL + href
            for href in links
            if href
        ]

    finally:
        await page.close()


# ------------------------------------------------------------------
# Download
# ------------------------------------------------------------------

async def download_pdf(context, pdf_url, output_path):
    page = await context.new_page()
    try:
        # Use CDP to fetch the PDF directly with the browser's cookies/session
        client = await context.new_cdp_session(page)
        
        # Navigate first to establish any necessary cookies
        await page.goto(pdf_url, wait_until="domcontentloaded", timeout=60000)
        
        # Then fetch the raw bytes via JS using the page's authenticated session
        pdf_bytes_b64 = await page.evaluate("""async (url) => {
            const response = await fetch(url);
            const buffer = await response.arrayBuffer();
            const bytes = new Uint8Array(buffer);
            let binary = '';
            for (let i = 0; i < bytes.byteLength; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            return btoa(binary);
        }""", pdf_url)
        
        import base64
        body = base64.b64decode(pdf_bytes_b64)
        
        if not body.startswith(b"%PDF"):
            raise Exception(f"Not a valid PDF, got: {body[:200]}")
        
        with open(output_path, "wb") as f:
            f.write(body)
        
        print(f"Downloaded: {output_path.name}")
        return True

    except Exception as e:
        print(f"Download failed: {pdf_url}\n{e}")
        return False
    finally:
        await page.close()

# ------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------

async def download_top20_company_reports(
    csv_path,
    output_dir="saudi_exchange_pdfs",
    limit=20,
):
    root = Path(output_dir)
    root.mkdir(exist_ok=True)

    companies = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            companies.append(row)

    companies = companies[:limit]

    metadata_rows = []

    playwright, browser, context = await create_browser()

    try:

        for i, company in enumerate(companies, start=1):

            code = company["code"]
            name = company["name"]
            url = company["url"]

            print(
                f"\n[{i}/{len(companies)}] "
                f"{code} - {name}"
            )

            company_folder = (
                root /
                f"{code}_{name}"
                    .replace("/", "_")
                    .replace("\\", "_")
                    .replace(" ", "_")
            )

            company_folder.mkdir(
                parents=True,
                exist_ok=True
            )

            try:

                pdf_links = await scrape_document_links(
                    context,
                    url
                )

                print(
                    f"Found {len(pdf_links)} PDFs"
                )

                for idx, pdf_url in enumerate(
                    pdf_links,
                    start=1
                ):

                    filename = (
                        pdf_url.split("/")[-1]
                        .split("?")[0]
                    )

                    if not filename.lower().endswith(
                        ".pdf"
                    ):
                        filename = (
                            f"report_{idx}.pdf"
                        )

                    local_path = (
                        company_folder /
                        filename
                    )

                    success = await download_pdf(
                        context,
                        pdf_url,
                        local_path,
                    )

                    metadata_rows.append({
                        "company_code": code,
                        "company_name": name,
                        "pdf_url": pdf_url,
                        "local_path": str(local_path),
                        "downloaded": success,
                    })

            except Exception as e:

                print(
                    f"Failed {code}: {e}"
                )

            # Anti-bot delay
            await random_delay(1, 3)

    finally:

        await browser.close()
        await playwright.stop()

    # ----------------------------------------------------------
    # Save metadata CSV
    # ----------------------------------------------------------

    metadata_file = (
        root /
        "download_metadata.csv"
    )

    with open(
        metadata_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=[
                "company_code",
                "company_name",
                "pdf_url",
                "local_path",
                "downloaded",
            ],
        )

        writer.writeheader()
        writer.writerows(metadata_rows)

    print(
        f"\nMetadata saved to: "
        f"{metadata_file}"
    )


if __name__ == "__main__":

    asyncio.run(
        download_top20_company_reports(
            "saudi_exchange_company_profiles.csv",
            limit=20,
        )
    )
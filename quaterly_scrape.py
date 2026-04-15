# quarterly_scrape_to_df.py
# One-file pipeline:
# - Launch URL
# - Click the "Quarterly" tab (iframe-safe)
# - Capture network responses via Chrome DevTools Protocol
# - If that fails, scrape rendered DOM (main document + iframes)
# - Mine the best quarterly-looking table
# - Return a pandas DataFrame (wide): Metric + date columns
#
# Deps:
#   pip install selenium webdriver-manager beautifulsoup4 lxml pandas
#
# Notes:
# - Set HEADLESS=False while debugging so you can see what's happening.
# - If a cookie/consent banner appears, click Accept once manually (or add a click).
# - Quarterly detector expects month columns among {3,6,9,12} with >= 4 columns.

import json, re, time, base64
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.set_option("display.max_colwidth", None)


# -------------------- CONFIG --------------------
HEADLESS = True                 # set True once it works
CAPTURE_INITIAL = 8              # seconds before clicking
CAPTURE_AFTER_CLICK = 10         # seconds after clicking Quarterly
SCROLL_STEPS = 10                # extra scrolls to trigger lazy loads
SCROLL_PAUSE = 0.4
NOISY_URL_BITS = [
    "collect?", "recaptcha", "analytics", "ChartGenerator", "TickerServlet",
    "Theme", "dojo", "font", ".js?", "icon", "manifest", "bootstrap"
]
# ------------------------------------------------


# -------------------- DATE/NUM UTIL --------------------
RE_Y   = re.compile(r"^(?:19|20)\d{2}$")
RE_ISO = re.compile(r"^(?:19|20)\d{2}-\d{2}-\d{2}$")
RE_S   = re.compile(r"^\d{1,2}/\d{1,2}/(?:19|20)\d{2}$")

def dateish(s):
    s = (str(s) or "").strip()
    return bool(RE_Y.match(s) or RE_ISO.match(s) or RE_S.match(s))

def norm_date(s):
    s = (str(s) or "").replace("\u00A0"," ").strip()
    for dayfirst in (False, True):
        dt = pd.to_datetime(s, dayfirst=dayfirst, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")
    return s

def to_number(x):
    if x is None:
        return None
    s = str(x).replace("\u00A0"," ").strip()
    if s in {"", "-", "—", "–"}:
        return None
    s = re.sub(r"^\((.*)\)$", r"-\1", s)  # (1,234) -> -1234
    s = s.replace(",", "")
    try:
        return float(s)
    except:
        return None
# --------------------------------------------------------


# -------------------- JSON SHAPE HELPERS ----------------
LABEL_KEYS = [
    "metric","name","label","account","item","description","heading",
    "field","title","lineItem","accountName","caption","displayName",
    "line_name","LineItem","Line_Name"
]

def walk(obj, path=()):
    yield path, obj
    if isinstance(obj, dict):
        for k,v in obj.items():
            yield from walk(v, path+(k,))
    elif isinstance(obj, list):
        for i,v in enumerate(obj):
            yield from walk(v, path+(i,))

def shape_json(node):
    # list of dicts
    if isinstance(node, list) and node and isinstance(node[0], dict):
        keys = set().union(*(r.keys() for r in node))
        date_cols = [k for k in keys if isinstance(k,str) and dateish(k)]
        if len(date_cols) >= 2:
            label = next((k for k in LABEL_KEYS if k in keys), None)
            if label:
                table = []
                for r in node:
                    rec = {"Metric": str(r.get(label,"")).strip()}
                    for d in date_cols:
                        rec[d] = r.get(d)
                    table.append(rec)
                return table, date_cols

    # columns/rows style
    if isinstance(node, dict):
        cols = node.get("columns") or node.get("headers") or node.get("dates") or node.get("Dates")
        rows = node.get("rows") or node.get("data") or node.get("items")
        if isinstance(cols, list) and isinstance(rows, list):
            date_cols = [c for c in cols if isinstance(c,str) and dateish(c)]
            if len(date_cols) >= 2:
                label = next((k for k in LABEL_KEYS if any(isinstance(r,dict) and k in r for r in rows)), None) or "name"
                table = []
                for r in rows:
                    if isinstance(r, dict):
                        vals = r.get("values") or r.get("data") or []
                        rec = {"Metric": str(r.get(label,"")).strip()}
                        if isinstance(vals, list) and vals:
                            for i, d in enumerate(date_cols):
                                rec[d] = vals[i] if i < len(vals) else None
                        else:
                            for d in date_cols:
                                rec[d] = r.get(d)
                        table.append(rec)
                return table, date_cols
    return None
# --------------------------------------------------------


# -------------------- HTML TABLE PARSER -----------------
def parse_html_table(tbl):
    # build header
    header = []
    thead = tbl.find("thead")
    if thead:
        header = [c.get_text(strip=True) for c in thead.find_all(["th","td"])]
    if not header:
        first_tr = tbl.find("tr")
        if first_tr:
            header = [c.get_text(strip=True) for c in first_tr.find_all(["th","td"])]

    date_cols = [h for h in header if dateish(h)]
    if len(date_cols) < 2:
        return None

    # metric col = first non-date header
    metric_idx = 0
    while metric_idx < len(header) and dateish(header[metric_idx]):
        metric_idx += 1

    rows = []
    for tr in tbl.find_all("tr"):
        cells = [c.get_text(strip=True).replace("\u00A0"," ") for c in tr.find_all(["th","td"])]
        if not cells:
            continue
        if cells == header:
            continue
        # skip section-only rows
        if sum(1 for c in cells if c not in ("","-","—","–")) == 1 and (cells[0] not in ("","-","—","–")):
            continue

        metric = cells[metric_idx] if metric_idx < len(cells) else (cells[0] if cells else "")
        rec = {"Metric": metric}
        for i, h in enumerate(header):
            if dateish(h):
                rec[h] = cells[i] if i < len(cells) else None
        rows.append(rec)

    return rows, date_cols
# --------------------------------------------------------


# -------------------- QUARTERLY DETECTOR ----------------
def is_quarterly(date_cols):
    try:
        months = [pd.to_datetime(d, dayfirst=True, errors="coerce").month for d in date_cols]
        months = [m for m in months if pd.notna(m)]
        return bool(months) and set(months).issubset({3,6,9,12}) and len(months) >= 4
    except:
        return False
# --------------------------------------------------------


# -------------------- SELENIUM + CDP --------------------
def start_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.set_capability("goog:loggingPrefs", {"performance":"ALL"})
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.execute_cdp_cmd("Network.enable", {"maxResourceBufferSize": 50_000_000, "maxTotalBufferSize": 100_000_000})
    drv.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    return drv

def click_quarterly(driver):
    """Find and click 'Quarterly' even when nested in iframes."""
    wait = WebDriverWait(driver, 12)
    driver.switch_to.default_content()

    def dfs(depth=0, max_depth=12):
        if depth > max_depth:
            return False
        try:
            # Heuristic "widget present" check
            if driver.find_elements(By.XPATH, "//*[normalize-space()='Annually' or normalize-space()='Quarterly' or contains(.,'FINANCIAL INFORMATION')]"):
                return True
        except:
            pass
        for f in driver.find_elements(By.TAG_NAME, "iframe"):
            try:
                driver.switch_to.frame(f)
                if dfs(depth+1, max_depth):
                    return True
                driver.switch_to.parent_frame()
            except:
                driver.switch_to.parent_frame()
        return False

    if not dfs():
        return False

    candidates = ["Quarterly","Quarter","ربع سنوي"]
    for label in candidates:
        for xp in [
            f"//*[@role='tab' and normalize-space()='{label}']",
            f"//button[normalize-space()='{label}']",
            f"//a[normalize-space()='{label}']",
            f"//*[contains(@class,'tab') and normalize-space()='{label}']",
            f"//*[normalize-space()='{label}']",
        ]:
            els = driver.find_elements(By.XPATH, xp)
            if not els:
                continue
            el = els[0]
            try:
                wait.until(EC.element_to_be_clickable(el)).click()
                return True
            except:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    driver.execute_script("arguments[0].click();", el)
                    return True
                except:
                    pass
    return False

def capture_bodies(driver, seconds, seen):
    """Return list of {url, mime, text} for responses observed during 'seconds'."""
    t0 = time.time()
    pending = {}  # reqId -> (url, mime)
    out = []
    while time.time() - t0 < seconds:
        for e in driver.get_log("performance"):
            try:
                msg = json.loads(e["message"])["message"]
            except Exception:
                continue
            method = msg.get("method",""); params = msg.get("params",{})
            if method == "Network.responseReceived":
                resp = params.get("response", {})
                req_id = params.get("requestId")
                url = resp.get("url") or ""
                mime = resp.get("mimeType") or ""
                if any(x in url for x in NOISY_URL_BITS):
                    continue
                pending[req_id] = (url, mime)
            elif method == "Network.loadingFinished":
                req_id = params.get("requestId")
                if req_id in seen or req_id not in pending:
                    continue
                url, mime = pending.pop(req_id)
                try:
                    body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                    text = body.get("body") or ""
                    if body.get("base64Encoded"):
                        try:
                            text = base64.b64decode(text).decode("utf-8","ignore")
                        except Exception:
                            continue
                    out.append({"url": url, "mime": mime, "text": text})
                    seen.add(req_id)
                except Exception:
                    pass
        time.sleep(0.15)
    return out
# --------------------------------------------------------


# -------------------- MINING LOGIC ----------------------
def mine_quarterly_table_from_bodies(bodies):
    """Find best quarterly-looking table across captured JSON/HTML bodies."""
    candidates = []

    # JSON first (usually cleaner)
    for b in bodies:
        if "json" in (b["mime"] or "").lower():
            try:
                payload = json.loads(b["text"])
            except Exception:
                continue
            obj = payload.get("json", payload)
            for _, node in walk(obj):
                shaped = shape_json(node)
                if shaped:
                    table, date_cols = shaped
                    if is_quarterly([norm_date(d) for d in date_cols]):
                        score = len(table) + 3*len(date_cols)
                        candidates.append(("json", score, table, date_cols, b["url"]))

    # HTML fallback
    for b in bodies:
        if "html" in (b["mime"] or "").lower():
            try:
                soup = BeautifulSoup(b["text"], "lxml")
            except Exception:
                continue
            for t in soup.find_all("table"):
                parsed = parse_html_table(t)
                if not parsed:
                    continue
                rows, date_cols = parsed
                if is_quarterly([norm_date(d) for d in date_cols]):
                    score = len(rows) + 3*len(date_cols)
                    candidates.append(("html", score, rows, date_cols, b["url"]))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    _, _, table, date_cols, src_url = candidates[0]
    return table, date_cols, src_url
# --------------------------------------------------------


# -------------------- DOM FALLBACK ----------------------
def wait_for_quarterly_visible(driver, timeout=14):
    """After clicking, wait until something table-ish or quarterly-ish shows up."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.any_of(
                EC.presence_of_all_elements_located((By.XPATH, "//table")),
                EC.presence_of_all_elements_located((By.XPATH, "//*[contains(., 'Quarter') or contains(., 'ربع سنوي')]"))
            )
        )
    except Exception:
        pass

def mine_quarterly_from_dom(driver):
    """Scan current document and iframes for tables; return first quarterly-looking table."""
    def scan_current_doc():
        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")
        print('html', )
        for t in soup.find_all("table"):
            parsed = parse_html_table(t)            
            if not parsed:
                continue
            rows, date_cols = parsed
            if is_quarterly([norm_date(d) for d in date_cols]):
                return rows, date_cols, "(dom)"
        return None

    # try main doc
    got = scan_current_doc()
    if got:
        return got

    # try iframes
    frames = driver.find_elements(By.TAG_NAME, "iframe")
    for f in frames:
        try:
            driver.switch_to.frame(f)
            got = scan_current_doc()
            driver.switch_to.parent_frame()
            if got:
                return got
        except Exception:
            try:
                driver.switch_to.parent_frame()
            except Exception:
                pass
    return None
# --------------------------------------------------------


# -------------------- PUBLIC API -----------------------
def scrape_quarterly_to_dataframe(url: str) -> pd.DataFrame:
    drv = start_driver()
    try:
        print("[step] opening URL…")
        drv.get(url)
        time.sleep(1.2)

        seen = set()
        print("[step] capturing idle network…")
        bodies = capture_bodies(drv, CAPTURE_INITIAL, seen)

        print("[step] clicking Quarterly…")
        clicked = click_quarterly(drv)
        if not clicked:
            print("[warn] couldn't click Quarterly via direct locators; trying anyway.")

        wait_for_quarterly_visible(drv, timeout=16)

        print("[step] capturing after click…")
        bodies += capture_bodies(drv, CAPTURE_AFTER_CLICK, seen)
        print("[step] lazy-load scroll pass…")
        drv.switch_to.default_content()
        for _ in range(SCROLL_STEPS):
            drv.execute_script("window.scrollBy(0, 1000);")
            time.sleep(SCROLL_PAUSE)
            bodies += capture_bodies(drv, 1.2, seen)

        n_json = sum(1 for b in bodies if "json" in (b["mime"] or "").lower())
        n_html = sum(1 for b in bodies if "html" in (b["mime"] or "").lower())
        print(f"[debug] captured bodies: json={n_json}, html={n_html}, total={len(bodies)}")

        mined = mine_quarterly_table_from_bodies(bodies)

        if not mined:
            print("[debug] network mining failed; trying DOM fallback…")
            dom_mined = mine_quarterly_from_dom(drv)
            if dom_mined:
                table, date_cols, src = dom_mined
                mined = (table, date_cols, src)

        if not mined:
            try:
                drv.save_screenshot("quarterly_debug.png")
                print("[debug] saved screenshot: quarterly_debug.png")
            except Exception:
                pass
            raise RuntimeError("No quarterly-looking tables found. Try HEADLESS=False, increase CAPTURE_AFTER_CLICK,"
                               " accept any cookie banner, and make sure the Quarterly tab actually loads.")

        table, date_cols, src = mined
        print(f"[ok] quarterly source: {src} | date cols: {date_cols[:6]}{'...' if len(date_cols)>6 else ''}")

        # Normalize to wide DataFrame: Metric + date columns
        iso_dates = [norm_date(d) for d in date_cols]
        wide_rows = []
        for r in table:
            metric = str(r.get("Metric","")).strip()
            if not metric:
                continue
            row = {"Metric": metric}
            for d_raw, d_iso in zip(date_cols, iso_dates):
                row[d_iso] = to_number(r.get(d_raw))
            if any(v is not None for k,v in row.items() if k != "Metric"):
                wide_rows.append(row)

        if not wide_rows:
            raise RuntimeError("Parsed a table but rows were empty after cleaning. The page might not render a table.")

        df = pd.DataFrame(wide_rows)
        # sort dates ascending
        date_cols_iso = sorted([c for c in df.columns if c != "Metric"])
        df = df[["Metric"] + date_cols_iso]
        return df

    finally:
        try:
            drv.quit()
        except:
            pass
# --------------------------------------------------------

def extract_company_code(url: str) -> str:
    m = re.search(r"/companySymbol/(\d+)", url)
    return m.group(1) if m else ""

def split_quarters(df: pd.DataFrame, company_code: str) -> dict[str, pd.DataFrame]:
    idx = df.set_index("Metric")
    quarter_dfs: dict[str, pd.DataFrame] = {}
    for date_col in [c for c in df.columns if c != "Metric"]:
        # Build dict: metric -> value for this quarter
        row_dict = idx[date_col].to_dict()
        row_dict["company_code"] = company_code
        qdf = pd.DataFrame([row_dict])
        # Reorder: company_code first, then metrics
        cols = ["company_code"] + [c for c in qdf.columns if c != "company_code"]
        quarter_dfs[date_col] = qdf[cols]
    return quarter_dfs


# -------------------- CLI DEMO -------------------------
if __name__ == "__main__":
    URL = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/!ut/p/z1/jZBLb4JQEIV_iwuXdUYUuHZHNcUHCEhahY250imQgtdeLpL01xftphr7mMxmZr6TnDkQwwbiPT_mKVe52POinaPY2OqWgdqUoccmkzEGjws2naOnoWHC-hLA0NdbwHcHDq7QRgPi_-jxh7Lwb318hbi2gcHSCjzN1BFD7Rq4YfEM_OJhDnFaiN1XHtZ-N2ApxJJeSZLs1bJdZ0odqvsudrFpml4qRFpQLxFlF29JMlEp2FyS5yTM7cy3h_0Z0xa2FY7RCA1z9TCyEBnCWlIlapkQrFJSLs_3S1HWicvlG6kJKZ4XFQQJTzJy6EiFz1OC8PRdXjVcJZmTVyqkghJFLxAtvdOpaichfS55SYokRKflFqK-qRuMDYdMHw3PESq-K-g5p-Yb2z8dJL3XVClHJLwgiLiEQ_m0wdwv10zdRePdB7UdWZ3OJ3E73tY!/dz/d5/L0lHSklKQ1NDbENsQ1FvS1VRb2dwUkNpQ2xFaVEvWU9ZRUFBSU1FQUFBRUVNQ01LR0lNQU9FT0JFQkVKRk5GTkpGRERMRExISU1FRFBQQXZBblBDS0EvNEpDaWpLMWJHTGppRUVwTWhTVFVVMXUybHNacVdhM2JTMjFWRktxaXBBISEvWjdfNUE2MDJIODBPMFZDNDA2ME80R01MODFHNTUvWjZfNUE2MDJIODBPR0YyRTBRRjlCUURFRzEwSzQvdmlldy9ub3JtYWwvbGFuZy9lbi9odHRwOiUwJTB0YWRhd3VsJTAvY29tcGFueVN5bWJvbC8yMDMw/?locale=en"
    df = scrape_quarterly_to_dataframe(URL)
    # print(df)
    # df.to_csv("quarterly_financials.csv", index=False)
    
    # === use it ===
    company_code = extract_company_code(URL)  # "2030"
    quarter_dfs = split_quarters(df, company_code)

    print("\n[2024-06-30]\n", quarter_dfs["2024-06-30"])

    for date_col, qdf in quarter_dfs.items():
        safe = date_col.replace("-", "_")
        fname = f"{company_code}_{safe}.csv"
        qdf.to_csv(fname, index=False)
        print("saved:", fname)
"""
generic_scraper.py — General-purpose Selenium table scraper.

Accepts any URL, extracts all HTML/DataTables table data, saves to CSV.
Runs inside its own thread; safe to run multiple instances in parallel.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import WebDriverException, TimeoutException
from db import generic_collection
import os
import time
import threading
from datetime import datetime

MAX_PAGES = 500          # Safety cap — stop after this many DataTable pages
MAX_CRAWL_LINKS = 30     # Max internal links to visit during deep crawl
MAX_LOG_LINES = 150
MAX_CONCURRENT = 5       # Max simultaneous Firefox sessions

# ── Session registry ──────────────────────────────────────────────────────────
# { session_id: { url, status, records, logs, is_running, csv_path, error } }
generic_sessions: dict = {}
_sessions_lock = threading.Lock()


def _make_session(session_id: str, url: str) -> dict:
    return {
        "session_id": session_id,
        "url": url,
        "status": "INITIALIZING",
        "records": 0,
        "logs": [],
        "is_running": True,
        "error": "",
        "started_at": datetime.now().strftime("%H:%M:%S"),
    }


def _log(session_id: str, message: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(f"[{session_id}] {message}")
    with _sessions_lock:
        sess = generic_sessions.get(session_id)
        if sess is None:
            return
        sess["logs"].append(line)
        if len(sess["logs"]) > MAX_LOG_LINES:
            sess["logs"] = sess["logs"][-MAX_LOG_LINES:]


def _set_status(session_id: str, status: str):
    with _sessions_lock:
        if session_id in generic_sessions:
            generic_sessions[session_id]["status"] = status


def _make_driver():
    """Create a headless Firefox instance using the same env-aware logic."""
    options = FirefoxOptions()
    options.page_load_strategy = 'eager'  # Do not wait for heavy ads and tracking scripts
    display_available = bool(os.environ.get("DISPLAY", "").strip())
    force_headless = os.environ.get("HEADLESS", "").strip() in {"1", "true", "True", "yes", "YES"}
    force_no_headless = os.environ.get("HEADLESS", "").strip() in {"0", "false", "False", "no", "NO"}
    if (not display_available or force_headless) and not force_no_headless:
        options.add_argument("-headless")
    options.set_preference("browser.cache.disk.enable", False)
    options.set_preference("browser.cache.memory.enable", False)
    options.set_preference("network.http.use-cache", False)
    driver = webdriver.Firefox(options=options)
    driver.set_page_load_timeout(30)   # Timeouts handled gracefully now
    driver.set_script_timeout(30)
    return driver


def _extract_generic_content(driver) -> list[list[str]]:
    """
    Extracts headings, paragraphs, meaningful links, and tables from any webpage.
    Returns formatting: [Content Type, Text/Data, Extra Info (e.g. Link)]
    """
    all_data = []

    # 1. Extract Headings (Headline News)
    for tag in ['h1', 'h2', 'h3']:
        for e in driver.find_elements(By.TAG_NAME, tag):
            text = e.text.strip()
            if text:
                all_data.append([tag.upper(), text, ""])

    # 2. Extract Paragraphs (Article Body)
    for e in driver.find_elements(By.TAG_NAME, 'p'):
        text = e.text.strip()
        if text and len(text) > 15: # Skip tiny UI fragments
            all_data.append(["Paragraph", text, ""])

    # 3. Extract Meaningful Links (Navigation / Related Articles)
    for e in driver.find_elements(By.TAG_NAME, 'a'):
        text = e.text.strip()
        href = e.get_attribute('href')
        if text and href and href.startswith('http') and len(text) > 10:
            all_data.append(["Link", text, href])

    # 4. Extract Tables (Data)
    tables = driver.find_elements(By.TAG_NAME, "table")
    for t_idx, table in enumerate(tables):
        for r_idx, row in enumerate(table.find_elements(By.TAG_NAME, "tr")):
            cells = row.find_elements(By.XPATH, ".//th | .//td")
            text_cells = " | ".join([c.text.strip() for c in cells if c.text.strip()])
            if text_cells:
                all_data.append([f"Table {t_idx+1}", text_cells, f"Row {r_idx+1}"])

    return all_data


def _has_datatable(driver) -> bool:
    try:
        return driver.execute_script(
            "return !!(window.jQuery && window.jQuery.fn.dataTable);"
        )
    except Exception:
        return False


def _datatable_next_enabled(driver) -> bool:
    """Return True if any DataTables 'Next' button is enabled on the page."""
    try:
        next_btns = driver.find_elements(By.CSS_SELECTOR, "[id$='_next']")
        for btn in next_btns:
            if "disabled" not in (btn.get_attribute("class") or ""):
                return True
        return False
    except Exception:
        return False


def _click_datatable_next(driver):
    """Click the first enabled DataTables Next button."""
    next_btns = driver.find_elements(By.CSS_SELECTOR, "[id$='_next']")
    for btn in next_btns:
        if "disabled" not in (btn.get_attribute("class") or ""):
            driver.execute_script("arguments[0].click();", btn)
            return True
    return False


def run_generic_scraper(session_id: str, start_url: str, mode: str, stop_event: threading.Event):
    """
    Core scraping function. Runs in its own thread.
    Writes results to generic_<session_id>.csv
    """
    driver = None
    records = 0

    queue = [start_url]
    visited = set()
    is_datatable = False

    try:
        driver = _make_driver()
        header_written = False

        from urllib.parse import urlparse
        base_domain = urlparse(start_url).netloc

        while queue and not stop_event.is_set():
                url = queue.pop(0)
                if url in visited: continue
                visited.add(url)

                _log(session_id, f"Opening URL ({len(visited)}/{len(visited)+len(queue)}): {url[:60]}")
                _set_status(session_id, "RUNNING")
                
                try:
                    driver.get(url)
                except TimeoutException:
                    _log(session_id, "Page load timed out (heavy ads/scripts). Forcing extract on loaded elements...")

                # Wait up to 10 seconds for the body tag to be somewhat populated
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except TimeoutException:
                    _log(session_id, f"No <body> found on {url}. Skipping.")
                    continue

                # On first page, check if Datatable
                if len(visited) == 1:
                    is_datatable = _has_datatable(driver)
                    _log(session_id, f"DataTables detected: {is_datatable}")

                # If deep mode, collect internal links on the first page
                if len(visited) == 1 and not is_datatable and mode == "deep":
                    _log(session_id, "Deep Crawl mode activated. Collecting links...")
                    links = driver.find_elements(By.TAG_NAME, 'a')
                    collected = 0
                    for a in links:
                        href = a.get_attribute("href")
                        if href and href.startswith("http") and base_domain in href and href not in queue and href != start_url:
                            queue.append(href)
                            collected += 1
                            if collected >= MAX_CRAWL_LINKS:
                                break
                    _log(session_id, f"Queued {collected} internal links for deep scraping.")
                elif len(visited) == 1 and mode == "single":
                    _log(session_id, "Single Page mode activated. No deep crawling.")

                page_num = 1
                while not stop_event.is_set():
                    time.sleep(1)   # let DOM settle
                    rows = _extract_generic_content(driver)

                    if len(visited) > 1 and page_num == 1:
                        sep_doc = {
                            "session_id": session_id,
                            "content_type": "SOURCE URL",
                            "extracted_data": url,
                            "extra_info": "---"
                        }
                        generic_collection.insert_one(sep_doc)
                        
                    docs = []
                    for row in rows:
                        doc = {
                            "session_id": session_id,
                            "content_type": row[0],
                            "extracted_data": row[1],
                            "extra_info": row[2]
                        }
                        docs.append(doc)
                        records += 1

                    if docs:
                        generic_collection.insert_many(docs)

                    with _sessions_lock:
                        generic_sessions[session_id]["records"] = records

                    _log(session_id, f"Page {page_num}: saved {len(rows)} elements (total {records}).")

                    # Pagination
                    if is_datatable and page_num < MAX_PAGES:
                        if _datatable_next_enabled(driver):
                            _click_datatable_next(driver)
                            page_num += 1
                        else:
                            _log(session_id, "Reached last DataTable page.")
                            break
                    else:
                        break   # plain HTML / normal page — break inner pagination loop

                # Pause to prevent IP ban while crawling
        # Loop ends here
        _log(session_id, f"Finished. {records} rows saved to database.")
        _set_status(session_id, "FINISHED")

    except WebDriverException as e:
        _log(session_id, f"Browser error: {e}")
        _set_status(session_id, "ERROR")
        with _sessions_lock:
            generic_sessions[session_id]["error"] = str(e)[:300]
    except Exception as e:
        _log(session_id, f"Unexpected error: {e}")
        _set_status(session_id, "ERROR")
        with _sessions_lock:
            generic_sessions[session_id]["error"] = str(e)[:300]
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        with _sessions_lock:
            if session_id in generic_sessions:
                generic_sessions[session_id]["is_running"] = False
                if generic_sessions[session_id]["status"] == "RUNNING":
                    generic_sessions[session_id]["status"] = "STOPPED"


def start_generic_session(url: str, mode: str = "single") -> tuple[str, str]:
    """
    Creates a new session, starts the scraper thread.
    Returns (session_id, error_message). error is "" on success.
    """
    # Count active sessions
    with _sessions_lock:
        active = sum(
            1 for s in generic_sessions.values() if s["is_running"]
        )
        if active >= MAX_CONCURRENT:
            return "", f"Max {MAX_CONCURRENT} concurrent scrapers allowed. Stop one first."

    # Generate a short ID from timestamp
    import uuid
    session_id = uuid.uuid4().hex[:8]
    stop_event = threading.Event()

    session = _make_session(session_id, url)
    session["stop_event"] = stop_event   # store for stop API

    with _sessions_lock:
        generic_sessions[session_id] = session

    t = threading.Thread(
        target=run_generic_scraper,
        args=(session_id, url, mode, stop_event),
        daemon=True,
    )
    t.start()
    return session_id, ""


def stop_generic_session(session_id: str) -> str:
    """Sends stop signal. Returns error string or '' on success."""
    with _sessions_lock:
        sess = generic_sessions.get(session_id)
        if not sess:
            return "Session not found."
        if not sess["is_running"]:
            return "Session is not running."
        stop_event: threading.Event = sess.get("stop_event")

    if stop_event:
        stop_event.set()
        _set_status(session_id, "STOPPING")
    return ""


def remove_generic_session(session_id: str) -> str:
    """Removes session from memory and deletes its CSV."""
    with _sessions_lock:
        sess = generic_sessions.get(session_id)
        if not sess:
            return "Session not found."
        if sess["is_running"]:
            return "Cannot remove a running session. Stop it first."
        
        # Remove from MongoDB
        try:
            generic_collection.delete_many({"session_id": session_id})
        except Exception:
            pass
        
        del generic_sessions[session_id]
    return ""


def flush_all_generics() -> str:
    """Stops all sessions, wipes memory, and deletes all generic_*.csv files."""
    import glob
    with _sessions_lock:
        for sess in generic_sessions.values():
            if sess["is_running"]:
                stop_event: threading.Event = sess.get("stop_event")
                if stop_event:
                    stop_event.set()
        
        generic_sessions.clear()

    # Give threads a tiny bit of time to yield
    time.sleep(0.5)

    # Remove all generic data from MongoDB
    try:
        generic_collection.delete_many({})
    except Exception:
        pass
        
    return ""


def get_sessions_snapshot() -> list[dict]:
    """Returns a JSON-safe snapshot of all sessions (no threading objects)."""
    with _sessions_lock:
        result = []
        for sess in generic_sessions.values():
            result.append({
                k: v for k, v in sess.items()
                if k != "stop_event"   # not JSON-serialisable
            })
    return result

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
import csv
import os
import time
import threading
from datetime import datetime

MAX_PAGES = 500          # Safety cap — stop after this many DataTable pages
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
        "csv_path": f"generic_{session_id}.csv",
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
    display_available = bool(os.environ.get("DISPLAY", "").strip())
    force_headless = os.environ.get("HEADLESS", "").strip() in {"1", "true", "True", "yes", "YES"}
    force_no_headless = os.environ.get("HEADLESS", "").strip() in {"0", "false", "False", "no", "NO"}
    if (not display_available or force_headless) and not force_no_headless:
        options.add_argument("-headless")
    options.set_preference("browser.cache.disk.enable", False)
    options.set_preference("browser.cache.memory.enable", False)
    options.set_preference("network.http.use-cache", False)
    driver = webdriver.Firefox(options=options)
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(60)
    return driver


def _extract_tables(driver) -> list[list[str]]:
    """
    Extract all rows from all <table> elements on the current page.
    Returns a flat list of rows (each row is a list of cell strings).
    Includes a header row per table prefixed with the table index.
    """
    tables = driver.find_elements(By.TAG_NAME, "table")
    all_rows = []
    for t_idx, table in enumerate(tables):
        rows = table.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            cells = row.find_elements(By.XPATH, ".//th | .//td")
            text_cells = [c.text.strip() for c in cells]
            if any(text_cells):          # skip fully empty rows
                all_rows.append([f"[T{t_idx+1}]"] + text_cells)
    return all_rows


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


def run_generic_scraper(session_id: str, url: str, stop_event: threading.Event):
    """
    Core scraping function. Runs in its own thread.
    Writes results to generic_<session_id>.csv
    """
    driver = None
    records = 0
    csv_path = generic_sessions[session_id]["csv_path"]

    try:
        _log(session_id, f"Opening URL: {url}")
        _set_status(session_id, "LOADING")
        driver = _make_driver()
        driver.get(url)

        # Wait for at least one table to appear (max 20 s)
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except TimeoutException:
            _log(session_id, "No <table> found on page within 20 s. Aborting.")
            _set_status(session_id, "ERROR")
            generic_sessions[session_id]["error"] = "No table found on page."
            return

        _log(session_id, "Table(s) detected. Starting extraction.")
        _set_status(session_id, "RUNNING")

        is_datatable = _has_datatable(driver)
        _log(session_id, f"DataTables detected: {is_datatable}")

        page_num = 1
        header_written = False

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            while not stop_event.is_set():
                time.sleep(0.8)   # let DOM settle after page change
                rows = _extract_tables(driver)

                if not header_written:
                    writer.writerow(["Table", "Data..."])  # generic header
                    header_written = True

                for row in rows:
                    writer.writerow(row)
                    records += 1

                with _sessions_lock:
                    generic_sessions[session_id]["records"] = records

                _log(session_id, f"Page {page_num}: saved {len(rows)} rows (total {records}).")

                # Pagination
                if is_datatable and page_num < MAX_PAGES:
                    if _datatable_next_enabled(driver):
                        _click_datatable_next(driver)
                        page_num += 1
                        time.sleep(1)
                    else:
                        _log(session_id, "Reached last DataTable page.")
                        break
                else:
                    break   # plain HTML table — single page

        _log(session_id, f"Finished. {records} rows saved to {csv_path}.")
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


def start_generic_session(url: str) -> tuple[str, str]:
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
        args=(session_id, url, stop_event),
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

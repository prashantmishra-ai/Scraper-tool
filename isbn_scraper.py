from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import StaleElementReferenceException, UnexpectedAlertPresentException, WebDriverException, TimeoutException
import time
import random
import os
import threading
from db import isbn_collection
import json
from datetime import datetime, timezone

scraper_state = {
    "is_running": False,
    "current_page": 1,
    "status": "STOPPED",
    "total_records": 0,
    "last_error": "",
    "logs": [],
    "consecutive_errors": 0,
}
stop_event = threading.Event()
MAX_LOG_LINES = 200
MAX_CONSECUTIVE_ERRORS = 6

# ── Heavy Duty Configuration ─────────────────────────────────────────

# We are now using MongoDB to store all records, avoiding file limits and locking issues entirely.
checkpoint_file = "scraper_state.json"

expected_columns = [
    "#", "Book Title", "ISBN", "Product Form", "Language",
    "Applicant Type", "Name of Publishing Agency/Publisher",
    "Imprint", "Name of Author/Editor", "Publication Date"
]

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def log_event(message):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(line)
    scraper_state["logs"].append(line)
    if len(scraper_state["logs"]) > MAX_LOG_LINES:
        scraper_state["logs"] = scraper_state["logs"][-MAX_LOG_LINES:]

def load_checkpoint():
    """
    Persistent store for resume.
    Returns a dict with at least: {"next_page": int, "total_records": int}
    """
    if not os.path.exists(checkpoint_file):
        return {"next_page": 1, "total_records": 0}
    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        next_page = int(data.get("next_page", 1))
        total_records = int(data.get("total_records", 0))
        return {"next_page": max(1, next_page), "total_records": max(0, total_records)}
    except Exception:
        # If checkpoint is corrupted, start from scratch rather than crash.
        return {"next_page": 1, "total_records": 0}

def save_checkpoint(*, next_page, total_records, last_error=""):
    data = {
        "next_page": int(next_page),
        "total_records": int(total_records),
        "last_error": str(last_error) if last_error else "",
        "updated_at": _utc_now_iso(),
    }
    tmp_path = f"{checkpoint_file}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp_path, checkpoint_file)

# Storage migrated to MongoDB.

page_num = 1
total_collected = 0

def run_scraper(start_page):
    """
    Runs the scraper starting from a specific page.
    Returns (last_successful_page, status_code)
    Status codes: 
        - "FINISHED": Reached the last page
        - "BLOCKED": Server blocked/rate limit alert
        - "ERROR": Any other unexpected exception
        - "STOPPED": User manually stopped
    """
    driver = None
    page_num = start_page
    total_collected_this_run = 0

    try:
        options = FirefoxOptions()
        # Auto-detect headless: containers have no DISPLAY so Firefox must be headless.
        # On a dev machine with a real display, only go headless if HEADLESS=1.
        # Set HEADLESS=0 to explicitly force non-headless (requires a real display).
        display_available = bool(os.environ.get("DISPLAY", "").strip())
        force_headless = os.environ.get("HEADLESS", "").strip() in {"1", "true", "True", "yes", "YES"}
        force_no_headless = os.environ.get("HEADLESS", "").strip() in {"0", "false", "False", "no", "NO"}
        if (not display_available or force_headless) and not force_no_headless:
            options.add_argument("-headless")
            log_event("Running Firefox in headless mode (no display detected or HEADLESS=1).")
        # Stability flags for containerised / CI environments
        options.set_preference("browser.cache.disk.enable", False)
        options.set_preference("browser.cache.memory.enable", False)
        options.set_preference("browser.cache.offline.enable", False)
        options.set_preference("network.http.use-cache", False)

        # Let Selenium Manager resolve geckodriver automatically (Selenium 4.6+).
        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(120)  # Wait max 2 minutes for page loads
        driver.set_script_timeout(120)     # Wait max 2 minutes for scripts
        driver.get("https://isbn.gov.in/Home/IsbnSearch")
        wait = WebDriverWait(driver, 15)

        # Step 1: Click Search Type dropdown
        search_type_dropdown = wait.until(
            EC.element_to_be_clickable((By.ID, "drpSearchType"))
        )
        search_type_dropdown.click()

        # Step 2: Select ISBN Number / Series option
        isbn_series_option = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//option[contains(text(),'Series')]")
            )
        )
        isbn_series_option.click()

        # Step 3: Wait for input box and enter value 1
        search_input = wait.until(
            EC.presence_of_element_located((By.ID, "txtsearch"))
        )
        search_input.clear()
        search_input.send_keys("978")

        # Step 4: Click Search button
        search_button = driver.find_element(By.ID, "btnSearchIsbnRptNew")
        search_button.click()

        log_event("Waiting for server to fetch records (5-10 seconds).")
        # Wait until the 'examplenew_info' element is present and populated with text
        wait.until(lambda d: d.find_element(By.ID, "examplenew_info").text.strip() != "")
        time.sleep(1) # Extra buffer for stability

        # --- DIRECT PAGE JUMP LOGIC START ---
        if page_num > 1:
            log_event(f"Fast-forwarding directly to page {page_num}...")
            try:
                jump_result = driver.execute_script(
                    """
                    var targetPage = arguments[0];
                    var selector = '#examplenew';
                    if (!window.jQuery || !window.jQuery(selector).length) {
                        return 'table_not_found';
                    }
                    if (window.jQuery.fn.dataTable && window.jQuery.fn.dataTable.isDataTable(selector)) {
                        window.jQuery(selector).DataTable().page(targetPage).draw('page');
                        return 'ok_modern';
                    }
                    var legacy = window.jQuery(selector).dataTable();
                    if (legacy && legacy.fnPageChange) {
                        legacy.fnPageChange(targetPage);
                        return 'ok_legacy';
                    }
                    return 'datatable_not_ready';
                    """,
                    page_num - 1
                )
                if not str(jump_result).startswith("ok"):
                    raise RuntimeError(f"page-jump failed: {jump_result}")
                
                # Wait for the "Showing X to Y" text to update to ensure the jump actually finished
                def jump_completed(d):
                    info_text = d.find_element(By.ID, "examplenew_info").text
                    # Check if the first row number displayed matches our target page math
                    # Use "{:,}" to add commas because DataTables displays "Showing 149,951 to..."
                    expected_start = "{:,}".format((page_num - 1) * 50 + 1)
                    return expected_start in info_text

                wait.until(jump_completed)
                time.sleep(1)  # Extra second for table rows to visually attach to DOM
                log_event(f"Jumped successfully to page {page_num}.")
            except Exception as e:
                log_event(f"Could not jump directly: {e}")
                return page_num, "ERROR"
        # --- DIRECT PAGE JUMP LOGIC END ---

        log_event(f"Starting scraper from page {page_num}.")

        # Step 5: Scrape all pages
        while True:
            if stop_event.is_set():
                log_event("Stop command received. Halting scraper from UI.")
                return page_num, "STOPPED"
                
            scraper_state["current_page"] = page_num

            # Retry loop for StaleElementReferenceException
            for attempt in range(3):
                try:
                    time.sleep(1)  # Give DataTables time to finish updating DOM
                    rows = driver.find_elements(By.XPATH, "//table[@id='examplenew']/tbody/tr")
                    page_data = []

                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        page_data.append([c.text.strip() for c in cols])

                    # Save directly to MongoDB instantly
                    docs = []
                    for item in page_data:
                        if item:
                            # Map array fields to Dictionary using expected_columns
                            doc = {
                                expected_columns[i]: (item[i] if i < len(item) else "")
                                for i in range(len(expected_columns))
                            }
                            docs.append(doc)
                    
                    if docs:
                        isbn_collection.insert_many(docs)
                        added = len(docs)
                        total_collected_this_run += added
                        scraper_state["total_records"] += added
                    
                    log_event(f"Page {page_num} scraped and saved.")
                    # Persist progress: resume from the *next* page.
                    save_checkpoint(
                        next_page=page_num + 1,
                        total_records=scraper_state["total_records"],
                    )
                    break  # Success
                except StaleElementReferenceException:
                    if attempt == 2: raise
                    time.sleep(2)
                    continue

            # Try clicking Next button
            try:
                next_btn = driver.find_element(By.ID, "examplenew_next")
                if "disabled" in next_btn.get_attribute("class"):
                    log_event("Reached the last page.")
                    return page_num, "FINISHED"

                # Use JS click to avoid 'element not interactable' errors
                driver.execute_script("arguments[0].click();", next_btn)
                page_num += 1
                
                # Randomized delay between clicks
                time.sleep(random.uniform(0.5, 2))
            except Exception as e:
                log_event(f"Could not click Next: {e}")
                return page_num, "ERROR"

    except UnexpectedAlertPresentException as e:
        log_event("Server blocked connection (rate limit/timeout).")
        log_event(f"Alert text: {e.alert_text}")
        scraper_state["last_error"] = "Server explicitly blocked connection."
        save_checkpoint(
            next_page=page_num,
            total_records=scraper_state["total_records"],
            last_error=scraper_state["last_error"],
        )
        return page_num, "BLOCKED"
    except (WebDriverException, Exception) as e:
        log_event(f"Scraping stopped unexpectedly on page {page_num}: {e}")
        scraper_state["last_error"] = str(e)
        save_checkpoint(
            next_page=page_num,
            total_records=scraper_state["total_records"],
            last_error=scraper_state["last_error"],
        )
        return page_num, "ERROR"
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def run_scraper_thread(start_page):
    current_page = start_page
    scraper_state["is_running"] = True
    
    while not stop_event.is_set():
        if scraper_state["status"] not in ["STOPPING", "STOPPED"]:
            scraper_state["status"] = "RUNNING"
            
        last_page_attempted, status = run_scraper(current_page)
        
        if status == "FINISHED":
            log_event(f"All pages successfully scraped up to {last_page_attempted}.")
            scraper_state["status"] = "FINISHED"
            scraper_state["consecutive_errors"] = 0
            break
        elif status == "STOPPED":
            log_event(f"Scraper stopped at page {last_page_attempted}.")
            scraper_state["status"] = "STOPPED"
            scraper_state["consecutive_errors"] = 0
            break
        elif status == "BLOCKED":
            wait_time = 30 # 30 seconds
            log_event(f"Scraper blocked at page {last_page_attempted}. Waiting {wait_time}s before auto-restart.")
            scraper_state["status"] = "BLOCKED (Auto-restarting)"
            scraper_state["consecutive_errors"] = 0
            current_page = last_page_attempted
            
            # Use a fast sleep check so we can stop during the wait
            for _ in range(wait_time * 2):
                if stop_event.is_set():
                    break
                time.sleep(0.5)
                
            if stop_event.is_set():
                break
                
            log_event(f"Auto-restarting from page {current_page}.")
        elif status == "ERROR":
            wait_time = 15 # 15 seconds
            scraper_state["consecutive_errors"] += 1
            if scraper_state["consecutive_errors"] >= MAX_CONSECUTIVE_ERRORS:
                scraper_state["status"] = "FAILED (manual restart required)"
                log_event(
                    f"Stopped after {scraper_state['consecutive_errors']} consecutive errors. "
                    f"Last error: {scraper_state['last_error']}"
                )
                break
            log_event(f"Error at page {last_page_attempted}. Retrying in {wait_time}s.")
            scraper_state["status"] = "ERROR (Auto-restarting)"
            current_page = last_page_attempted
            
            # Use a fast sleep check so we can stop during the wait
            for _ in range(wait_time * 2):
                if stop_event.is_set():
                    break
                time.sleep(0.5)

            if stop_event.is_set():
                break

            log_event(f"Retrying page {current_page}.")
        else:
            log_event(f"Unknown status '{status}' at page {last_page_attempted}. Exiting.")
            scraper_state["status"] = f"UNKNOWN ERROR: {status}"
            break
            
    scraper_state["is_running"] = False
    if scraper_state["status"] not in {"FINISHED", "FAILED (manual restart required)"}:
        scraper_state["status"] = "STOPPED"

if __name__ == "__main__":
    ckpt = load_checkpoint()
    # Keep in-memory totals consistent across restarts.
    scraper_state["total_records"] = ckpt.get("total_records", 0)
    try:
        user_val = input(
            f"\n🔢 Enter the page number to START from (default: {ckpt['next_page']}): "
        ).strip()
        current_page = int(user_val) if user_val else int(ckpt["next_page"])
    except ValueError:
        log_event(f"Invalid input, starting from page {ckpt['next_page']}.")
        current_page = int(ckpt["next_page"])
        
    run_scraper_thread(current_page)


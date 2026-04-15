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

MAX_CONCURRENT = 5       # Max simultaneous Firefox sessions
# NO LIMITS on pages, links, or log lines - will run until task completion

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


def _save_to_csv_fallback(session_id: str, docs: list):
    """Fallback CSV writer when MongoDB is unavailable"""
    import csv
    import os
    
    csv_file = f"generic_{session_id}_fallback.csv"
    file_exists = os.path.exists(csv_file)
    
    try:
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Content Type", "Extracted Data", "Extra Info"])
            for doc in docs:
                writer.writerow([
                    doc.get("content_type", ""),
                    doc.get("extracted_data", ""),
                    doc.get("extra_info", "")
                ])
    except Exception as e:
        print(f"CSV fallback also failed: {e}")


def _log(session_id: str, message: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(f"[{session_id}] {message}")
    with _sessions_lock:
        sess = generic_sessions.get(session_id)
        if sess is None:
            return
        sess["logs"].append(line)
        # Keep last 200 logs for UI display (unlimited in console/database)
        if len(sess["logs"]) > 200:
            sess["logs"] = sess["logs"][-200:]


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


def _extract_generic_content(driver, session_id: str) -> list[list[str]]:
    """
    Extracts ALL content via JavaScript in one shot — completely stale-element proof.
    Groups: Heading → merged paragraphs → links → tables → next Heading
    """
    all_data = []

    try:
        raw = driver.execute_script("""
        (function() {
            var EXCLUDE_TAGS = new Set(['nav','header','footer','aside','script',
                                        'style','iframe','ul','ol','li','noscript']);
            var EXCLUDE_KW   = ['sidebar','ad-','advertisement','related','trending',
                                'popular','widget','promo','banner','social','share',
                                'comment','navigation','menu','breadcrumb','sponsored',
                                'outbrain','taboola','recommend','disqus','livefyre',
                                'facebook-comment','fb-comment','comment-section',
                                'comment-box','comment-area','comment-list',
                                'user-comment','reader-comment','discussion',
                                'feedback','reply','replies','thread','conversation'];
            var SKIP_START   = ['share','follow us','subscribe','advertisement',
                                'sign up','log in','cookie','privacy policy','read more'];

            function isExcluded(el) {
                if (!el || el.nodeType !== 1) return true;
                var tag = el.tagName.toLowerCase();
                if (EXCLUDE_TAGS.has(tag)) return true;
                var combined = ((el.className || '') + ' ' + (el.id || '')).toLowerCase();
                for (var k = 0; k < EXCLUDE_KW.length; k++) {
                    if (combined.indexOf(EXCLUDE_KW[k]) !== -1) return true;
                }
                return false;
            }

            function hasExcludedAncestor(el) {
                var p = el.parentElement;
                while (p) {
                    if (isExcluded(p)) return true;
                    p = p.parentElement;
                }
                return false;
            }

            function getY(el) {
                var r = el.getBoundingClientRect();
                return r.top + window.pageYOffset;
            }

            // Find best main container
            var containerSelectors = [
                'article', 'main', '[role="main"]',
                '[class*="article"]', '[class*="story"]',
                '[class*="post-body"]', '[class*="entry-content"]',
                '[id*="article"]', '[id*="content"]', '[id*="story"]'
            ];
            var container = null;
            for (var s = 0; s < containerSelectors.length; s++) {
                var found = document.querySelector(containerSelectors[s]);
                if (found) { container = found; break; }
            }
            if (!container) container = document.body;

            // Collect headings
            var headings = [];
            var hTags = container.querySelectorAll('h1,h2,h3');
            for (var i = 0; i < hTags.length; i++) {
                var h = hTags[i];
                if (hasExcludedAncestor(h)) continue;
                var txt = h.innerText ? h.innerText.trim() : '';
                if (txt.length > 5) {
                    headings.push({ type: h.tagName + ' Heading', text: txt, y: getY(h), content: [] });
                }
            }
            headings.sort(function(a,b){ return a.y - b.y; });

            function findParent(y) {
                var best = null;
                for (var i = 0; i < headings.length; i++) {
                    if (headings[i].y <= y) best = headings[i];
                    else break;
                }
                return best;
            }

            // Collect paragraphs
            var pTags = container.querySelectorAll('p');
            for (var i = 0; i < pTags.length; i++) {
                var p = pTags[i];
                if (hasExcludedAncestor(p)) continue;
                var txt = p.innerText ? p.innerText.trim() : '';
                if (txt.length < 15) continue;
                var lower = txt.toLowerCase();
                var skip = false;
                for (var k = 0; k < SKIP_START.length; k++) {
                    if (lower.indexOf(SKIP_START[k]) === 0) { skip = true; break; }
                }
                if (skip) continue;
                var y = getY(p);
                var parent = findParent(y);
                var item = { type: 'Paragraph', text: txt, extra: '', y: y };
                if (parent) parent.content.push(item);
            }

            // Collect links
            var aTags = container.querySelectorAll('a[href]');
            var SKIP_LINK = ['share','tweet','facebook','subscribe','follow',
                             'login','sign up','read more','click here'];
            for (var i = 0; i < aTags.length; i++) {
                var a = aTags[i];
                if (hasExcludedAncestor(a)) continue;
                var txt = a.innerText ? a.innerText.trim() : '';
                var href = a.href || '';
                if (!txt || txt.length < 15 || !href.startsWith('http')) continue;
                var lower = txt.toLowerCase();
                var skip = false;
                for (var k = 0; k < SKIP_LINK.length; k++) {
                    if (lower.indexOf(SKIP_LINK[k]) !== -1) { skip = true; break; }
                }
                if (skip) continue;
                var y = getY(a);
                var parent = findParent(y);
                if (parent) parent.content.push({ type: 'Related Link', text: txt, extra: href, y: y });
            }

            // Build output: heading → merged paragraphs → links
            var result = [];
            for (var i = 0; i < headings.length; i++) {
                var h = headings[i];
                result.push([h.type, h.text, '']);
                h.content.sort(function(a,b){ return a.y - b.y; });

                var paras = [], links = [];
                for (var j = 0; j < h.content.length; j++) {
                    if (h.content[j].type === 'Paragraph') paras.push(h.content[j].text);
                    else links.push(h.content[j]);
                }
                if (paras.length > 0) {
                    result.push(['Article Content', paras.join(' '), '']);
                }
                for (var j = 0; j < links.length; j++) {
                    result.push([links[j].type, links[j].text, links[j].extra]);
                }
            }

            return result;
        })();
        """)

        if raw:
            all_data = [list(row) for row in raw]
            para_count = sum(1 for r in all_data if r[0] == 'Article Content')
            _log(session_id, f"  ✅ Extracted {len(all_data)} items ({para_count} article content blocks)")
            if all_data:
                for row in all_data:
                    if row[0] == 'Article Content' and row[1]:
                        preview = row[1][:120] + '...' if len(row[1]) > 120 else row[1]
                        _log(session_id, f"  📄 Sample: {preview}")
                        break
        else:
            _log(session_id, "  ⚠ No content extracted from page")

    except Exception as e:
        _log(session_id, f"  ✗ Extraction error: {e}")

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
    
    Mode 'single': Scrapes only the provided URL (no link following)
    Mode 'deep': Extracts ALL links from the start page, then scrapes each link individually
                 (opens link, scrapes ONLY that page's content, moves to next link)
    
    NO LIMITS - will run until all links are processed or user stops manually.
    """
    driver = None
    records = 0

    queue = []  # Start with empty queue
    visited = set()
    is_datatable = False

    try:
        driver = _make_driver()

        from urllib.parse import urlparse
        base_domain = urlparse(start_url).netloc

        # First, visit the start URL to collect links if in deep mode
        if mode == "deep":
            _log(session_id, f"Deep Crawl mode: Opening main page to collect all links...")
            _set_status(session_id, "COLLECTING LINKS")
            
            try:
                driver.get(start_url)
            except TimeoutException:
                _log(session_id, "Page load timed out. Forcing extract on loaded elements...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                _log(session_id, f"No <body> found on {start_url}. Aborting.")
                _set_status(session_id, "ERROR")
                return

            # Collect ALL internal links from the main page (article links only)
            _log(session_id, "Extracting all article links from main page...")
            # Extract all hrefs via JS in one shot to avoid stale element errors
            raw_hrefs = driver.execute_script("""
                var anchors = document.querySelectorAll('a[href]');
                var hrefs = [];
                for (var i = 0; i < anchors.length; i++) {
                    hrefs.push(anchors[i].href);
                }
                return hrefs;
            """)
            collected = 0
            seen = set()
            for href in (raw_hrefs or []):
                if not href or not isinstance(href, str):
                    continue
                href = href.split('#')[0].rstrip('/')  # strip fragments and trailing slash
                if href and href.startswith("http") and base_domain in href:
                    if href != start_url and href not in visited and href not in seen:
                        seen.add(href)
                        queue.append(href)
                        collected += 1
            
            _log(session_id, f"✓ Collected {collected} article links. Starting individual scraping...")
            _log(session_id, f"📰 Will scrape {len(queue)} articles")
            
            # Mark the main listing page as visited so we don't scrape it
            visited.add(start_url)
        else:
            # Single mode - just scrape the provided URL
            queue.append(start_url)

        # Now process each URL in the queue (article links only)
        link_counter = 0
        while queue and not stop_event.is_set():
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)
            link_counter += 1

            _log(session_id, f"📰 [{link_counter}/{link_counter + len(queue)}] Opening article: {url[:80]}")
            _set_status(session_id, "RUNNING")
            
            try:
                driver.get(url)
                _log(session_id, f"  ✓ Page loaded: {driver.title[:60]}")
            except TimeoutException:
                _log(session_id, "  ⚠ Page load timed out. Extracting available content...")

            try:
                WebDriverWait(driver, 15).until(  # Increased from 10 to 15 seconds
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Additional wait for article content to load
                time.sleep(2)  # Give extra time for dynamic content
            except TimeoutException:
                _log(session_id, f"  ✗ No <body> found. Skipping this link.")
                continue

            # Check for DataTables only on first page
            if len(visited) == 1:
                is_datatable = _has_datatable(driver)
                _log(session_id, f"DataTables detected: {is_datatable}")

            # Scrape this page (with DataTable pagination if applicable)
            page_num = 1
            while not stop_event.is_set():
                time.sleep(1)   # let DOM settle
                
                # Verify we're on the correct page
                current_url = driver.current_url
                _log(session_id, f"  📄 Extracting from: {current_url[:80]}")
                
                rows = _extract_generic_content(driver, session_id)

                # Add separator for each article
                if page_num == 1:
                    sep_doc = {
                        "session_id": session_id,
                        "content_type": "━━━ ARTICLE START ━━━",
                        "extracted_data": url,
                        "extra_info": f"Article {link_counter} of {link_counter + len(queue)}"
                    }
                    try:
                        from db import is_db_connected
                        if is_db_connected():
                            generic_collection.insert_one(sep_doc)
                        else:
                            _save_to_csv_fallback(session_id, [sep_doc])
                    except Exception as db_err:
                        _log(session_id, f"Database connection issue: {db_err}")
                        time.sleep(2)
                        try:
                            generic_collection.insert_one(sep_doc)
                        except:
                            pass  # Continue even if separator fails
                    
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
                    try:
                        from db import is_db_connected
                        if is_db_connected():
                            generic_collection.insert_many(docs)
                            _log(session_id, f"  ✓ Saved {len(docs)} items to database")
                        else:
                            # Fallback: save to CSV if DB is down
                            _save_to_csv_fallback(session_id, docs)
                            _log(session_id, f"  ⚠ DB unavailable - saved {len(docs)} items to CSV fallback")
                    except Exception as db_err:
                        _log(session_id, f"  Database error: {db_err}. Saving to CSV fallback...")
                        _save_to_csv_fallback(session_id, docs)
                        time.sleep(1)

                with _sessions_lock:
                    generic_sessions[session_id]["records"] = records

                _log(session_id, f"  Page {page_num}: {len(rows)} items extracted (total: {records})")

                # Handle DataTable pagination (only if detected)
                if is_datatable:
                    if _datatable_next_enabled(driver):
                        _click_datatable_next(driver)
                        page_num += 1
                    else:
                        _log(session_id, "  Reached last DataTable page for this link.")
                        break
                else:
                    # No DataTable - just scrape this single page and move on
                    break

            # Small delay between links to avoid overwhelming the server
            if queue:  # Only delay if there are more links to process
                time.sleep(0.5)

        # All links processed
        _log(session_id, f"✓ COMPLETED: Scraped {link_counter} links, {records} total records saved.")
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

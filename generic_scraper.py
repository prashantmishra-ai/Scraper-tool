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
from text_utils import normalize_text
import os
import time
import threading
from datetime import datetime
import html
import json
import re

MAX_CONCURRENT = 5       # Max simultaneous Firefox sessions
# NO LIMITS on pages, links, or log lines - will run until task completion

# ── Session registry ──────────────────────────────────────────────────────────
# { session_id: { url, status, records, logs, is_running, csv_path, error } }
generic_sessions: dict = {}
_sessions_lock = threading.Lock()

TRACKING_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "gclid", "fbclid", "igshid", "cmpid", "gaa_at", "smid",
    "s", "ss", "sr_share", "ncid", "taid", "output"
}


def _make_session(session_id: str, url: str, mode: str = "single") -> dict:
    return {
        "session_id": session_id,
        "url": url,
        "mode": mode,
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
        encoding = 'utf-8-sig' if not file_exists else 'utf-8'
        with open(csv_file, 'a', newline='', encoding=encoding) as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["Content Type", "Extracted Data", "Extra Info"])
            for doc in docs:
                writer.writerow([
                    normalize_text(doc.get("content_type", "")),
                    normalize_text(doc.get("extracted_data", "")),
                    normalize_text(doc.get("extra_info", ""))
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


def _canonicalize_url(url: str) -> str:
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    if not isinstance(url, str) or not url.strip():
        return ""

    parsed = urlparse(url.strip())
    filtered_query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_KEYS
    ]
    cleaned = parsed._replace(
        fragment="",
        query=urlencode(filtered_query, doseq=True),
        path=(parsed.path or "/").rstrip("/") if parsed.path not in {"", "/"} else parsed.path,
    )
    normalized = urlunparse(cleaned)
    return normalized.rstrip("/") if normalized.endswith("/") and cleaned.path not in {"", "/"} else normalized


def _safe_body_text(driver) -> str:
    try:
        return driver.find_element(By.TAG_NAME, "body").text.strip()
    except Exception:
        return ""


def _make_driver():
    """Create a headless Firefox instance using the same env-aware logic."""
    options = FirefoxOptions()
    options.page_load_strategy = 'eager'  # Do not wait for heavy ads and tracking scripts
    
    # Add realistic user agent to avoid CAPTCHA/bot detection
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    options.set_preference("general.useragent.override", user_agent)
    
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


def _dismiss_common_overlays(driver, session_id: str):
    overlay_xpaths = [
        "//button[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'accept')]",
        "//button[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'agree')]",
        "//button[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'continue')]",
        "//button[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'got it')]",
        "//button[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'ok')]",
        "//a[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'accept')]",
        "//a[contains(translate(normalize-space(.), 'ACEGILNOPTUY', 'acegilnoptuy'), 'continue')]",
    ]

    clicked = 0
    for xpath in overlay_xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
        except Exception:
            continue

        for element in elements[:3]:
            try:
                if not element.is_displayed():
                    continue
                text = (element.text or "").strip().lower()
                if not text:
                    continue
                if text in {"ok"} or "accept" in text or "agree" in text or "continue" in text or "got it" in text:
                    driver.execute_script("arguments[0].click();", element)
                    clicked += 1
                    time.sleep(0.5)
                    if clicked >= 2:
                        break
            except Exception:
                continue
        if clicked >= 2:
            break

    if clicked:
        _log(session_id, f"  ✓ Dismissed {clicked} overlay prompt(s)")


def _wait_for_page_ready(driver, session_id: str, extra_wait: float = 2.0):
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except TimeoutException:
        raise

    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") in {"interactive", "complete"}
        )
    except Exception:
        pass

    try:
        _dismiss_common_overlays(driver, session_id)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 0);")
    except Exception:
        pass

    time.sleep(extra_wait)


def _extract_article_metadata(driver, session_id: str) -> dict:
    """
    Extracts article metadata: published date/time, updated date/time, read time, source, category.
    Returns a dict with keys: published_date, published_time, updated_date, updated_time, read_time, source, category
    
    Uses multiple strategies to find metadata:
    1. Direct XPath/CSS selectors for common patterns
    2. Full page text scan with regex patterns
    3. Meta tag extraction
    """
    import re
    from datetime import datetime as dt
    
    metadata = {
        "published_date": "",
        "published_time": "",
        "updated_date": "",
        "updated_time": "",
        "read_time": "",
        "source": "",
        "category": ""
    }
    
    try:
        # Strategy 1: Extract all text from page and search for date patterns
        try:
            page_text = driver.find_element(By.TAG_NAME, 'body').text
        except:
            page_text = ""
        
        # Find published date/time from page text
        # Pattern: "Published OnApr 16, 2026 09:51 am IST" or "Apr 16, 2026 09:51 am"
        pub_pattern = r'Published\s*(?:On)?\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*(\d{1,2})[\s,]*(\d{4})[\s]*(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)?'
        pub_match = re.search(pub_pattern, page_text.replace('\n', ' '))
        if pub_match:
            metadata["published_date"] = f"{pub_match.group(1)} {pub_match.group(2)}, {pub_match.group(3)}"
            metadata["published_time"] = f"{pub_match.group(4)}:{pub_match.group(5)} {pub_match.group(6) or ''}".strip()
        
        # Find last updated date/time from page text
        # Pattern: "Last Updated OnApr 16, 2026 09:52 am IST"
        upd_pattern = r'Last\s+Updated\s*(?:On)?\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*(\d{1,2})[\s,]*(\d{4})[\s]*(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)?'
        upd_match = re.search(upd_pattern, page_text.replace('\n', ' '))
        if upd_match:
            metadata["updated_date"] = f"{upd_match.group(1)} {upd_match.group(2)}, {upd_match.group(3)}"
            metadata["updated_time"] = f"{upd_match.group(4)}:{upd_match.group(5)} {upd_match.group(6) or ''}".strip()
        
        # Find read time from page text
        # Pattern: "Read Time: 4 mins" or "Reading Time: 4 mins"
        read_pattern = r'(?:Read|Reading)\s+(?:Time)?[\s:]*(\d+)\s*mins?'
        read_match = re.search(read_pattern, page_text, re.IGNORECASE)
        if read_match:
            metadata["read_time"] = f"{read_match.group(1)} mins"
        
        # Strategy 2: Try meta tags for additional metadata
        try:
            meta_tags = driver.find_elements(By.TAG_NAME, 'meta')
            for meta in meta_tags:
                name = (meta.get_attribute('name') or '').lower()
                content = meta.get_attribute('content') or ''
                
                if 'publish' in name and not metadata["published_date"]:
                    if content:
                        date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d+', content)
                        if date_match:
                            metadata["published_date"] = date_match.group(0)
                
                if 'category' in name and not metadata["category"]:
                    metadata["category"] = content[:50]  # Keep it short
                
                if 'author' in name and not metadata["source"]:
                    metadata["source"] = content[:50]
        except:
            pass
        
        # Strategy 3: Look for structured metadata in specific elements
        try:
            # Try article meta sections
            meta_selectors = [
                "article [class*='meta']",
                "[class*='article-meta']",
                "[class*='article-info']",
                "[class*='story-meta']"
            ]
            
            for selector in meta_selectors:
                try:
                    meta_elem = driver.find_element(By.CSS_SELECTOR, selector)
                    meta_text = meta_elem.text
                    
                    # Look for dates in meta text
                    if ' Apr ' in meta_text or 'published' in meta_text.lower():
                        date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d+', meta_text)
                        if date_match and not metadata["published_date"]:
                            metadata["published_date"] = date_match.group(0)
                        
                        time_match = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm|AM|PM)?', meta_text)
                        if time_match and not metadata["published_time"]:
                            metadata["published_time"] = time_match.group(0)
                except:
                    continue
        except:
            pass
        
        # 4. Extract read time
        read_time_patterns = [
            "//span[contains(text(), 'Read Time')]/following-sibling::*",
            "//text()[contains(., 'Read Time')]/following::span",
            "[class*='read'][class*='time']",
            "[class*='reading'][class*='time']"
        ]
        
        for pattern in read_time_patterns:
            try:
                if pattern.startswith("//"):
                    elements = driver.find_elements(By.XPATH, pattern)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                
                for elem in elements:
                    text = elem.text.strip()
                    if text and ('min' in text.lower() or 'minute' in text.lower()):
                        metadata["read_time"] = text
                        break
            except:
                continue
            
            if metadata["read_time"]:
                break
        
        # 5. Extract source/agency
        source_patterns = [
            "//span[contains(text(), 'Reuters') or contains(text(), 'News') or contains(text(), 'Agency')]",
            "//a[contains(@href, 'agencies') or contains(@href, 'author')]",
            "[class*='source']",
            "[class*='agency']",
            "[class*='author']"
        ]
        
        for pattern in source_patterns:
            try:
                if pattern.startswith("//"):
                    elements = driver.find_elements(By.XPATH, pattern)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                
                for elem in elements:
                    text = elem.text.strip()
                    if text and len(text) < 100:  # Source names are usually short
                        metadata["source"] = text
                        break
            except:
                continue
            
            if metadata["source"]:
                break
        
        # 6. Extract category (usually in breadcrumb or navigation)
        category_patterns = [
            "//a[contains(@href, 'india-news') or contains(@href, 'world-news') or contains(@href, 'news')]",
            "//li[contains(@class, 'breadcrumb')]//a",
            "[class*='category']",
            "[class*='section']"
        ]
        
        for pattern in category_patterns:
            try:
                if pattern.startswith("//"):
                    elements = driver.find_elements(By.XPATH, pattern)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                
                for elem in elements:
                    text = elem.text.strip()
                    if text and 'news' in text.lower() and len(text) < 50:
                        metadata["category"] = text
                        break
            except:
                continue
            
            if metadata["category"]:
                break
        
        _log(session_id, f"  📋 Metadata: {metadata['published_date']} {metadata['published_time']} | Updated: {metadata['updated_date']} | Read: {metadata['read_time']} | Source: {metadata['source']}")
        
    except Exception as e:
        _log(session_id, f"  ⚠ Metadata extraction issue: {e}")
    
    return metadata


def _get_site_specific_selectors(driver, url: str) -> list:
    """
    Returns site-specific content selectors based on the website URL
    """
    from urllib.parse import urlparse
    
    domain = urlparse(url).netloc.lower()
    
    # Base selectors that work for most news sites
    base_selectors = [
        'article', 'main', '[role="main"]',
        '[class*="article-content"]', '[class*="story-content"]',
        '[class*="article-text"]', '[class*="story-text"]',
        '[class*="article-body"]', '[class*="story-body"]',
        '[class*="post-content"]', '[class*="post-body"]', '[class*="entry-content"]',
    ]
    
    # Site-specific selectors
    if 'aajtak' in domain:
        # aajtak.in specific selectors
        return [
            '[class*="article-headline"]',
            '[class*="content-area"]',
            '[class*="story-"]',
            'article',
            'main',
        ] + base_selectors
    
    elif 'ndtv' in domain:
        # NDTV specific selectors
        return [
            '[data-attr-arrowhead="article-content"]',
            '[class*="article-content"]',
            'article',
            'main',
        ] + base_selectors
    
    elif 'timesofindia' in domain or 'indiatimes' in domain:
        # Times of India (timesofindia.indiatimes.com) specific selectors
        return [
            '[data-testid="article-details"]',
            '[class*="article-details"]',
            '[class*="story-content"]',
            '[class$="_strycntr"]',
            '[class*="article-body"]',
            '[class*="article-inner"]',
            '[class*="article-text"]',
            '[class*="content-wrapper"]',
            '[class*="article--content"]',
            'article',
            'main',
        ] + base_selectors
    
    elif 'nytimes' in domain:
        # New York Times (nytimes.com) specific selectors
        return [
            '[data-testid="article"]',
            '[data-testid="story"]',
            'article[role="article"]',
            '[class*="article-body"]',
            '[class*="article-content"]',
            '[class*="story-body"]',
            '[class*="story-content"]',
            '[id*="story-body"]',
            'article',
            'main',
        ] + base_selectors
    
    # Default selectors for unknown sites
    return base_selectors + [
        '[class*="article"]', '[class*="content"]', '[class*="story"]',
        '[id*="article"]', '[id*="content"]', '[id*="story"]'
    ]


def _clean_embedded_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = html.unescape(text)
    text = text.replace("\\n", "\n").replace("\\r", "\n").replace("\\t", " ")
    text = re.sub(r"\s+", " ", text)
    return normalize_text(text.strip())


def _walk_json(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_json(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)


def _extract_embedded_article_data_from_html(page_source: str) -> dict:
    data = {
        "headline": "",
        "description": "",
        "article_body": "",
        "published_date": "",
        "updated_date": "",
        "source": "",
        "category": "",
    }
    if not page_source:
        return data

    candidates = []
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        page_source,
        re.IGNORECASE | re.DOTALL,
    ):
        payload = match.group(1).strip()
        if payload:
            candidates.append(payload)

    for raw in candidates:
        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        for node in _walk_json(parsed):
            node_type = str(node.get("@type", "")).lower()
            if not any(marker in node_type for marker in ("newsarticle", "article", "reportage", "analysisnewsarticle")):
                continue

            if not data["headline"]:
                data["headline"] = _clean_embedded_text(node.get("headline") or node.get("name") or "")
            if not data["description"]:
                data["description"] = _clean_embedded_text(node.get("description") or "")
            if not data["article_body"]:
                data["article_body"] = _clean_embedded_text(node.get("articleBody") or node.get("text") or "")
            if not data["published_date"]:
                data["published_date"] = _clean_embedded_text(node.get("datePublished") or "")
            if not data["updated_date"]:
                data["updated_date"] = _clean_embedded_text(node.get("dateModified") or "")
            if not data["category"]:
                data["category"] = _clean_embedded_text(node.get("articleSection") or node.get("section") or "")

            author = node.get("author")
            if not data["source"]:
                if isinstance(author, dict):
                    data["source"] = _clean_embedded_text(author.get("name") or "")
                elif isinstance(author, list):
                    names = []
                    for item in author:
                        if isinstance(item, dict) and item.get("name"):
                            names.append(_clean_embedded_text(item["name"]))
                    data["source"] = ", ".join([name for name in names if name])

    if not data["headline"]:
        match = re.search(r'"headline"\s*:\s*"((?:\\.|[^"\\])*)"', page_source, re.DOTALL)
        if match:
            data["headline"] = _clean_embedded_text(json.loads(f'"{match.group(1)}"'))

    if not data["article_body"]:
        match = re.search(r'"articleBody"\s*:\s*"((?:\\.|[^"\\])*)"', page_source, re.DOTALL)
        if match:
            data["article_body"] = _clean_embedded_text(json.loads(f'"{match.group(1)}"'))

    if not data["description"]:
        match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', page_source, re.IGNORECASE | re.DOTALL)
        if match:
            data["description"] = _clean_embedded_text(match.group(1))

    meta_patterns = {
        "headline": [
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\'](.*?)["\']',
        ],
        "description": [
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\'](.*?)["\']',
        ],
        "published_date": [
            r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']article.published["\'][^>]+content=["\'](.*?)["\']',
        ],
        "updated_date": [
            r'<meta[^>]+property=["\']article:modified_time["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']last-modified["\'][^>]+content=["\'](.*?)["\']',
        ],
        "source": [
            r'<meta[^>]+name=["\']author["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+property=["\']article:author["\'][^>]+content=["\'](.*?)["\']',
        ],
        "category": [
            r'<meta[^>]+property=["\']article:section["\'][^>]+content=["\'](.*?)["\']',
            r'<meta[^>]+name=["\']news_keywords["\'][^>]+content=["\'](.*?)["\']',
        ],
    }

    for field, patterns in meta_patterns.items():
        if data[field]:
            continue
        for pattern in patterns:
            match = re.search(pattern, page_source, re.IGNORECASE | re.DOTALL)
            if match:
                data[field] = _clean_embedded_text(match.group(1))
                if data[field]:
                    break

    return data


def _extract_candidate_links_from_page(driver, session_id: str, start_url: str) -> list[str]:
    from urllib.parse import urljoin, urlparse

    base_domain = urlparse(start_url).netloc.lower()
    candidates = []
    seen = set()

    def add_candidate(url: str):
        canonical = _canonicalize_url(url)
        if not canonical or canonical in seen:
            return
        if _is_article_url(canonical, base_domain):
            seen.add(canonical)
            candidates.append(canonical)

    try:
        for anchor in driver.find_elements(By.TAG_NAME, "a"):
            href = anchor.get_attribute("href")
            if not href:
                continue
            add_candidate(urljoin(start_url, href))
    except Exception as exc:
        _log(session_id, f"  DOM link scan issue: {exc}")

    page_source = driver.page_source or ""
    for match in re.finditer(r'href=["\'](.*?)["\']', page_source, re.IGNORECASE):
        add_candidate(urljoin(start_url, html.unescape(match.group(1))))

    for match in re.finditer(r'"url"\s*:\s*"((?:\\.|[^"\\])*)"', page_source):
        try:
            add_candidate(urljoin(start_url, json.loads(f'"{match.group(1)}"')))
        except Exception:
            continue

    return candidates


def _extract_embedded_article_rows(driver, session_id: str) -> list[list[str]]:
    try:
        embedded = _extract_embedded_article_data_from_html(driver.page_source)
    except Exception as exc:
        _log(session_id, f"  Embedded article fallback failed: {exc}")
        return []

    rows = []
    if embedded["published_date"]:
        rows.append(["Published Date", embedded["published_date"], ""])
    if embedded["updated_date"]:
        rows.append(["Last Updated Date", embedded["updated_date"], ""])
    if embedded["source"]:
        rows.append(["Source", embedded["source"], ""])
    if embedded["category"]:
        rows.append(["Category", embedded["category"], ""])
    if embedded["headline"]:
        rows.append(["H1 Heading", embedded["headline"], ""])

    article_text = embedded["article_body"] or embedded["description"]
    if article_text:
        rows.append(["Article Content", article_text, ""])

    if rows:
        _log(session_id, f"  ✓ Recovered {len(rows)} rows from embedded article data")
    return rows


def _page_has_extractable_content(driver) -> tuple[bool, int]:
    body_text = _safe_body_text(driver)

    if len(body_text) >= 50:
        return True, len(body_text)

    try:
        embedded = _extract_embedded_article_data_from_html(driver.page_source)
    except Exception:
        embedded = {}

    embedded_text = " ".join([
        embedded.get("headline", ""),
        embedded.get("description", ""),
        embedded.get("article_body", ""),
    ]).strip()
    if len(embedded_text) >= 80:
        return True, len(body_text)

    return False, len(body_text)


def _extract_generic_content(driver, session_id: str) -> list[list[str]]:
    """
    Extracts ALL content from news/article pages in a hierarchical structure.
    Groups content by headings: Heading → All related content (paragraphs, links, tables) → Next Heading
    Filters out ads, sidebars, navigation, and list items.
    Returns formatting: [Content Type, Text/Data, Extra Info]
    
    AGGRESSIVE EXTRACTION: Captures every paragraph, even short ones, to ensure complete content.
    Includes metadata extraction: published date, updated date, source, category.
    Uses site-specific selectors for better content detection.
    """
    all_data = []
    
    # Extract metadata first
    metadata = _extract_article_metadata(driver, session_id)
    
    # Add metadata to output if present
    if metadata["published_date"]:
        all_data.append(["Published Date", metadata["published_date"], ""])
    if metadata["published_time"]:
        all_data.append(["Published Time", metadata["published_time"], ""])
    if metadata["updated_date"]:
        all_data.append(["Last Updated Date", metadata["updated_date"], ""])
    if metadata["updated_time"]:
        all_data.append(["Last Updated Time", metadata["updated_time"], ""])
    if metadata["source"]:
        all_data.append(["Source", metadata["source"], ""])
    if metadata["category"]:
        all_data.append(["Category", metadata["category"], ""])

    # Get site-specific selectors
    url = driver.current_url
    main_content_selectors = _get_site_specific_selectors(driver, url)
    
    main_container = None
    for selector in main_content_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                main_container = elements[0]
                _log(session_id, f"  ✓ Found content using selector: {selector[:50]}")
                break
        except:
            continue
    
    # If no main container found, use body
    if not main_container:
        try:
            main_container = driver.find_element(By.TAG_NAME, 'body')
        except:
            return all_data

    def is_excluded_element(element):
        """Check if element should be excluded (ads, sidebars, trending news, etc.)"""
        try:
            tag_name = element.tag_name.lower()
            if tag_name in ['nav', 'header', 'footer', 'aside', 'script', 'style', 'iframe', 'ul', 'ol', 'li']:
                return True
            
            class_attr = element.get_attribute('class') or ''
            id_attr = element.get_attribute('id') or ''
            text_content = element.text.lower() if hasattr(element, 'text') else ''
            combined = (class_attr + ' ' + id_attr).lower()
            
            # Comprehensive exclusion keywords
            exclude_keywords = [
                'sidebar', 'ad-', 'advertisement', 'related', 'trending',
                'popular', 'widget', 'promo', 'banner', 'social', 'share',
                'comment', 'navigation', 'menu', 'breadcrumb', 'footer',
                'header', 'sponsored', 'outbrain', 'taboola', 'recommend',
                'marquee', 'ticker', 'marquee-item', 'live-blog',
                'breaking', 'latest', 'story-list', 'article-list',
                'news-list', 'video-list', 'photo-gallery', 'gallery',
                'embed', 'twitter', 'facebook', 'instagram', 'youtube'
            ]
            
            for keyword in exclude_keywords:
                if keyword in combined:
                    return True
            
            # Exclude common trending/sidebar text patterns
            trending_indicators = [
                'ये भी पढ़ें',  # Hindi: "Also read"
                'यह भी देखें',  # Hindi: "Also watch"
                'आप पढ़ रहे हैं',  # Hindi: "You are reading"
                'भी देखें', # Also watch
                'ट्रेंडिंग',  # Trending
                'trending',
                'latest news',
                'breaking news',
                'also check',
                'more from'
            ]
            
            for indicator in trending_indicators:
                if indicator.lower() in text_content:
                    return True
            
            return False
        except:
            return False

    def get_element_position(element):
        """Get the vertical position of an element in the DOM"""
        try:
            return driver.execute_script(
                "return arguments[0].getBoundingClientRect().top + window.pageYOffset;",
                element
            )
        except:
            return 0

    def get_parent_heading(element, all_headings):
        """Find the closest heading above this element"""
        try:
            element_pos = get_element_position(element)
            # Find the last heading that appears before this element
            closest_heading = None
            for heading in all_headings:
                if heading['position'] <= element_pos:
                    closest_heading = heading
                else:
                    break
            return closest_heading
        except:
            return None

    # Step 1: Collect all headings with their positions
    all_headings = []
    try:
        for tag in ['h1', 'h2', 'h3']:
            for e in main_container.find_elements(By.TAG_NAME, tag):
                if is_excluded_element(e):
                    continue
                text = e.text.strip()
                if text and len(text) > 5:
                    position = get_element_position(e)
                    all_headings.append({
                        'type': f'{tag.upper()} Heading',
                        'text': text,
                        'position': position,
                        'content': []  # Will store related content here
                    })
    except:
        pass

    # Sort headings by position
    all_headings.sort(key=lambda x: x['position'])

    # Step 2: Collect ALL paragraphs and assign to headings
    paragraph_count = 0
    try:
        all_paragraphs = main_container.find_elements(By.TAG_NAME, 'p')
        _log(session_id, f"  📝 Found {len(all_paragraphs)} total <p> tags on page")
        
        for e in all_paragraphs:
            if is_excluded_element(e):
                continue
            text = e.text.strip()
            
            # More lenient filter - capture more content
            # Skip only very short text and obvious UI elements
            if text and len(text) > 15:  # Reduced from 20 to capture even more
                # Skip only obvious non-content
                skip_phrases = [
                    'share', 'follow us', 'subscribe', 'advertisement', 
                    'sign up', 'log in', 'cookie', 'privacy policy', 'read more',
                    'यह भी देखें', 'भी देखें', 'ये भी पढ़ें', 'भी पढ़ें',
                    'ट्रेंडिंग', 'latest', 'breaking', 'also check', 'more from',
                    'आप पढ़', 'ये है', 'यह है'
                ]
                if any(text.lower().startswith(phrase.lower()) for phrase in skip_phrases):
                    continue
                
                # Additional check: skip paragraphs that contain only section/category names
                if len(text.split()) < 5 and any(x in text.lower() for x in ['latest', 'trending', 'breaking', 'news']):
                    continue
                
                position = get_element_position(e)
                parent_heading = get_parent_heading(e, all_headings)
                
                content_item = {
                    'type': 'Paragraph',
                    'text': text,
                    'extra': '',
                    'position': position
                }
                
                paragraph_count += 1
                
                if parent_heading:
                    parent_heading['content'].append(content_item)
                else:
                    # Content before any heading - add to beginning
                    all_data.append(['Paragraph', text, ''])
        
        _log(session_id, f"  ✅ Extracted {paragraph_count} paragraphs after filtering")
        
        # Log first paragraph as sample
        if paragraph_count > 0:
            first_para = all_data[0][1] if all_data and all_data[0][0] == 'Paragraph' else None
            if not first_para and all_headings and all_headings[0]['content']:
                first_para = all_headings[0]['content'][0]['text']
            if first_para:
                preview = first_para[:100] + "..." if len(first_para) > 100 else first_para
                _log(session_id, f"  📄 Sample paragraph: {preview}")
    except Exception as e:
        _log(session_id, f"  Error extracting paragraphs: {e}")

    # Step 2b: Also collect text from div elements (some sites use divs for article content)
    div_count = 0
    try:
        for e in main_container.find_elements(By.TAG_NAME, 'div'):
            if is_excluded_element(e):
                continue
            
            # Only get direct text from this div (not nested elements)
            try:
                text = driver.execute_script("""
                    var element = arguments[0];
                    var text = '';
                    for (var i = 0; i < element.childNodes.length; i++) {
                        var node = element.childNodes[i];
                        if (node.nodeType === Node.TEXT_NODE) {
                            text += node.textContent;
                        }
                    }
                    return text.trim();
                """, e)
                
                if text and len(text) > 50:  # Only substantial div text
                    position = get_element_position(e)
                    parent_heading = get_parent_heading(e, all_headings)
                    
                    content_item = {
                        'type': 'Content Block',
                        'text': text,
                        'extra': '',
                        'position': position
                    }
                    
                    div_count += 1
                    
                    if parent_heading:
                        parent_heading['content'].append(content_item)
                    else:
                        all_data.append(['Content Block', text, ''])
            except:
                continue
        
        if div_count > 0:
            _log(session_id, f"  Extracted {div_count} additional content blocks from divs")
    except Exception as e:
        _log(session_id, f"  Error extracting div content: {e}")

    # Step 3: Collect links and assign to headings
    try:
        for e in main_container.find_elements(By.TAG_NAME, 'a'):
            if is_excluded_element(e):
                continue
            text = e.text.strip()
            href = e.get_attribute('href')
            # Only include links with substantial text and valid URLs
            if text and href and href.startswith('http') and len(text) > 15:
                # Skip common navigation/social links
                if not any(skip in text.lower() for skip in ['share', 'tweet', 'facebook', 'subscribe', 'follow', 'login', 'sign up', 'read more', 'click here']):
                    position = get_element_position(e)
                    parent_heading = get_parent_heading(e, all_headings)
                    
                    content_item = {
                        'type': 'Related Link',
                        'text': text,
                        'extra': href,
                        'position': position
                    }
                    
                    if parent_heading:
                        parent_heading['content'].append(content_item)
    except:
        pass

    # Step 4: Collect tables and assign to headings
    try:
        tables = main_container.find_elements(By.TAG_NAME, "table")
        for t_idx, table in enumerate(tables):
            if is_excluded_element(table):
                continue
            position = get_element_position(table)
            parent_heading = get_parent_heading(table, all_headings)
            
            table_rows = []
            for r_idx, row in enumerate(table.find_elements(By.TAG_NAME, "tr")):
                cells = row.find_elements(By.XPATH, ".//th | .//td")
                text_cells = " | ".join([c.text.strip() for c in cells if c.text.strip()])
                if text_cells:
                    table_rows.append({
                        'type': f'Table {t_idx+1}',
                        'text': text_cells,
                        'extra': f'Row {r_idx+1}',
                        'position': position + (r_idx * 0.1)
                    })
            
            if parent_heading:
                parent_heading['content'].extend(table_rows)
            else:
                # Table before any heading
                for row in table_rows:
                    all_data.append([row['type'], row['text'], row['extra']])
    except:
        pass

    # Step 5: Build final output - Heading followed by MERGED content
    total_items = 0
    for heading in all_headings:
        # Add the heading
        all_data.append([heading['type'], heading['text'], ''])
        total_items += 1
        
        # Sort content under this heading by position
        heading['content'].sort(key=lambda x: x['position'])
        
        # MERGE all paragraphs into a single combined paragraph
        paragraphs = []
        other_content = []
        
        for item in heading['content']:
            if item['type'] in ['Paragraph', 'Content Block']:
                paragraphs.append(item['text'])
            else:
                other_content.append(item)
        
        # Add merged paragraph if any paragraphs exist
        if paragraphs:
            merged_text = ' '.join(paragraphs)  # Join all paragraphs with space
            all_data.append(['Article Content', merged_text, ''])
            total_items += 1
        
        # Add other content (links, tables) separately
        for item in other_content:
            all_data.append([item['type'], item['text'], item['extra']])
            total_items += 1
    
    # Log extraction summary
    try:
        # Count by type
        type_counts = {}
        for item in all_data:
            content_type = item[0]
            type_counts[content_type] = type_counts.get(content_type, 0) + 1
        
        summary = ", ".join([f"{count} {ctype}" for ctype, count in type_counts.items()])
        _log(session_id, f"  Extraction complete: {total_items} total items ({summary})")
    except Exception as e:
        pass

    has_story_rows = any(row[0] in {"Article Content", "Paragraph", "Content Block"} for row in all_data)
    if has_story_rows:
        return all_data

    fallback_rows = _extract_embedded_article_rows(driver, session_id)
    if not fallback_rows:
        return all_data

    existing_pairs = {(row[0], row[1]) for row in all_data}
    for row in fallback_rows:
        if (row[0], row[1]) not in existing_pairs:
            all_data.append(row)

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


def _is_article_url(url: str, base_domain: str) -> bool:
    """
    Determines if a URL is an actual article page vs a category/listing page.
    Article URLs typically have a numeric ID at the end or contain news path segments.
    Supports NDTV, Times of India, aajtak, and other news sites.
    """
    from urllib.parse import urlparse
    parsed = urlparse(_canonicalize_url(url))
    
    # Must be same domain
    if base_domain not in parsed.netloc:
        return False
    
    path = parsed.path.rstrip('/')
    # Times of India specific patterns
    if 'indiatimes' in base_domain or 'timesofindia' in base_domain:
        # TOI articles have /articleshow/ followed by numbers
        if '/articleshow/' in path:
            return True
        # Or /business/article/, /india/article/, /sport/article/, etc.
        if '/article/' in path or '/photo/' in path or '/video/' in path:
            if not any(x in path for x in ['/topic/', '/search', '/tag/', '/author/']):
                return True
        
        # Skip policy/legal/footer pages
        policy_keywords = ['privacy', 'policy', 'terms', 'contact', 'about', 'faq', 'sitemap', 'ad', 'advertising', 'advertise', 'feed']
        if any(kw in path.lower() for kw in policy_keywords):
            return False
        
        # Or general TOI article paths: /section/subsection/article-slug or /section/article-slug/
        segments = [s for s in path.split('/') if s]
        if len(segments) == 1:
            # Single segment like /privacy-policy should be rejected
            return False
        if len(segments) >= 2:
            # Skip known non-article sections
            first_segment = segments[0].lower()
            non_article_sections = ['blog', 'topic', 'search', 'tag', 'author', 'page']
            if first_segment not in non_article_sections:
                # Check if last segment looks like an article slug (has hyphens/numbers but not too short)
                last_segment = segments[-1]
                # Article slugs are typically longer than 10 chars with hyphens
                if len(last_segment) > 10 and '-' in last_segment:
                    return True
            # Or look for explicit news keywords
            potential_article_keywords = ['news', 'story', '/india/', '/business/', '/sports/']
            if any(keyword in path.lower() for keyword in potential_article_keywords):
                return True
    
    # aajtak specific patterns
    if 'aajtak' in base_domain:
        if '/story/' in path or '/video/' in path:
            # aajtak: /india/news/story/... or /short-videos/video/...
            if not any(x in path for x in ['/topic/', '/search', '/tag/']):
                return True
    
    # NDTV specific patterns
    if 'ndtv' in base_domain:
        # NDTV-style: ends with -XXXXXXXX (8+ digit number)
        import re
        if re.search(r'-\d{6,}$', path):
            return True
    
    # New York Times specific patterns
    if 'nytimes' in base_domain:
        # NYT: /YYYY/MM/DD/section/slug.html
        import re
        if re.match(r'/\d{4}/\d{2}/\d{2}/[a-z-]+/.+\.html', path):
            return True
        # Or articles with /article/ path
        if '/article/' in path or '/briefing/' in path or '/interactive/' in path:
            if not any(x in path for x in ['/search', '/tag/', '/topic/']):
                return True

    if any(domain in base_domain for domain in ('cnn.com', 'bbc.com', 'reuters.com', 'thehindu.com', 'hindustantimes.com', 'indianexpress.com')):
        if re.search(r'/\d{4}/\d{2}/\d{2}/', path):
            return True
        if any(token in path for token in ('/story/', '/news/', '/article/', '/articles/', '/world/', '/india/', '/politics/', '/business/')):
            segments = [s for s in path.split('/') if s]
            if len(segments) >= 2 and len(segments[-1]) > 12:
                return True
    
    # Skip obvious non-article pages
    skip_paths = [
        '/video-', '/live', '/videos', '/photos', '/gallery',  # Keep /video alone (TOI)
        '/topic/', '/topics/', '/search', '/tag/', '/tags/',
        '/author/', '/authors/', '/agencies/', '/page/',
        '/world/diaspora', '/india-global', '/cities',
        '/opinion', '/offbeat', '/sports/', '/entertainment/',
        '/tech/', '/education/', '/lifestyle/', '/travel/', '/food/',
        '/privacy', '/terms', '/contact', '/about', '/newsletter',
        '/weather', '/crossword', '/games', '/podcasts',
    ]
    
    for skip in skip_paths:
        if path == skip or path.startswith(skip):
            return False
    
    # Skip root and very short paths (category pages)
    if len(path) < 10:
        return False
    
    # Skip paths that are just section names (no article slug)
    # Article URLs typically end with a numeric ID
    import re
    # NDTV-style: ends with -XXXXXXXX (8+ digit number)
    if re.search(r'-\d{6,}$', path):
        return True
    
    # Generic: has a long slug with hyphens (likely an article)
    segments = [s for s in path.split('/') if s]
    if len(segments) >= 2:
        last_segment = segments[-1]
        # Article slugs are typically long with hyphens
        if len(last_segment) > 15 and '-' in last_segment:
            # Make sure it's not just a category name
            if not last_segment.replace('-', '').isalpha():  # Should have numbers or special chars
                return True
    
    return False


def run_generic_scraper(session_id: str, start_url: str, mode: str, stop_event: threading.Event):
    """
    Core scraping function. Runs in its own thread.
    
    Mode 'single': Scrapes only the provided URL
    Mode 'deep':
        1. Opens the start URL (listing/category page)
        2. Collects ONLY actual article links (filters out category/nav links)
        3. Opens each article link one by one
        4. Extracts the FULL article content (all paragraphs merged)
        5. Saves: Article URL → Heading → Full merged content → Links
        6. Moves to next article
    """
    driver = None
    records = 0
    from urllib.parse import urlparse

    queue = []
    visited = set()
    is_datatable = False

    try:
        driver = _make_driver()
        base_domain = urlparse(start_url).netloc

        if mode == "deep":
            _log(session_id, f"Opening listing page to collect article links...")
            _set_status(session_id, "COLLECTING LINKS")

            try:
                driver.get(start_url)
            except TimeoutException:
                _log(session_id, "Page load timed out. Extracting available links...")

            try:
                _wait_for_page_ready(driver, session_id, extra_wait=1.5)
            except TimeoutException:
                _log(session_id, "No <body> found. Aborting.")
                _set_status(session_id, "ERROR")
                return

            # Collect ONLY article links - filter out category/nav/listing pages
            _log(session_id, "Filtering article links from page...")

            queue.extend(_extract_candidate_links_from_page(driver, session_id, start_url))
            visited.add(_canonicalize_url(start_url))
            _log(session_id, f"✓ Found {len(queue)} article links to scrape")

        else:
            queue.append(start_url)

        # ── Process each article ──────────────────────────────────────────────
        link_counter = 0
        total = len(queue)

        while queue and not stop_event.is_set():
            url = queue.pop(0)
            clean_url = _canonicalize_url(url)
            if clean_url in visited:
                continue
            visited.add(clean_url)
            link_counter += 1

            _log(session_id, f"[{link_counter}/{total}] Opening: {url[:80]}")
            _set_status(session_id, "RUNNING")

            try:
                driver.get(url)
            except TimeoutException:
                _log(session_id, "  ⚠ Timeout - extracting what loaded...")
            except WebDriverException as e:
                error_msg = str(e)[:100]
                _log(session_id, f"  ✗ Browser error: {error_msg}. Skipping.")
                continue
            except Exception as e:
                error_msg = str(e)[:100]
                _log(session_id, f"  ✗ Connection error: {error_msg}. Skipping.")
                continue

            try:
                _wait_for_page_ready(driver, session_id, extra_wait=2.0)
            except TimeoutException:
                _log(session_id, "  ✗ Page body not found. Skipping.")
                continue

            # Check if page has actual text content or embedded article data
            try:
                has_content, body_len = _page_has_extractable_content(driver)
                if not has_content:
                    try:
                        driver.refresh()
                        _wait_for_page_ready(driver, session_id, extra_wait=1.5)
                        has_content, body_len = _page_has_extractable_content(driver)
                    except Exception:
                        pass
                if not has_content:
                    _log(session_id, f"  ✗ Page empty/minimal content. Skipping.")
                    continue
                body_text = _safe_body_text(driver)
            except:
                _log(session_id, "  ✗ Could not read page content. Skipping.")
                continue

            _log(session_id, f"  ✓ Loaded: {driver.title[:70]} ({body_len} visible chars)")

            # Check DataTables on first article only
            if link_counter == 1:
                is_datatable = _has_datatable(driver)

            page_num = 1
            while not stop_event.is_set():
                time.sleep(1)

                rows = _extract_generic_content(driver, session_id)
                if not rows:
                    _log(session_id, "  ✗ No extractable rows found. Skipping save for this page.")
                    break

                # Save article separator
                sep_doc = {
                    "session_id": session_id,
                    "content_type": normalize_text("━━━ ARTICLE START ━━━"),
                    "extracted_data": normalize_text(clean_url or url),
                    "extra_info": normalize_text(f"Article {link_counter} of {total}")
                }

                docs = [sep_doc]
                for row in rows:
                    docs.append({
                        "session_id": session_id,
                        "content_type": normalize_text(row[0]),
                        "extracted_data": normalize_text(row[1]),
                        "extra_info": normalize_text(row[2])
                    })
                    records += 1

                if docs:
                    try:
                        from db import is_db_connected
                        if is_db_connected():
                            generic_collection.insert_many(docs)
                        else:
                            _save_to_csv_fallback(session_id, docs)
                    except Exception as db_err:
                        _log(session_id, f"  DB error: {db_err}. Using CSV fallback.")
                        _save_to_csv_fallback(session_id, docs)

                with _sessions_lock:
                    generic_sessions[session_id]["records"] = records

                _log(session_id, f"  ✓ Saved {len(rows)} items (total: {records})")

                if is_datatable and _datatable_next_enabled(driver):
                    _click_datatable_next(driver)
                    page_num += 1
                else:
                    break

            time.sleep(0.5)

        _log(session_id, f"✓ DONE: {link_counter} articles, {records} records saved.")
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

    session = _make_session(session_id, url, mode)
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

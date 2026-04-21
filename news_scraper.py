"""
news_scraper.py — Multi-source news article scraper.

Scrapes news articles from various news channels and stores them in MongoDB.
Uses threading for parallel scraping and handles multiple news sources.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import threading
import time
from typing import Dict, List, Optional
from article_schema import ArticleSchema
from db import news_articles_collection
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# News sources configuration - STRICT SCHEMA ONLY
NEWS_SOURCES = {
    "bbc": {
        "url": "https://www.bbc.com/news",
        "source_name": "BBC News",
        "topic": "general"
    },
    "cnn": {
        "url": "https://www.cnn.com",
        "source_name": "CNN",
        "topic": "general"
    },
    "techcrunch": {
        "url": "https://techcrunch.com",
        "source_name": "TechCrunch",
        "topic": "technology"
    },
    "google_news": {
        "url": "https://news.google.com",
        "source_name": "Google News",
        "topic": "general"
    },
}

# Global state for news scraper
news_scraper_state = {
    "is_running": False,
    "status": "IDLE",
    "current_source": "",
    "total_articles_scraped": 0,
    "total_articles_saved": 0,
    "last_error": "",
    "started_at": None,
    "articles_by_source": {},
}

news_scraper_lock = threading.Lock()
stop_news_event = threading.Event()


def extract_image_url(link, base_url: str) -> Optional[str]:
    """
    Extract image URL from a news link with comprehensive search strategy.
    Searches for images in multiple ways and locations to ensure every article gets an image.
    
    Args:
        link: BeautifulSoup link element
        base_url: Base URL for converting relative URLs
    
    Returns:
        Image URL if found, otherwise None
    """
    image_url = None
    
    # Strategy 1: Check for img tag directly in the link
    img = link.find('img')
    if img:
        image_url = img.get('src') or img.get('data-src') or img.get('srcset')
        if image_url:
            # Handle srcset (comma-separated, take first)
            if ',' in image_url:
                image_url = image_url.split(',')[0].strip().split()[0]
    
    # Strategy 2: Check for data attributes with image info
    if not image_url:
        image_url = (link.get('data-image') or 
                    link.get('data-src') or 
                    link.get('data-thumbnail') or
                    link.get('data-image-url'))
    
    # Strategy 3: Check parent elements (up to 6 levels)
    if not image_url:
        parent = link.parent
        for level in range(6):
            if parent:
                # Check for picture tag first
                picture = parent.find('picture', recursive=False)
                if picture:
                    source = picture.find('source')
                    if source:
                        image_url = source.get('srcset')
                        if image_url:
                            if ',' in image_url:
                                image_url = image_url.split(',')[0].strip().split()[0]
                            break
                    else:
                        img = picture.find('img')
                        if img:
                            image_url = img.get('src') or img.get('srcset')
                            if image_url:
                                if ',' in image_url:
                                    image_url = image_url.split(',')[0].strip().split()[0]
                                break
                
                # Check for img tags in parent
                if not image_url:
                    img = parent.find('img', recursive=False)  # Direct children only
                    if img:
                        image_url = img.get('src') or img.get('data-src') or img.get('srcset')
                        if image_url:
                            if ',' in image_url:
                                image_url = image_url.split(',')[0].strip().split()[0]
                            break
                
                # Check for background-image style
                if not image_url:
                    style = parent.get('style', '')
                    if 'background-image' in style.lower():
                        import re
                        match = re.search(r'url\([\'"]?([^\)]+)[\'"]?\)', style)
                        if match:
                            image_url = match.group(1)
                            break
                
                # Check data attributes
                if not image_url:
                    image_url = (parent.get('data-image') or 
                                parent.get('data-src') or 
                                parent.get('data-thumbnail') or
                                parent.get('data-image-url'))
                    if image_url:
                        break
                
                parent = parent.parent
    
    # Strategy 4: Check siblings for images
    if not image_url and link.parent:
        # Check next siblings
        sibling = link.next_sibling
        attempts = 0
        while sibling and not image_url and attempts < 5:
            if hasattr(sibling, 'find'):
                img = sibling.find('img')
                if img:
                    image_url = img.get('src') or img.get('data-src') or img.get('srcset')
                    if image_url and ',' in image_url:
                        image_url = image_url.split(',')[0].strip().split()[0]
                    break
            sibling = sibling.next_sibling
            attempts += 1
    
    # Convert relative URLs to absolute
    if image_url:
        image_url = image_url.strip()
        
        # Remove quotes if present
        image_url = image_url.strip('\'"')
        
        # Handle relative URLs
        if not image_url.startswith('http'):
            if image_url.startswith('/'):
                from urllib.parse import urljoin
                image_url = urljoin(base_url, image_url)
            elif image_url.startswith('..'):
                from urllib.parse import urljoin
                image_url = urljoin(base_url, image_url)
            else:
                from urllib.parse import urljoin
                image_url = urljoin(base_url, image_url)
    
    return image_url


def remove_related_content_from_text(text: str) -> str:
    """
    Remove related links/articles references from article text content.
    Strips out text mentioning "related", "see also", "more articles", etc.
    
    Args:
        text: Article content text
    
    Returns:
        Cleaned text without related content references
    """
    if not text:
        return text
    
    # Split by newlines and filter out lines containing related keywords
    related_keywords = [
        'related', 'more articles', 'see also', 'similar', 
        'further reading', 'also read', 'trending', 'suggested',
        'recommended', 'more from', 'also from', 'you may like'
    ]
    
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line_lower = line.lower().strip()
        # Skip lines that are section headers for related content
        is_related_header = any(keyword in line_lower for keyword in related_keywords)
        
        if not is_related_header or len(line.strip()) > 50:  # Keep actual content
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()


def scrape_news_from_source(source_key: str, source_config: Dict) -> List[Dict]:
    """
    Scrape news articles from a specific news source.
    Extracts ALL links on the page and processes them according to STRICT SCHEMA.
    
    Args:
        source_key: Key identifying the news source
        source_config: Configuration for the news source
    
    Returns:
        List of article dictionaries matching EXACT schema only
    """
    articles = []
    
    try:
        logger.info(f"Scraping {source_config['source_name']}...")
        
        # Set headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(source_config["url"], headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find ALL links on the page
        all_links = soup.find_all('a', href=True)
        logger.info(f"Found {len(all_links)} links on {source_config['source_name']}")
        
        processed_urls = set()  # Track processed URLs to avoid duplicates on same page
        
        for link in all_links:
            if stop_news_event.is_set():
                break
            
            try:
                # SKIP RELATED LINKS SECTIONS
                # Check if link is within a "related" container
                parent = link.parent
                skip_related = False
                
                # Check up to 5 levels up for related/trending/suggested containers
                for _ in range(5):
                    if parent:
                        # Check class attribute
                        classes = parent.get('class', [])
                        if isinstance(classes, list):
                            class_str = ' '.join(classes).lower()
                        else:
                            class_str = str(classes).lower()
                        
                        # Check id attribute
                        element_id = parent.get('id', '').lower()
                        
                        # Check for related/trending/suggested keywords
                        related_keywords = [
                            # Related links
                            'related', 'trending', 'suggested', 'recommended',
                            'similar', 'more-from', 'also-read', 'sidebar',
                            'more-stories', 'related-stories', 'you-may-like',
                            'more-articles', 'see-also', 'further-reading',
                            # Advertisements and sponsored
                            'advertisement', 'ad-', 'ads', 'sponsored',
                            'sponsor', 'promotional', 'promo', 'promotion',
                            'advertise', 'native-ad', 'sponsored-content',
                            'paid-content', 'partner-content', 'branded-content',
                            'ad-space', 'ad-section', 'advertisement-section',
                            'commercial', 'marketing', 'promotion-section',
                            # Google News specific - STRICT FILTERING
                            'google-news-feed', 'feed-line', 'news-feed-line',
                            'google-news-cluster', 'news-cluster', 'cluster',
                            'google-news-story', 'news-story',
                            'story-meta', 'news-story-meta'
                        ]
                        
                        for keyword in related_keywords:
                            if keyword in class_str or keyword in element_id:
                                skip_related = True
                                break
                        
                        if skip_related:
                            break
                        
                        parent = parent.parent
                    else:
                        break
                
                # Skip this link if it's in a related section
                if skip_related:
                    logger.debug(f"Skipping link in related section: {link.get_text(strip=True)[:50]}...")
                    continue
                
                # Extract URL
                url = link.get('href', '').strip()
                
                if not url or url in processed_urls:
                    continue
                
                # Skip empty, fragment-only, or javascript links
                if not url or url.startswith('#') or url.startswith('javascript:'):
                    continue
                
                # Make absolute URL if relative
                if url.startswith('/'):
                    from urllib.parse import urljoin
                    url = urljoin(source_config["url"], url)
                elif not url.startswith('http'):
                    from urllib.parse import urljoin
                    url = urljoin(source_config["url"], url)
                
                # Skip non-http URLs
                if not url.startswith('http'):
                    continue
                
                processed_urls.add(url)
                
                # Extract title - REQUIRED FIELD
                title = link.get_text(strip=True)
                
                # If link text is empty, try parent elements
                if not title or len(title) < 3:
                    parent = link.parent
                    for _ in range(3):  # Check up to 3 levels up
                        if parent:
                            for tag in ['h1', 'h2', 'h3', 'span', 'div']:
                                heading = parent.find(tag)
                                if heading:
                                    title = heading.get_text(strip=True)
                                    if title and len(title) > 3:
                                        break
                            if title and len(title) > 3:
                                break
                            parent = parent.parent
                
                # Clean title
                title = title.strip() if title else None
                
                # Skip if no valid title
                if not title or len(title) < 3:
                    continue
                
                # Extract image URL using comprehensive search strategy
                image_url = extract_image_url(link, source_config["url"])
                
                # Create article with STRICT SCHEMA - only these fields
                article = ArticleSchema.create_article(
                    title=title,
                    url=url,
                    topic=source_config["topic"],
                    description=None,
                    content=None,
                    urlToImage=image_url,
                    source=source_config["source_name"],
                    author=None,
                    publishedAt=None,
                    fetchedAt=None  # Will be auto-generated
                )
                
                # Clean any related content from article if content exists
                if article.get("content"):
                    article["content"] = remove_related_content_from_text(article["content"])
                if article.get("description"):
                    article["description"] = remove_related_content_from_text(article["description"])
                
                articles.append(article)
                
                # Limit to prevent excessive scraping
                if len(articles) >= 100:
                    break
            
            except Exception as e:
                logger.error(f"Error processing link from {source_config['source_name']}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(articles)} articles from {source_config['source_name']}")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error scraping {source_config['source_name']}: {e}")
        with news_scraper_lock:
            news_scraper_state["last_error"] = f"Error scraping {source_config['source_name']}: {str(e)}"
    
    except Exception as e:
        logger.error(f"Unexpected error scraping {source_config['source_name']}: {e}")
        with news_scraper_lock:
            news_scraper_state["last_error"] = f"Unexpected error: {str(e)}"
    
    return articles


def save_articles_to_db(articles: List[Dict], source_name: str) -> int:
    """
    Save articles to MongoDB, avoiding duplicates based on URL.
    Only saves articles that match the STRICT SCHEMA - removes any extra fields.
    
    Args:
        articles: List of article dictionaries to save
        source_name: Name of the source for tracking
    
    Returns:
        Number of articles successfully saved
    """
    saved_count = 0
    
    try:
        for article in articles:
            # Validate article matches schema
            if not ArticleSchema.validate_article(article):
                logger.warning(f"Skipping invalid article: missing required fields")
                continue
            
            # Clean article - ONLY keep the 10 schema fields
            cleaned_article = ArticleSchema.clean_article(article)
            
            # Check if article with same URL already exists
            existing = news_articles_collection.find_one({"url": cleaned_article["url"]})
            
            if existing:
                logger.debug(f"Article already exists: {cleaned_article['title'][:50]}...")
            else:
                # Insert new article - STRICTLY ONLY the 10 schema fields
                result = news_articles_collection.insert_one(cleaned_article)
                saved_count += 1
                logger.info(f"Saved article: {cleaned_article['title'][:50]}...")
        
        with news_scraper_lock:
            news_scraper_state["total_articles_saved"] += saved_count
            if source_name not in news_scraper_state["articles_by_source"]:
                news_scraper_state["articles_by_source"][source_name] = 0
            news_scraper_state["articles_by_source"][source_name] += saved_count
    
    except Exception as e:
        logger.error(f"Error saving articles to database: {e}")
        with news_scraper_lock:
            news_scraper_state["last_error"] = f"Database error: {str(e)}"
    
    return saved_count


def run_news_scraper_thread(sources: Optional[List[str]] = None):
    """
    Run the news scraper in a separate thread.
    
    Args:
        sources: List of source keys to scrape. If None, scrapes all sources.
    """
    
    with news_scraper_lock:
        news_scraper_state["is_running"] = True
        news_scraper_state["status"] = "SCRAPING"
        news_scraper_state["started_at"] = datetime.now().isoformat()
        news_scraper_state["total_articles_scraped"] = 0
        news_scraper_state["total_articles_saved"] = 0
        news_scraper_state["articles_by_source"] = {}
        news_scraper_state["last_error"] = ""
    
    try:
        # Determine which sources to scrape
        sources_to_scrape = sources if sources else list(NEWS_SOURCES.keys())
        
        for source_key in sources_to_scrape:
            # Check if stop was requested
            if stop_news_event.is_set():
                logger.info("Stop event detected. Halting news scraper.")
                break
            
            if source_key not in NEWS_SOURCES:
                logger.warning(f"Unknown news source: {source_key}")
                continue
            
            with news_scraper_lock:
                news_scraper_state["current_source"] = source_key
            
            source_config = NEWS_SOURCES[source_key]
            
            # Scrape articles
            articles = scrape_news_from_source(source_key, source_config)
            
            with news_scraper_lock:
                news_scraper_state["total_articles_scraped"] += len(articles)
            
            # Save to database
            if articles:
                saved = save_articles_to_db(articles, source_config["source_name"])
                logger.info(f"Saved {saved}/{len(articles)} articles from {source_config['source_name']}")
            
            # Rate limiting - be respectful to news sites
            time.sleep(2)
        
        with news_scraper_lock:
            news_scraper_state["status"] = "COMPLETED"
            news_scraper_state["is_running"] = False
            logger.info(f"News scraping completed. Total articles scraped: {news_scraper_state['total_articles_scraped']}, saved: {news_scraper_state['total_articles_saved']}")
    
    except Exception as e:
        logger.error(f"Critical error in news scraper thread: {e}")
        with news_scraper_lock:
            news_scraper_state["status"] = "ERROR"
            news_scraper_state["is_running"] = False
            news_scraper_state["last_error"] = f"Critical error: {str(e)}"


def start_news_scraper(sources: Optional[List[str]] = None):
    """
    Start the news scraper in a background thread.
    
    Args:
        sources: List of source keys to scrape. If None, scrapes all sources.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    
    with news_scraper_lock:
        if news_scraper_state["is_running"]:
            return False, "News scraper is already running"
    
    stop_news_event.clear()
    scraper_thread = threading.Thread(
        target=run_news_scraper_thread,
        args=(sources,),
        daemon=True
    )
    scraper_thread.start()
    
    return True, "News scraper started"


def stop_news_scraper():
    """
    Request the news scraper to stop gracefully.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    
    with news_scraper_lock:
        if not news_scraper_state["is_running"]:
            return False, "News scraper is not running"
        
        news_scraper_state["status"] = "STOPPING"
    
    stop_news_event.set()
    return True, "Stop signal sent to news scraper"


def get_news_scraper_status() -> Dict:
    """
    Get the current status of the news scraper.
    
    Returns:
        Dictionary containing scraper status and statistics
    """
    
    with news_scraper_lock:
        return dict(news_scraper_state)


def get_all_articles(filters: Optional[Dict] = None, limit: Optional[int] = 100) -> List[Dict]:
    """
    Retrieve articles from database.
    Returns ONLY articles with the 10 strict schema fields - NO extras.
    
    Args:
        filters: MongoDB query filters (optional)
        limit: Maximum number of articles to return (None for no limit)
    
    Returns:
        List of article documents with ONLY the 10 schema fields
    """
    
    try:
        query = filters or {}
        query_cursor = news_articles_collection.find(query).sort("fetchedAt", -1)
        
        if limit:
            query_cursor = query_cursor.limit(limit)
        
        articles = list(query_cursor)
        
        # Clean articles - ONLY keep the 10 schema fields
        cleaned_articles = []
        ALLOWED_FIELDS = {
            "title", "description", "content", "url", "urlToImage",
            "source", "author", "topic", "publishedAt", "fetchedAt"
        }
        
        for article in articles:
            # Remove MongoDB _id field
            if "_id" in article:
                del article["_id"]
            
            # Keep only the 10 allowed fields
            cleaned = {k: v for k, v in article.items() if k in ALLOWED_FIELDS}
            
            # Ensure required fields exist
            if "title" in cleaned and "url" in cleaned and "topic" in cleaned:
                cleaned_articles.append(cleaned)
        
        return cleaned_articles
    
    except Exception as e:
        logger.error(f"Error retrieving articles: {e}")
        return []


def get_articles_by_topic(topic: str, limit: int = 50) -> List[Dict]:
    """
    Get articles filtered by topic.
    
    Args:
        topic: Topic to filter by
        limit: Maximum number of articles to return
    
    Returns:
        List of article documents
    """
    
    return get_all_articles({"topic": topic}, limit)


def get_articles_by_source(source: str, limit: int = 50) -> List[Dict]:
    """
    Get articles filtered by source.
    
    Args:
        source: Source name to filter by
        limit: Maximum number of articles to return
    
    Returns:
        List of article documents
    """
    
    return get_all_articles({"source": source}, limit)


def search_articles(search_term: str, limit: int = 50) -> List[Dict]:
    """
    Search articles by title or content.
    
    Args:
        search_term: Search term to look for
        limit: Maximum number of articles to return
    
    Returns:
        List of matching article documents
    """
    
    try:
        query = {
            "$or": [
                {"title": {"$regex": search_term, "$options": "i"}},
                {"description": {"$regex": search_term, "$options": "i"}},
                {"content": {"$regex": search_term, "$options": "i"}},
            ]
        }
        return get_all_articles(query, limit)
    
    except Exception as e:
        logger.error(f"Error searching articles: {e}")
        return []


def get_article_count(filters: Optional[Dict] = None) -> int:
    """
    Get total count of articles, optionally filtered.
    
    Args:
        filters: MongoDB query filters (optional)
    
    Returns:
        Number of articles matching the filters
    """
    
    try:
        query = filters or {}
        return news_articles_collection.count_documents(query)
    
    except Exception as e:
        logger.error(f"Error counting articles: {e}")
        return 0


def get_topics() -> List[str]:
    """
    Get all unique topics in the database.
    
    Returns:
        List of unique topic strings
    """
    
    try:
        topics = news_articles_collection.distinct("topic")
        return sorted(topics)
    
    except Exception as e:
        logger.error(f"Error getting topics: {e}")
        return []


def get_sources() -> List[str]:
    """
    Get all unique sources in the database.
    
    Returns:
        List of unique source strings
    """
    
    try:
        sources = news_articles_collection.distinct("source")
        return sorted(sources)
    
    except Exception as e:
        logger.error(f"Error getting sources: {e}")
        return []

from flask import Flask, render_template, request, jsonify, send_file
import threading
import os

# ── ISBN scraper ───────────────────────────────────────────────────────────────
from isbn_scraper import (
    run_scraper_thread,
    scraper_state,
    stop_event,
    load_checkpoint,
    log_event,
)
from db import isbn_collection, generic_collection, news_articles_collection
from text_utils import normalize_text

# ── Generic scraper ────────────────────────────────────────────────────────────
from generic_scraper import (
    start_generic_session,
    stop_generic_session,
    remove_generic_session,
    flush_all_generics,
    get_sessions_snapshot,
    generic_sessions,
    _normalize_generic_mode,
)

# ── News scraper ──────────────────────────────────────────────────────────────
from news_scraper import (
    start_news_scraper,
    stop_news_scraper,
    get_news_scraper_status,
    get_all_articles,
    get_articles_by_topic,
    get_articles_by_source,
    search_articles,
    get_article_count,
    get_topics,
    get_sources,
)

app = Flask(__name__)

# Global variable to hold the ISBN scraper thread
scraper_thread = None

# ══════════════════════════════════════════════════════════════════════════════
#  ISBN Scraper routes (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/start', methods=['POST'])
def start_scraper():
    global scraper_thread

    if scraper_state["is_running"]:
        return jsonify({"status": "error", "message": "Scraper is already running"}), 400

    data = request.json or {}
    start_page = data.get('start_page')
    if start_page in (None, ""):
        start_page = load_checkpoint().get("next_page", 1)

    try:
        start_page = int(start_page)
        if start_page < 1:
            start_page = 1
    except ValueError:
        start_page = 1

    stop_event.clear()
    scraper_state["is_running"] = True
    scraper_state["status"] = "INITIALIZING"
    scraper_state["current_page"] = start_page
    scraper_state["last_error"] = ""
    log_event(f"Start requested from UI. Starting at page {start_page}.")

    scraper_thread = threading.Thread(target=run_scraper_thread, args=(start_page,))
    scraper_thread.daemon = True
    scraper_thread.start()

    return jsonify({"status": "success", "message": f"Scraper started at page {start_page}"})


@app.route('/api/stop', methods=['POST'])
def stop_scraper_api():
    if not scraper_state["is_running"]:
        return jsonify({"status": "error", "message": "Scraper is not running"}), 400

    scraper_state["status"] = "STOPPING"
    stop_event.set()
    log_event("Stop requested from UI.")
    return jsonify({"status": "success", "message": "Stop signal sent. Scraper will stop after finishing the current page."})


@app.route('/api/status', methods=['GET'])
def get_status():
    checkpoint = load_checkpoint()
    payload = dict(scraper_state)
    payload["checkpoint_next_page"] = checkpoint.get("next_page", 1)
    payload["checkpoint_total_records"] = checkpoint.get("total_records", 0)
    
    total_docs = isbn_collection.count_documents({})
    payload["csv_exists"] = total_docs > 0
    payload["csv_size_mb"] = round(total_docs * 0.0005, 2) # rough mock estimate
    return jsonify(payload)


@app.route('/api/download', methods=['GET'])
def download_data():
    total = isbn_collection.count_documents({})
    if total == 0:
        return jsonify({"status": "error", "message": "No data available yet"}), 404
        
    def generate():
        import csv
        from io import StringIO
        output = StringIO()
        writer = csv.writer(output)
        columns = ["#", "Book Title", "ISBN", "Product Form", "Language", "Applicant Type", "Name of Publishing Agency/Publisher", "Imprint", "Name of Author/Editor", "Publication Date"]
        writer.writerow(columns)
        yield "\ufeff" + output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        for doc in isbn_collection.find():
            writer.writerow([normalize_text(doc.get(col, "")) for col in columns])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
            
    from flask import Response
    return Response(
        generate(), 
        content_type='text/csv; charset=utf-8',
        headers={"Content-Disposition": "attachment; filename=isbn_full_data.csv"}
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Generic multi-site scraper routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/generic/add', methods=['POST'])
def generic_add():
    data = request.json or {}
    url = (data.get('url') or '').strip()
    mode = _normalize_generic_mode(data.get('mode', 'single'))
    if not url:
        return jsonify({"status": "error", "message": "URL is required."}), 400
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    session_id, err = start_generic_session(url, mode)
    if err:
        return jsonify({"status": "error", "message": err}), 429

    return jsonify({"status": "success", "session_id": session_id, "message": f"Scraper started for {url}"})


@app.route('/api/generic/sessions', methods=['GET'])
def generic_list():
    return jsonify(get_sessions_snapshot())


@app.route('/api/generic/<session_id>/stop', methods=['POST'])
def generic_stop(session_id):
    err = stop_generic_session(session_id)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify({"status": "success", "message": "Stop signal sent."})


@app.route('/api/generic/<session_id>/remove', methods=['DELETE'])
def generic_remove(session_id):
    err = remove_generic_session(session_id)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    return jsonify({"status": "success", "message": "Session removed."})

@app.route('/api/generic/flush', methods=['DELETE'])
def generic_flush():
    err = flush_all_generics()
    if err:
        return jsonify({"status": "error", "message": err}), 500
    return jsonify({"status": "success", "message": "All generic data flushed."})


@app.route('/api/generic/<session_id>/download', methods=['GET'])
def generic_download(session_id):
    sess = generic_sessions.get(session_id)
    if not sess:
        return jsonify({"status": "error", "message": "Session not found."}), 404
        
    count = generic_collection.count_documents({"session_id": session_id})
    if count == 0:
        return jsonify({"status": "error", "message": "No data available yet."}), 404

    domain = sess["url"].replace("https://", "").replace("http://", "").split("/")[0]

    def generate_generic():
        import csv
        from io import StringIO
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Content Type", "Extracted Data", "Extra Info / Link"])
        yield "\ufeff" + output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        for doc in generic_collection.find({"session_id": session_id}):
            writer.writerow([
                normalize_text(doc.get("content_type", "")),
                normalize_text(doc.get("extracted_data", "")),
                normalize_text(doc.get("extra_info", "")),
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    from flask import Response
    return Response(
        generate_generic(), 
        content_type='text/csv; charset=utf-8',
        headers={"Content-Disposition": f"attachment; filename=scraped_{domain}_{session_id}.csv"}
    )


# ══════════════════════════════════════════════════════════════════════════════
#  News Scraper routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/news/start', methods=['POST'])
def start_news_scraping():
    """Start the news scraper"""
    data = request.json or {}
    sources = data.get('sources')  # Optional: list of specific sources to scrape
    
    success, message = start_news_scraper(sources)
    if success:
        return jsonify({"status": "success", "message": message})
    else:
        return jsonify({"status": "error", "message": message}), 400


@app.route('/api/news/stop', methods=['POST'])
def stop_news_scraping():
    """Stop the news scraper"""
    success, message = stop_news_scraper()
    if success:
        return jsonify({"status": "success", "message": message})
    else:
        return jsonify({"status": "error", "message": message}), 400


@app.route('/api/news/status', methods=['GET'])
def get_news_status():
    """Get the current status of the news scraper"""
    status = get_news_scraper_status()
    total_count = get_article_count()
    status['total_articles_in_db'] = total_count
    return jsonify(status)


@app.route('/api/news/articles', methods=['GET'])
def list_news_articles():
    """Get all news articles with optional pagination"""
    limit = request.args.get('limit', default=100, type=int)
    topic = request.args.get('topic')
    source = request.args.get('source')
    search = request.args.get('search')
    
    if search:
        articles = search_articles(search, limit)
    elif topic:
        articles = get_articles_by_topic(topic, limit)
    elif source:
        articles = get_articles_by_source(source, limit)
    else:
        articles = get_all_articles(limit=limit)
    
    return jsonify({
        "status": "success",
        "count": len(articles),
        "articles": articles
    })


@app.route('/api/news/articles/<article_id>', methods=['GET'])
def get_news_article(article_id):
    """Get a specific article by ID - STRICT SCHEMA ONLY"""
    try:
        from bson.objectid import ObjectId
        article = news_articles_collection.find_one({"_id": ObjectId(article_id)})
        if article:
            # Remove _id and keep only the 10 schema fields
            if "_id" in article:
                del article["_id"]
            
            ALLOWED_FIELDS = {
                "title", "description", "content", "url", "urlToImage",
                "source", "author", "topic", "publishedAt", "fetchedAt"
            }
            cleaned = {k: v for k, v in article.items() if k in ALLOWED_FIELDS}
            
            if "title" in cleaned and "url" in cleaned and "topic" in cleaned:
                return jsonify({"status": "success", "article": cleaned})
            else:
                return jsonify({"status": "error", "message": "Invalid article format"}), 400
        else:
            return jsonify({"status": "error", "message": "Article not found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": f"Invalid article ID: {str(e)}"}), 400


@app.route('/api/news/topics', methods=['GET'])
def get_news_topics():
    """Get all unique topics"""
    topics = get_topics()
    return jsonify({
        "status": "success",
        "topics": topics,
        "count": len(topics)
    })


@app.route('/api/news/sources', methods=['GET'])
def get_news_sources():
    """Get all unique sources"""
    sources = get_sources()
    return jsonify({
        "status": "success",
        "sources": sources,
        "count": len(sources)
    })


@app.route('/api/news/count', methods=['GET'])
def get_news_count():
    """Get article count with optional filters"""
    topic = request.args.get('topic')
    source = request.args.get('source')
    
    filters = {}
    if topic:
        filters['topic'] = topic
    if source:
        filters['source'] = source
    
    count = get_article_count(filters)
    return jsonify({
        "status": "success",
        "count": count,
        "filters": filters
    })


@app.route('/api/news/download', methods=['GET'])
def download_news_articles():
    """Download all articles as CSV"""
    total = get_article_count()
    if total == 0:
        return jsonify({"status": "error", "message": "No articles available yet"}), 404
    
    def generate():
        import csv
        from io import StringIO
        output = StringIO()
        writer = csv.writer(output)
        
        # Header with all fields
        headers = ["Title", "URL", "Topic", "Source", "Description", "Author", "Published At", "Fetched At", "Image URL"]
        writer.writerow(headers)
        yield "\ufeff" + output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        # Write articles
        for article in get_all_articles(limit=10000):  # Large limit for download
            writer.writerow([
                article.get("title", ""),
                article.get("url", ""),
                article.get("topic", ""),
                article.get("source", ""),
                article.get("description", ""),
                article.get("author", ""),
                article.get("publishedAt", ""),
                article.get("fetchedAt", ""),
                article.get("urlToImage", ""),
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
    
    from flask import Response
    return Response(
        generate(), 
        content_type='text/csv; charset=utf-8',
        headers={"Content-Disposition": "attachment; filename=news_articles.csv"}
    )


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    port = int(os.environ.get("PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=False)

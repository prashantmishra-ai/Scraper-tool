from flask import Flask, render_template, request, jsonify, send_file
import threading
import os

# ── ISBN scraper ───────────────────────────────────────────────────────────────
from isbn_scraper import (
    run_scraper_thread,
    scraper_state,
    stop_event,
    output_csv,
    load_checkpoint,
    log_event,
)

# ── Generic scraper ────────────────────────────────────────────────────────────
from generic_scraper import (
    start_generic_session,
    stop_generic_session,
    remove_generic_session,
    get_sessions_snapshot,
    generic_sessions,
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
    payload["csv_exists"] = os.path.exists(output_csv)
    payload["csv_size_mb"] = round(os.path.getsize(output_csv) / (1024 * 1024), 2) if os.path.exists(output_csv) else 0
    return jsonify(payload)


@app.route('/api/download', methods=['GET'])
def download_data():
    if not os.path.exists(output_csv):
        return jsonify({"status": "error", "message": "No data available yet"}), 404
    return send_file(
        output_csv,
        mimetype='text/csv',
        download_name='isbn_full_data.csv',
        as_attachment=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Generic multi-site scraper routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/generic/add', methods=['POST'])
def generic_add():
    data = request.json or {}
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({"status": "error", "message": "URL is required."}), 400
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    session_id, err = start_generic_session(url)
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


@app.route('/api/generic/<session_id>/download', methods=['GET'])
def generic_download(session_id):
    sess = generic_sessions.get(session_id)
    if not sess:
        return jsonify({"status": "error", "message": "Session not found."}), 404
    csv_path = sess.get("csv_path", "")
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"status": "error", "message": "No data available yet."}), 404

    domain = sess["url"].replace("https://", "").replace("http://", "").split("/")[0]
    return send_file(
        csv_path,
        mimetype='text/csv',
        download_name=f'scraped_{domain}_{session_id}.csv',
        as_attachment=True,
    )


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    port = int(os.environ.get("PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=False)

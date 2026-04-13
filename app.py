from flask import Flask, render_template, request, jsonify, send_file
import threading
import time
import os

# Import the updated scraper function and states
from isbn_scraper import (
    run_scraper_thread,
    scraper_state,
    stop_event,
    output_csv,
    load_checkpoint,
    log_event,
)

app = Flask(__name__)

# Global variable to hold the scraper thread
scraper_thread = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/start', methods=['POST'])
def start_scraper():
    global scraper_thread
    
    # Check if already running
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
        
    # Reset states
    stop_event.clear()
    scraper_state["is_running"] = True
    scraper_state["status"] = "INITIALIZING"
    scraper_state["current_page"] = start_page
    scraper_state["last_error"] = ""
    log_event(f"Start requested from UI. Starting at page {start_page}.")
    
    # Start thread
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
        as_attachment=True
    )

if __name__ == '__main__':
    # Run with env-configurable port for Coolify deployments
    port = int(os.environ.get("PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=False)

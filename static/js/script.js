// ── ISBN scraper state ────────────────────────────────────────────────────────
let pollInterval = null;
let clearedConsole = false;

// ── Generic sessions state ────────────────────────────────────────────────────
let genericPollInterval = null;
let knownSessions = {};   // { session_id: rendered }

// ══════════════════════════════════════════════════════════════════════════════
//  Startup
// ══════════════════════════════════════════════════════════════════════════════
document.addEventListener("DOMContentLoaded", () => {
    fetchStatus();
    fetchGenericSessions();
    pollInterval        = setInterval(fetchStatus,          2000);
    genericPollInterval = setInterval(fetchGenericSessions, 2000);

    // Allow pressing Enter in the URL field
    document.getElementById("scrapeUrl").addEventListener("keydown", (e) => {
        if (e.key === "Enter") addGenericScraper();
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  ISBN Scraper
// ══════════════════════════════════════════════════════════════════════════════
function setMessage(message, isError = false) {
    const el = document.getElementById("action-message");
    el.textContent = message;
    el.classList.remove("hidden");
    el.style.borderColor = isError ? "#7f1d1d" : "#2f4f80";
    el.style.background  = isError ? "#3f1212" : "#122643";
}

async function fetchStatus() {
    try {
        const response = await fetch("/api/status");
        const data = await response.json();
        updateUI(data);
    } catch {
        const h = document.getElementById("service-health");
        h.textContent = "API Unreachable";
        h.style.background = "#7f1d1d";
    }
}

function normalizeStatusClass(status) {
    return status.split(" ")[0].replace(/[()]/g, "");
}

function updateUI(data) {
    const statusEl = document.getElementById("run-status");
    const statusClass = normalizeStatusClass(data.status || "STOPPED");
    statusEl.textContent = data.status || "STOPPED";
    statusEl.className = `metric status-${statusClass}`;

    document.getElementById("current-page").textContent   = data.current_page ?? 1;
    document.getElementById("total-records").textContent  = (data.total_records ?? 0).toLocaleString();
    document.getElementById("resume-page").textContent    = data.checkpoint_next_page ?? 1;
    document.getElementById("csv-size").textContent       = `${data.csv_size_mb ?? 0} MB`;

    const health = document.getElementById("service-health");
    health.textContent  = data.is_running ? "Running" : "Idle";
    health.style.background = data.is_running ? "#14532d" : "#334155";

    const btnStart  = document.getElementById("btn-start");
    const btnStop   = document.getElementById("btn-stop");
    const inputPage = document.getElementById("startPage");

    if (data.is_running || data.status === "STOPPING") {
        btnStart.classList.add("hidden");
        btnStop.classList.remove("hidden");
        inputPage.disabled    = true;
        btnStop.disabled      = data.status === "STOPPING";
        btnStop.textContent   = data.status === "STOPPING" ? "Stopping…" : "■ Stop";
    } else {
        btnStart.classList.remove("hidden");
        btnStop.classList.add("hidden");
        inputPage.disabled = false;
        if (document.activeElement !== inputPage && !inputPage.value) {
            inputPage.placeholder = `Checkpoint page ${data.checkpoint_next_page ?? 1}`;
        }
    }

    const errorBox  = document.getElementById("error-box");
    const errorText = document.getElementById("last-error");
    if (data.last_error) {
        errorBox.classList.remove("hidden");
        errorText.textContent = data.last_error;
    } else {
        errorBox.classList.add("hidden");
    }

    const logs = Array.isArray(data.logs) ? data.logs : [];
    const consoleEl = document.getElementById("log-console");
    if (!clearedConsole || logs.length > 0) {
        consoleEl.textContent = logs.length ? logs.join("\n") : "No logs yet.";
        consoleEl.scrollTop = consoleEl.scrollHeight;
    }
}

async function startScraper() {
    const raw = document.getElementById("startPage").value.trim();
    const payload = {};
    if (raw !== "") payload.start_page = parseInt(raw, 10);
    try {
        const response = await fetch("/api/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        const result = await response.json();
        if (!response.ok) { setMessage(result.message || "Failed to start scraper.", true); return; }
        setMessage(result.message || "Scraper started.");
        fetchStatus();
    } catch { setMessage("Could not reach backend /api/start", true); }
}

async function stopScraper() {
    try {
        const response = await fetch("/api/stop", { method: "POST" });
        const result = await response.json();
        if (!response.ok) { setMessage(result.message || "Failed to stop scraper.", true); return; }
        setMessage(result.message || "Stop signal sent.");
        fetchStatus();
    } catch { setMessage("Could not reach backend /api/stop", true); }
}

function downloadCsv()   { window.location.href = "/api/download"; }
function clearConsole()  { clearedConsole = true; document.getElementById("log-console").textContent = "Console view cleared."; }

// ══════════════════════════════════════════════════════════════════════════════
//  Generic Multi-site Scraper
// ══════════════════════════════════════════════════════════════════════════════
function setGenericMessage(msg, isError = false) {
    const el = document.getElementById("generic-message");
    el.textContent = msg;
    el.classList.remove("hidden");
    el.style.borderColor = isError ? "#7f1d1d" : "#2f4f80";
    el.style.background  = isError ? "#3f1212" : "#122643";
    setTimeout(() => el.classList.add("hidden"), 5000);
}

async function addGenericScraper(mode) {
    const urlInput = document.getElementById("scrapeUrl");
    const url = urlInput.value.trim();
    if (!url) { setGenericMessage("Please enter a URL.", true); return; }

    const btn1 = document.getElementById("btn-g-single");
    const btn2 = document.getElementById("btn-g-deep");
    if(btn1) btn1.disabled = true;
    if(btn2) btn2.disabled = true;

    try {
        const res = await fetch("/api/generic/add", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url, mode }),
        });
        const data = await res.json();
        if (!res.ok) {
            setGenericMessage(data.message || "Failed to start scraper.", true);
        } else {
            setGenericMessage(`✓ Scraper started — session ${data.session_id}`);
            urlInput.value = "";
            fetchGenericSessions();
        }
    } catch {
        setGenericMessage("Could not reach /api/generic/add", true);
    } finally {
        if(btn1) btn1.disabled = false;
        if(btn2) btn2.disabled = false;
    }
}

async function fetchGenericSessions() {
    try {
        const res = await fetch("/api/generic/sessions");
        const sessions = await res.json();
        renderGenericCards(sessions);
    } catch { /* silent */ }
}

function renderGenericCards(sessions) {
    const grid = document.getElementById("generic-cards-grid");
    const hint = document.getElementById("no-sessions-hint");

    if (!sessions || sessions.length === 0) {
        grid.style.display = "none";
        hint.style.display = "block";
        return;
    }

    grid.style.display = "grid";
    hint.style.display = "none";

    // Build map for fast lookup
    const byId = {};
    sessions.forEach(s => byId[s.session_id] = s);

    // Remove cards for sessions that no longer exist
    Array.from(grid.querySelectorAll(".g-panel-wrap")).forEach(card => {
        if (!byId[card.dataset.id]) card.remove();
    });

    // Insert / update cards (newest first)
    const sorted = [...sessions].reverse();
    sorted.forEach((sess, idx) => {
        let card = grid.querySelector(`.g-panel-wrap[data-id="${sess.session_id}"]`);
        if (!card) {
            card = document.createElement("div");
            card.className = "g-panel-wrap";
            card.dataset.id = sess.session_id;
            grid.insertBefore(card, grid.firstChild);
        }
        card.innerHTML = buildCardHTML(sess);
    });
}

function buildCardHTML(sess) {
    const statusClass = normalizeStatusClass(sess.status || "STOPPED");
    const domain = (() => {
        try { return new URL(sess.url).hostname; } catch { return sess.url; }
    })();
    const shortUrl = sess.url.length > 55 ? sess.url.slice(0, 52) + "…" : sess.url;
    const logs = Array.isArray(sess.logs) ? sess.logs.slice(-8).join("\n") : "";
    const isRunning = sess.is_running;
    const hasCsv = sess.records > 0;

    return `
        <section class="panel" style="border: 1px solid #334871;">
            <div class="panel-head" style="margin-bottom: 20px; align-items: flex-start;">
                <div>
                    <h3 style="margin: 0; font-size: 1.25rem; color: #fff;">${domain}</h3>
                    <p style="margin: 4px 0 0; color: var(--muted); font-size: 0.85rem;" title="${sess.url}">${shortUrl}</p>
                </div>
                <div class="actions">
                    ${isRunning
                        ? `<button class="btn btn-danger btn-sm" onclick="stopGenericSession('${sess.session_id}')">■ Stop</button>`
                        : `<button class="btn btn-ghost btn-sm" onclick="removeGenericSession('${sess.session_id}')">🗑 Remove</button>`}
                    ${hasCsv
                        ? `<button class="btn btn-secondary btn-sm" onclick="downloadGenericCsv('${sess.session_id}')">⬇ Download CSV</button>`
                        : ""}
                </div>
            </div>

            <div class="cards" style="margin-bottom: 20px;">
                <div class="card">
                    <h3>Status</h3>
                    <p class="metric status-${statusClass}">${sess.status}</p>
                </div>
                <div class="card">
                    <h3>Rows Saved</h3>
                    <p class="metric">${(sess.records || 0).toLocaleString()}</p>
                </div>
                <div class="card">
                    <h3>Started At</h3>
                    <p class="metric">${sess.started_at || "—"}</p>
                </div>
                <div class="card">
                    <h3>Session ID</h3>
                    <p class="metric">${sess.session_id}</p>
                </div>
            </div>

            ${sess.error ? `<div class="message" style="margin-bottom: 16px; background: #3f1212; border-color: #7f1d1d; color: #fca5a5;">⚠ ${sess.error}</div>` : ""}

            <pre class="console" style="max-height: 160px; margin: 0;">${logs || "Waiting for logs…"}</pre>
        </section>
    `;
}

async function stopGenericSession(id) {
    try {
        const res = await fetch(`/api/generic/${id}/stop`, { method: "POST" });
        const data = await res.json();
        if (!res.ok) setGenericMessage(data.message || "Stop failed.", true);
        else { setGenericMessage("Stop signal sent."); fetchGenericSessions(); }
    } catch { setGenericMessage("Could not reach stop endpoint.", true); }
}

async function removeGenericSession(id) {
    if (!confirm("Remove this scraper and delete its data?")) return;
    try {
        const res = await fetch(`/api/generic/${id}/remove`, { method: "DELETE" });
        const data = await res.json();
        if (!res.ok) setGenericMessage(data.message || "Remove failed.", true);
        else { setGenericMessage("Scraper removed."); fetchGenericSessions(); }
    } catch { setGenericMessage("Could not reach remove endpoint.", true); }
}

function downloadGenericCsv(id) {
    window.location.href = `/api/generic/${id}/download`;
}

let pollInterval = null;
let clearedConsole = false;

document.addEventListener("DOMContentLoaded", () => {
    fetchStatus();
    pollInterval = setInterval(fetchStatus, 2000);
});

function setMessage(message, isError = false) {
    const el = document.getElementById("action-message");
    el.textContent = message;
    el.classList.remove("hidden");
    el.style.borderColor = isError ? "#7f1d1d" : "#2f4f80";
    el.style.background = isError ? "#3f1212" : "#122643";
}

async function fetchStatus() {
    try {
        const response = await fetch("/api/status");
        const data = await response.json();
        updateUI(data);
    } catch (error) {
        document.getElementById("service-health").textContent = "API Unreachable";
        document.getElementById("service-health").style.background = "#7f1d1d";
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

    document.getElementById("current-page").textContent = data.current_page ?? 1;
    document.getElementById("total-records").textContent = (data.total_records ?? 0).toLocaleString();
    document.getElementById("resume-page").textContent = data.checkpoint_next_page ?? 1;
    document.getElementById("csv-size").textContent = `${data.csv_size_mb ?? 0} MB`;

    const health = document.getElementById("service-health");
    health.textContent = data.is_running ? "Running" : "Idle";
    health.style.background = data.is_running ? "#14532d" : "#334155";

    const btnStart = document.getElementById("btn-start");
    const btnStop = document.getElementById("btn-stop");
    const inputPage = document.getElementById("startPage");

    if (data.is_running || data.status === "STOPPING") {
        btnStart.classList.add("hidden");
        btnStop.classList.remove("hidden");
        inputPage.disabled = true;
        btnStop.disabled = data.status === "STOPPING";
        btnStop.textContent = data.status === "STOPPING" ? "Stopping..." : "Stop";
    } else {
        btnStart.classList.remove("hidden");
        btnStop.classList.add("hidden");
        inputPage.disabled = false;
        if (document.activeElement !== inputPage && !inputPage.value) {
            inputPage.placeholder = `Checkpoint page ${data.checkpoint_next_page ?? 1}`;
        }
    }

    const errorBox = document.getElementById("error-box");
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
            body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) {
            setMessage(result.message || "Failed to start scraper.", true);
            return;
        }
        setMessage(result.message || "Scraper started.");
        fetchStatus();
    } catch (error) {
        setMessage("Could not reach backend /api/start", true);
    }
}

async function stopScraper() {
    try {
        const response = await fetch("/api/stop", { method: "POST" });
        const result = await response.json();
        if (!response.ok) {
            setMessage(result.message || "Failed to stop scraper.", true);
            return;
        }
        setMessage(result.message || "Stop signal sent.");
        fetchStatus();
    } catch (error) {
        setMessage("Could not reach backend /api/stop", true);
    }
}

function downloadCsv() {
    window.location.href = "/api/download";
}

function clearConsole() {
    clearedConsole = true;
    document.getElementById("log-console").textContent = "Console view cleared.";
}

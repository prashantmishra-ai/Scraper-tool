FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# ── System dependencies ──────────────────────────────────────────────────────
# All libraries required by Firefox ESR in headless / container mode.
# NOTE: No inline comments inside the apt-get install block — they break the build.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    firefox-esr \
    wget \
    ca-certificates \
    libx11-xcb1 \
    libxt6 \
    libxrender1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgtk-3-0 \
    libgdk-pixbuf2.0-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libcairo-gobject2 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libasound2 \
    libdbus-glib-1-2 \
    libgbm1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# ── geckodriver ───────────────────────────────────────────────────────────────
# v0.35.0 works with Firefox ESR 115+
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz \
    && tar -xzf geckodriver-v0.35.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/geckodriver \
    && rm geckodriver-v0.35.0-linux64.tar.gz \
    && chmod +x /usr/local/bin/geckodriver

# ── App ───────────────────────────────────────────────────────────────────────
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Force Firefox into headless mode — no display server exists in a container.
ENV HEADLESS=1
ENV MOZ_HEADLESS=1

EXPOSE 5000

CMD ["python3", "app.py"]
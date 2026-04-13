FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install Firefox ESR — apt will automatically pull all required libs as dependencies.
# We only explicitly add wget (for geckodriver download), ca-certificates, and fonts.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    firefox-esr \
    wget \
    ca-certificates \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# geckodriver v0.35.0 — compatible with Firefox ESR 115+
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz \
    && tar -xzf geckodriver-v0.35.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/geckodriver \
    && rm geckodriver-v0.35.0-linux64.tar.gz \
    && chmod +x /usr/local/bin/geckodriver

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Force Firefox into headless mode — containers have no display server.
ENV HEADLESS=1
ENV MOZ_HEADLESS=1

EXPOSE 5000

CMD ["python3", "app.py"]
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
    firefox-esr \
    wget \
    unzip \
    xvfb \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libasound2 \
    libx11-xcb1 \
    libxt6 \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz \
    && tar -xvzf geckodriver-v0.35.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/ \
    && rm geckodriver-v0.35.0-linux64.tar.gz \
    && chmod +x /usr/local/bin/geckodriver

WORKDIR /app

COPY . .

RUN pip install selenium pandas

# Send a blank line into the Python script by default to bypass the `input()` 
# prompt and start on page 1 automatically when Coolify runs it in the background.
CMD echo "" | python3 isbn_scraper.py
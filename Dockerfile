FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y firefox-esr wget unzip && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/mozilla/geckodriver/releases/latest/download/geckodriver-linux64.tar.gz \
    && tar -xvzf geckodriver-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver

WORKDIR /app

COPY . .

RUN pip install selenium pandas

CMD ["python3", "isbn_scraper.py"]
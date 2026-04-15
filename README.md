# Web Scraper - ISBN & Generic Scraper

## Quick Start

```bash
# Start the server
python3 app.py

# Open browser
http://localhost:5000
```

## Features

### 1. ISBN Scraper
- Scrapes 70,000+ ISBN records from isbn.gov.in
- Auto-resume from checkpoint
- Ultra-resilient for long runs

### 2. Generic Scraper

#### Deep Crawl Mode (🕸️ Scrape Every Link)
**How it works:**

1. **Opens main page** (e.g., https://www.ndtv.com/latest)
2. **Collects all article links** (e.g., 240 links)
3. **For each article:**
   - Opens the article page
   - Extracts heading (H1, H2, H3)
   - Extracts ALL paragraphs
   - Merges paragraphs into single text
   - Saves: Heading → Complete Article Content
   - Moves to next article
4. **Repeats** until all articles scraped

**Output format:**
```
━━━ ARTICLE START ━━━ | Article URL | Article 1 of 240
H2 Heading | Article Title |
Article Content | [ALL PARAGRAPHS MERGED] |
━━━ ARTICLE START ━━━ | Article URL | Article 2 of 240
H1 Heading | Article Title |
Article Content | [ALL PARAGRAPHS MERGED] |
```

#### Single Page Mode (📄 Scrape Single Page)
Scrapes only the provided URL.

## Usage

### Scrape News Site
1. Enter URL: `https://www.ndtv.com/latest`
2. Click: **🕸️ Scrape Every Link**
3. Wait for completion
4. Download CSV

### What Gets Scraped
- ✅ Article headings
- ✅ Complete article text (all paragraphs merged)
- ✅ Related links
- ✅ Data tables
- ❌ Ads, sidebars, comments (filtered out)

## Requirements

```bash
pip3 install -r requirements.txt
```

## Database

- **MongoDB** (primary storage)
- **CSV fallback** (automatic if MongoDB unavailable)

## Test

```bash
# Test single article
python3 test_single_article.py

# Test MongoDB connection
python3 test_db_connection.py
```

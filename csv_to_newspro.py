"""
csv_to_newspro.py - Convert CSV data to NewsPro format and send to application

This script reads your scraped CSV file, transforms it to NewsPro schema,
and either saves as JSON or sends directly to NewsPro endpoint.

Usage:
    # Convert CSV to JSON (for inspection)
    python csv_to_newspro.py --csv your_file.csv --output articles.json
    
    # Send CSV data directly to NewsPro
    python csv_to_newspro.py --csv your_file.csv --send --password admin
"""

import csv
import json
import requests
import argparse
import hashlib
from datetime import datetime
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVToNewsPro:
    """Convert CSV scraped data to NewsPro format."""
    
    def __init__(self):
        self.articles = []
    
    def parse_csv(self, csv_file: str) -> list:
        """
        Parse CSV file and extract articles.
        
        Args:
            csv_file: Path to CSV file
        
        Returns:
            List of article dictionaries
        """
        articles = []
        current_article = {}
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Skip empty rows
                    if not any(row.values()):
                        continue
                    
                    # Check if this is a new article (has URL/Link)
                    if row.get('Extracted Data', '').startswith('https://'):
                        # Save previous article if exists
                        if current_article.get('url'):
                            articles.append(current_article)
                        
                        # Start new article
                        current_article = {
                            'url': row.get('Extracted Data', ''),
                            'source': 'NDTV'
                        }
                    
                    # Map CSV columns to article fields
                    content_type = row.get('Content Type', '').lower()
                    extracted_data = row.get('Extracted Data', '')
                    
                    if 'headline' in content_type or 'h1' in content_type:
                        current_article['title'] = extracted_data
                    
                    elif 'h2' in content_type:
                        if 'description' not in current_article:
                            current_article['description'] = extracted_data
                    
                    elif 'article content' in content_type:
                        if 'content' not in current_article:
                            current_article['content'] = extracted_data
                        else:
                            # Append if multiple content sections
                            current_article['content'] += '\n\n' + extracted_data
                    
                    elif 'category' in content_type:
                        current_article['topic'] = extracted_data.lower().replace(' news', '')
                    
                    elif 'published date' in content_type:
                        current_article['publishedAt'] = extracted_data
                    
                    elif 'source' in content_type or 'author' in content_type:
                        current_article['author'] = extracted_data
                
                # Add last article
                if current_article.get('url'):
                    articles.append(current_article)
            
            logger.info(f"✓ Parsed {len(articles)} articles from CSV")
            return articles
        
        except Exception as e:
            logger.error(f"✗ Error parsing CSV: {e}")
            return []
    
    def transform_to_newspro(self, articles: list) -> list:
        """
        Transform articles to NewsPro schema.
        
        Args:
            articles: List of parsed articles
        
        Returns:
            List of articles in NewsPro format
        """
        transformed = []
        
        for article in articles:
            # Skip if missing required fields
            if not article.get('url') or not article.get('title'):
                logger.warning(f"Skipping article - missing url or title")
                continue
            
            # Generate unique ID
            article_id = hashlib.md5(article['url'].encode()).hexdigest()
            
            # Parse dates
            def parse_date(date_str):
                if not date_str:
                    return datetime.utcnow().isoformat() + "Z"
                
                try:
                    # Try ISO format first
                    if 'T' in date_str:
                        return date_str
                    # Try "Apr 2026" format
                    elif 'apr' in date_str.lower():
                        return datetime(2026, 4, 21).isoformat() + "Z"
                    else:
                        return datetime.utcnow().isoformat() + "Z"
                except:
                    return datetime.utcnow().isoformat() + "Z"
            
            # Transform to NewsPro schema
            newspro_article = {
                "title": article.get('title', '').strip(),
                "description": article.get('description', '').strip(),
                "content": article.get('content', '').strip(),
                "url": article.get('url', '').strip(),
                "urlToImage": None,  # CSV doesn't have images
                "source": article.get('source', 'NDTV').strip(),
                "author": article.get('author', 'Unknown').strip(),
                "topic": article.get('topic', 'news').lower().strip(),
                "publishedAt": parse_date(article.get('publishedAt')),
                "fetchedAt": datetime.utcnow().isoformat() + "Z"
            }
            
            # Validate required fields
            if not newspro_article['title'] or not newspro_article['url']:
                logger.warning(f"Skipping - invalid article")
                continue
            
            transformed.append(newspro_article)
        
        logger.info(f"✓ Transformed {len(transformed)} articles to NewsPro format")
        return transformed
    
    def save_to_json(self, articles: list, output_file: str) -> bool:
        """
        Save articles to JSON file.
        
        Args:
            articles: List of articles
            output_file: Path to output JSON file
        
        Returns:
            True if successful
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✓ Saved {len(articles)} articles to {output_file}")
            logger.info(f"  File size: {Path(output_file).stat().st_size / 1024:.2f} KB")
            return True
        
        except Exception as e:
            logger.error(f"✗ Error saving JSON: {e}")
            return False
    
    def send_to_newspro(self, articles: list, newspro_url: str, password: str) -> bool:
        """
        Send articles to NewsPro endpoint.
        
        Args:
            articles: List of articles
            newspro_url: NewsPro base URL
            password: Admin password
        
        Returns:
            True if successful
        """
        endpoint = f"{newspro_url}/api/news/scraper"
        
        # Format for newsdata.io format (optional, but compatible)
        payload = {
            "data": {
                "status": "success",
                "totalResults": len(articles),
                "results": [
                    {
                        "article_id": hashlib.md5(a['url'].encode()).hexdigest(),
                        "link": a['url'],
                        "title": a['title'],
                        "description": a['description'],
                        "content": a['content'],
                        "keywords": [],
                        "creator": [a['author']],
                        "language": "english",
                        "country": ["in"],
                        "category": [a['topic']],
                        "datatype": "news",
                        "pubDate": a['publishedAt'].replace('Z', '').replace('T', ' '),
                        "pubDateTZ": "UTC",
                        "fetched_at": a['fetchedAt'].replace('Z', '').replace('T', ' '),
                        "image_url": a['urlToImage'],
                        "video_url": None,
                        "source_id": a['source'].lower().replace(' ', '_'),
                        "source_name": a['source'],
                        "source_priority": 50,
                        "source_url": "https://ndtv.com",
                        "source_icon": None,
                        "sentiment": "neutral",
                        "ai_tag": [a['topic']],
                        "duplicate": False
                    }
                    for a in articles
                ]
            },
            "password": password
        }
        
        try:
            logger.info(f"Sending {len(articles)} articles to {endpoint}...")
            
            response = requests.post(
                endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✓ Success! {result['message']}")
                
                if 'stats' in result:
                    stats = result['stats']
                    logger.info(f"  - Total processed: {stats['totalProcessed']}")
                    logger.info(f"  - Valid articles: {stats['validArticles']}")
                    logger.info(f"  - New saved: {stats['newArticlesSaved']}")
                    logger.info(f"  - Duplicates skipped: {stats['duplicatesSkipped']}")
                
                logger.info(f"\n✓ Visit http://localhost:3000 to see your articles!")
                return True
            else:
                error = response.json().get('error', 'Unknown error')
                logger.error(f"✗ Failed: {error}")
                logger.error(f"  Status: {response.status_code}")
                return False
        
        except requests.exceptions.ConnectionError:
            logger.error(f"✗ Could not connect to NewsPro at {newspro_url}")
            logger.error(f"  Make sure NewsPro is running: npm run dev")
            return False
        except Exception as e:
            logger.error(f"✗ Error sending to NewsPro: {e}")
            return False
    
    def display_sample(self, articles: list, count: int = 3):
        """
        Display sample articles for verification.
        
        Args:
            articles: List of articles
            count: Number of samples to show
        """
        logger.info("\n" + "="*60)
        logger.info("SAMPLE ARTICLES (First 3)")
        logger.info("="*60)
        
        for i, article in enumerate(articles[:count], 1):
            logger.info(f"\nArticle {i}:")
            logger.info(f"  Title: {article['title'][:60]}...")
            logger.info(f"  URL: {article['url'][:50]}...")
            logger.info(f"  Topic: {article['topic']}")
            logger.info(f"  Author: {article['author']}")
            logger.info(f"  Description: {article['description'][:50]}...")
            logger.info(f"  Published: {article['publishedAt']}")
        
        logger.info("\n" + "="*60)


def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV scraper data to NewsPro format"
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Path to CSV file to convert'
    )
    parser.add_argument(
        '--output',
        help='Output JSON file (if not sending directly)'
    )
    parser.add_argument(
        '--send',
        action='store_true',
        help='Send directly to NewsPro instead of saving JSON'
    )
    parser.add_argument(
        '--url',
        default='http://localhost:3000',
        help='NewsPro URL (default: http://localhost:3000)'
    )
    parser.add_argument(
        '--password',
        default='admin',
        help='Admin password for NewsPro'
    )
    
    args = parser.parse_args()
    
    # Verify CSV file exists
    if not Path(args.csv).exists():
        logger.error(f"✗ CSV file not found: {args.csv}")
        return 1
    
    # Initialize converter
    converter = CSVToNewsPro()
    
    # Parse and transform
    logger.info(f"Reading CSV file: {args.csv}")
    parsed = converter.parse_csv(args.csv)
    
    if not parsed:
        logger.error("✗ No articles found in CSV")
        return 1
    
    articles = converter.transform_to_newspro(parsed)
    
    if not articles:
        logger.error("✗ No valid articles after transformation")
        return 1
    
    # Show samples
    converter.display_sample(articles)
    
    # Save or send
    if args.send:
        logger.info(f"\n{'='*60}")
        logger.info("SENDING TO NEWSPRO")
        logger.info(f"{'='*60}\n")
        success = converter.send_to_newspro(articles, args.url, args.password)
        return 0 if success else 1
    else:
        # Default to saving JSON
        output_file = args.output or 'articles.json'
        logger.info(f"\n{'='*60}")
        logger.info("SAVING TO JSON")
        logger.info(f"{'='*60}\n")
        success = converter.save_to_json(articles, output_file)
        
        if success:
            logger.info(f"\n💡 Next steps:")
            logger.info(f"  1. Review {output_file}")
            logger.info(f"  2. Send to NewsPro:")
            logger.info(f"     python csv_to_newspro.py --csv {args.csv} --send --password admin")
        
        return 0 if success else 1


if __name__ == "__main__":
    exit(main())

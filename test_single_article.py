#!/usr/bin/env python3
"""
Test script to scrape a single article and verify content extraction
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
import time

def test_article(url):
    """Test scraping a single article URL"""
    print(f"\n{'='*80}")
    print(f"Testing Article Extraction")
    print(f"{'='*80}")
    print(f"URL: {url}\n")
    
    # Setup Firefox
    options = FirefoxOptions()
    options.add_argument("-headless")
    driver = webdriver.Firefox(options=options)
    
    try:
        # Load the page
        print("📰 Loading page...")
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        print(f"✓ Page loaded: {driver.title}\n")
        print(f"Current URL: {driver.current_url}\n")
        
        # Find all paragraphs
        paragraphs = driver.find_elements(By.TAG_NAME, 'p')
        print(f"📝 Found {len(paragraphs)} <p> tags\n")
        
        # Extract and display paragraphs
        article_paragraphs = []
        for i, p in enumerate(paragraphs, 1):
            text = p.text.strip()
            if text and len(text) > 20:
                # Skip obvious UI elements
                skip_phrases = ['share', 'follow', 'subscribe', 'advertisement', 'sign up', 'log in']
                if any(text.lower().startswith(phrase) for phrase in skip_phrases):
                    continue
                
                article_paragraphs.append(text)
                print(f"Paragraph {i}: {text[:100]}{'...' if len(text) > 100 else ''}")
        
        print(f"\n{'='*80}")
        print(f"✅ Extracted {len(article_paragraphs)} article paragraphs")
        print(f"{'='*80}\n")
        
        # Show merged content
        if article_paragraphs:
            merged = ' '.join(article_paragraphs)
            print("📄 MERGED ARTICLE CONTENT:")
            print(f"{merged[:500]}...")
            print(f"\n📊 Total words: {len(merged.split())}")
            print(f"📊 Total characters: {len(merged)}")
        else:
            print("❌ No paragraphs extracted!")
            print("\nDebugging info:")
            print(f"- Page title: {driver.title}")
            print(f"- Current URL: {driver.current_url}")
            print(f"- Total <p> tags: {len(paragraphs)}")
            
            # Show all paragraph text for debugging
            print("\nAll <p> tag content:")
            for i, p in enumerate(paragraphs[:10], 1):  # Show first 10
                print(f"{i}. {p.text.strip()[:100]}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    # Test with the article you mentioned
    test_url = "https://www.ndtv.com/india-news/maharashtra-sexual-assault-case-mohammad-ayaz-tanveer-bulldozer-action-on-maharashtra-man-accused-of-sexually-assaulting-180-minors-11360085"
    
    print("\n" + "="*80)
    print("SINGLE ARTICLE EXTRACTION TEST")
    print("="*80)
    print("\nThis script will:")
    print("1. Open the article page")
    print("2. Extract all paragraphs")
    print("3. Show you what content is found")
    print("4. Display the merged article text")
    
    test_article(test_url)
    
    print("\n" + "="*80)
    print("Test complete!")
    print("="*80)

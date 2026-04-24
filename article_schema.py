"""
article_schema.py — MongoDB schema definition for news articles.

Defines the structure for storing news articles in MongoDB.
"""

from datetime import datetime
from typing import Optional, Dict, Any


class ArticleSchema:
    """
    News article schema for MongoDB storage.
    
    Required fields:
        - title: Article headline
        - url: Unique article link
        - topic: Article category/topic
    
    Optional fields:
        - description: Brief summary
        - content: Full article text
        - urlToImage: Image as base64 data URI (data:image/...;base64,...) or URL fallback
        - source: News source name
        - author: Author name
        - publishedAt: Publication date (ISO format)
        - fetchedAt: When scraped (ISO format)
    """
    
    @staticmethod
    def create_article(
        title: str,
        url: str,
        topic: str,
        description: Optional[str] = None,
        content: Optional[str] = None,
        urlToImage: Optional[str] = None,
        source: Optional[str] = None,
        author: Optional[str] = None,
        publishedAt: Optional[str] = None,
        fetchedAt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new article document for MongoDB.
        STRICTLY ONLY these 10 fields - NO EXTRA FIELDS ALLOWED.
        
        Args:
            title: Article headline (REQUIRED)
            url: Unique article link (REQUIRED)
            topic: Category/topic (REQUIRED)
            description: Brief summary (optional)
            content: Full article text (optional)
            urlToImage: Image as base64 data URI or URL (optional)
            source: News source name (optional)
            author: Author name (optional)
            publishedAt: Publication date in ISO format (optional)
            fetchedAt: Fetch timestamp in ISO format (optional)
        
        Returns:
            Dictionary with ONLY these 10 fields - no extras
        """
        
        # Validate required fields
        if not title or not isinstance(title, str):
            raise ValueError("title is required and must be a string")
        if not url or not isinstance(url, str):
            raise ValueError("url is required and must be a string")
        if not topic or not isinstance(topic, str):
            raise ValueError("topic is required and must be a string")
        
        # Build the article document - STRICT SCHEMA ORDER (EXACTLY 10 FIELDS)
        article = {}
        
        # Field 1: title (REQUIRED)
        article["title"] = title.strip()
        
        # Field 2: description (optional)
        if description and description.strip():
            article["description"] = description.strip()
        
        # Field 3: content (optional)
        if content and content.strip():
            article["content"] = content.strip()
        
        # Field 4: url (REQUIRED)
        article["url"] = url.strip()
        
        # Field 5: urlToImage (optional)
        if urlToImage and urlToImage.strip():
            article["urlToImage"] = urlToImage.strip()
        
        # Field 6: source (optional)
        if source and source.strip():
            article["source"] = source.strip()
        
        # Field 7: author (optional)
        if author and author.strip():
            article["author"] = author.strip()
        
        # Field 8: topic (REQUIRED)
        article["topic"] = topic.strip()
        
        # Field 9: publishedAt (optional)
        if publishedAt and publishedAt.strip():
            article["publishedAt"] = publishedAt.strip()
        
        # Field 10: fetchedAt (optional - auto-generated if not provided)
        article["fetchedAt"] = fetchedAt or datetime.utcnow().isoformat() + "Z"
        
        return article
    
    @staticmethod
    def clean_article(article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean an article to ensure ONLY the 10 allowed fields exist.
        Removes any extra fields that may have been added.
        
        Args:
            article: Article document to clean
        
        Returns:
            Article with ONLY the 10 allowed fields
        """
        ALLOWED_FIELDS = {
            "title", "description", "content", "url", "urlToImage",
            "source", "author", "topic", "publishedAt", "fetchedAt"
        }
        
        # Create new dict with only allowed fields
        cleaned = {k: v for k, v in article.items() if k in ALLOWED_FIELDS}
        
        return cleaned
    
    @staticmethod
    def validate_article(article: Dict[str, Any]) -> bool:
        """
        Validate that an article has all required fields.
        
        Args:
            article: Article document to validate
        
        Returns:
            True if valid, False otherwise
        """
        required_fields = {"title", "url", "topic"}
        return all(field in article and article[field] for field in required_fields)

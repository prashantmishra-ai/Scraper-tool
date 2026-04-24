"""
image_utils.py — Image downloading and base64 encoding utilities.

Provides functions to download images from URLs and convert them to base64 format.
Makes the scraper tolerant to broken image links by storing complete image data.
"""

import requests
import base64
import logging
from typing import Optional
from io import BytesIO
from PIL import Image

logger = logging.getLogger(__name__)

# Maximum image size to download (5MB)
MAX_IMAGE_SIZE = 5 * 1024 * 1024

# Timeout for image downloads (seconds)
IMAGE_DOWNLOAD_TIMEOUT = 10


def download_image_as_base64(image_url: str, max_size: int = MAX_IMAGE_SIZE) -> Optional[str]:
    """
    Download an image from a URL and convert it to base64 format.
    
    Args:
        image_url: URL of the image to download
        max_size: Maximum size in bytes to download (default 5MB)
    
    Returns:
        Base64 encoded string with data URI prefix (e.g., "data:image/jpeg;base64,...")
        Returns None if download fails or image is too large
    """
    if not image_url or not isinstance(image_url, str):
        return None
    
    # Skip data URIs that are already base64
    if image_url.startswith('data:'):
        return image_url
    
    try:
        # Set headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': image_url.rsplit('/', 1)[0] + '/',
        }
        
        # Download image with streaming to check size
        response = requests.get(
            image_url,
            headers=headers,
            timeout=IMAGE_DOWNLOAD_TIMEOUT,
            stream=True,
            allow_redirects=True
        )
        response.raise_for_status()
        
        # Check content length if available
        content_length = response.headers.get('content-length')
        if content_length and int(content_length) > max_size:
            logger.warning(f"Image too large ({content_length} bytes): {image_url[:100]}")
            return None
        
        # Read image data in chunks
        image_data = BytesIO()
        downloaded_size = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                downloaded_size += len(chunk)
                if downloaded_size > max_size:
                    logger.warning(f"Image exceeded max size during download: {image_url[:100]}")
                    return None
                image_data.write(chunk)
        
        # Get the image bytes
        image_bytes = image_data.getvalue()
        
        if not image_bytes:
            logger.warning(f"Empty image data: {image_url[:100]}")
            return None
        
        # Detect content type from response headers
        content_type = response.headers.get('content-type', '').lower()
        
        # If content type not in headers, try to detect from image data
        if not content_type or not content_type.startswith('image/'):
            try:
                img = Image.open(BytesIO(image_bytes))
                format_lower = img.format.lower() if img.format else 'jpeg'
                content_type = f'image/{format_lower}'
            except Exception:
                # Default to jpeg if detection fails
                content_type = 'image/jpeg'
        
        # Clean up content type (remove charset and other parameters)
        content_type = content_type.split(';')[0].strip()
        
        # Convert to base64
        base64_data = base64.b64encode(image_bytes).decode('utf-8')
        
        # Create data URI
        data_uri = f"data:{content_type};base64,{base64_data}"
        
        logger.info(f"Successfully converted image to base64 ({len(image_bytes)} bytes): {image_url[:100]}")
        return data_uri
    
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout downloading image: {image_url[:100]}")
        return None
    
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error downloading image {image_url[:100]}: {e}")
        return None
    
    except Exception as e:
        logger.error(f"Unexpected error processing image {image_url[:100]}: {e}")
        return None


def download_image_as_base64_with_fallback(image_url: str) -> Optional[str]:
    """
    Download an image and convert to base64, with fallback to URL on failure.
    
    Args:
        image_url: URL of the image to download
    
    Returns:
        Base64 encoded string if successful, original URL if download fails, None if no URL provided
    """
    if not image_url:
        return None
    
    # Try to download and convert to base64
    base64_data = download_image_as_base64(image_url)
    
    if base64_data:
        return base64_data
    
    # Fallback: return original URL
    logger.info(f"Falling back to URL for image: {image_url[:100]}")
    return image_url


def is_base64_image(data: str) -> bool:
    """
    Check if a string is a base64 encoded image (data URI).
    
    Args:
        data: String to check
    
    Returns:
        True if the string is a base64 data URI, False otherwise
    """
    if not data or not isinstance(data, str):
        return False
    
    return data.startswith('data:image/')


def get_image_size_from_base64(base64_data: str) -> Optional[int]:
    """
    Get the size in bytes of a base64 encoded image.
    
    Args:
        base64_data: Base64 data URI string
    
    Returns:
        Size in bytes, or None if invalid
    """
    if not is_base64_image(base64_data):
        return None
    
    try:
        # Extract base64 part (after "data:image/...;base64,")
        if ';base64,' in base64_data:
            base64_part = base64_data.split(';base64,', 1)[1]
            # Calculate approximate size (base64 is ~33% larger than original)
            return len(base64_part) * 3 // 4
        return None
    except Exception:
        return None

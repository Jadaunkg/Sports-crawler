"""
Article validator - checks if page is a valid article.
Rejects category pages, tag pages, galleries, etc.
"""

import re
from typing import Optional
from bs4 import BeautifulSoup

from src.config import get_config
from src.logging_config import get_logger

logger = get_logger("article.validator")


class ArticleValidator:
    """
    Validates that a page is a genuine news article.
    Checks for required elements and rejects non-article pages.
    """
    
    # Minimum content length (characters)
    MIN_CONTENT_LENGTH = 200
    
    # Required elements for valid article
    HEADLINE_SELECTORS = [
        "h1",
        "article h1",
        ".headline",
        ".article-title",
        "[itemprop='headline']",
        ".post-title",
    ]
    
    DATE_SELECTORS = [
        "time[datetime]",
        "[itemprop='datePublished']",
        ".publish-date",
        ".article-date",
        ".post-date",
        "meta[property='article:published_time']",
    ]
    
    CONTENT_SELECTORS = [
        "article",
        "[itemprop='articleBody']",
        ".article-body",
        ".article-content",
        ".post-content",
        ".entry-content",
        ".story-body",
    ]
    
    def __init__(self):
        self.config = get_config()
        self.reject_patterns = self.config.reject_patterns
    
    def is_valid_url(self, url: str) -> bool:
        """
        Check if URL pattern indicates an article.
        
        Args:
            url: URL to check
            
        Returns:
            False if URL matches rejection patterns
        """
        url_lower = url.lower()
        
        for pattern in self.reject_patterns:
            if pattern.lower() in url_lower:
                logger.debug(f"URL rejected by pattern '{pattern}'", extra={"url": url})
                return False
        
        return True
    
    def has_headline(self, soup: BeautifulSoup) -> bool:
        """Check if page has a headline."""
        for selector in self.HEADLINE_SELECTORS:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return True
        return False
    
    def has_date(self, soup: BeautifulSoup) -> bool:
        """Check if page has a publish date."""
        for selector in self.DATE_SELECTORS:
            element = soup.select_one(selector)
            if element:
                # Check for datetime attribute or text content
                if element.get("datetime") or element.get("content") or element.get_text(strip=True):
                    return True
        return False
    
    def has_content(self, soup: BeautifulSoup) -> bool:
        """Check if page has article content."""
        for selector in self.CONTENT_SELECTORS:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if len(text) >= self.MIN_CONTENT_LENGTH:
                    return True
        
        # Fallback: check paragraphs
        paragraphs = soup.find_all("p")
        total_text = " ".join(p.get_text(strip=True) for p in paragraphs)
        return len(total_text) >= self.MIN_CONTENT_LENGTH
    
    def validate(self, url: str, html_content: str) -> tuple[bool, Optional[str]]:
        """
        Validate that HTML content is a valid article.
        
        Args:
            url: Article URL
            html_content: HTML page content
            
        Returns:
            Tuple of (is_valid, rejection_reason)
        """
        # Check URL pattern first
        if not self.is_valid_url(url):
            return False, "URL matches rejection pattern"
        
        try:
            soup = BeautifulSoup(html_content, "lxml")
        except Exception as e:
            return False, f"HTML parse error: {e}"
        
        # Check for headline
        if not self.has_headline(soup):
            return False, "No headline found"
        
        # Check for date (optional but preferred)
        has_date = self.has_date(soup)
        
        # Check for content
        if not self.has_content(soup):
            return False, "Insufficient content"
        
        # Log validation result
        logger.debug(
            f"Article validated (date={'yes' if has_date else 'no'})",
            extra={"url": url}
        )
        
        return True, None
    
    def quick_validate_url(self, url: str) -> bool:
        """Quick URL-only validation without fetching content."""
        return self.is_valid_url(url)

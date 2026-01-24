"""
Article content extractor.
Extracts clean article data from HTML.
"""

import re
from typing import Optional
from dataclasses import dataclass
from datetime import datetime
from bs4 import BeautifulSoup, NavigableString
from dateutil.parser import parse as parse_date

from src.logging_config import get_logger

logger = get_logger("article.extractor")


@dataclass
class ExtractedArticle:
    """Extracted article data."""
    url: str
    title: str
    author: Optional[str] = None
    publish_date: Optional[str] = None
    content: str = ""
    sport_category: Optional[str] = None
    source_site: str = ""


class ArticleExtractor:
    """
    Extracts article content from HTML.
    Removes ads, scripts, and irrelevant markup.
    """
    
    # Selectors for various article parts
    TITLE_SELECTORS = [
        "h1",
        "article h1",
        ".headline",
        ".article-title",
        "[itemprop='headline']",
        "meta[property='og:title']",
    ]
    
    AUTHOR_SELECTORS = [
        "[itemprop='author']",
        ".author-name",
        ".byline",
        ".article-author",
        "meta[name='author']",
        "[rel='author']",
    ]
    
    DATE_SELECTORS = [
        "time[datetime]",
        "[itemprop='datePublished']",
        ".publish-date",
        ".article-date",
        "meta[property='article:published_time']",
    ]
    
    CONTENT_SELECTORS = [
        "[itemprop='articleBody']",
        "article .content",
        ".article-body",
        ".article-content",
        ".post-content",
        ".entry-content",
        ".story-body",
        "article",
    ]
    
    # Elements to remove from content
    REMOVE_SELECTORS = [
        "script",
        "style",
        "nav",
        "header",
        "footer",
        "aside",
        ".advertisement",
        ".ad",
        ".ads",
        ".social-share",
        ".related-articles",
        ".comments",
        ".comment-section",
        ".newsletter",
        ".subscription",
        "iframe",
        "form",
    ]
    
    def __init__(self):
        pass
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract article title."""
        for selector in self.TITLE_SELECTORS:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == "meta":
                    content = elem.get("content", "")
                    if content:
                        return self._clean_text(content)
                else:
                    text = elem.get_text(strip=True)
                    if text:
                        return self._clean_text(text)
        
        # Fallback to page title
        if soup.title:
            return self._clean_text(soup.title.get_text())
        
        return "Untitled"
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author name."""
        for selector in self.AUTHOR_SELECTORS:
            elem = soup.select_one(selector)
            if elem:
                if elem.name == "meta":
                    content = elem.get("content", "")
                    if content:
                        return self._clean_text(content)
                else:
                    text = elem.get_text(strip=True)
                    if text and len(text) < 100:  # Sanity check
                        return self._clean_text(text)
        return None
    
    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publish date as ISO string."""
        for selector in self.DATE_SELECTORS:
            elem = soup.select_one(selector)
            if elem:
                date_str = None
                
                # Try datetime attribute
                if elem.get("datetime"):
                    date_str = elem["datetime"]
                elif elem.get("content"):
                    date_str = elem["content"]
                else:
                    date_str = elem.get_text(strip=True)
                
                if date_str:
                    try:
                        parsed = parse_date(date_str)
                        return parsed.isoformat()
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract clean article body."""
        # Find content container
        content_elem = None
        for selector in self.CONTENT_SELECTORS:
            content_elem = soup.select_one(selector)
            if content_elem:
                break
        
        if not content_elem:
            # Fallback: use entire body
            content_elem = soup.body or soup
        
        # Clone to avoid modifying original
        content_elem = BeautifulSoup(str(content_elem), "lxml")
        
        # Remove unwanted elements
        for selector in self.REMOVE_SELECTORS:
            for elem in content_elem.select(selector):
                elem.decompose()
        
        # Extract text from paragraphs
        paragraphs = []
        for p in content_elem.find_all(["p", "h2", "h3", "h4", "blockquote"]):
            text = p.get_text(strip=True)
            if text and len(text) > 20:  # Skip tiny fragments
                paragraphs.append(text)
        
        content = "\n\n".join(paragraphs)
        return self._clean_text(content)
    
    def extract(self, url: str, html_content: str, source_site: str) -> ExtractedArticle:
        """
        Extract article data from HTML.
        
        Args:
            url: Article URL
            html_content: HTML content
            source_site: Source site name
            
        Returns:
            ExtractedArticle with all extracted data
        """
        try:
            soup = BeautifulSoup(html_content, "lxml")
        except Exception as e:
            logger.error(f"HTML parse error: {e}", extra={"url": url})
            return ExtractedArticle(
                url=url,
                title="Parse Error",
                source_site=source_site
            )
        
        title = self._extract_title(soup)
        author = self._extract_author(soup)
        publish_date = self._extract_date(soup)
        content = self._extract_content(soup)
        
        article = ExtractedArticle(
            url=url,
            title=title,
            author=author,
            publish_date=publish_date,
            content=content,
            source_site=source_site
        )
        
        logger.info(
            f"Extracted article: {title[:50]}...",
            extra={"url": url}
        )
        
        return article

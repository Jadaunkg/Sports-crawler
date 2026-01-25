"""
Sitemap XML parser.
Supports both sitemap index and urlset formats.
"""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from lxml import etree
from dateutil.parser import parse as parse_date

from src.logging_config import get_logger

logger = get_logger("sitemap.parser")

# XML namespaces
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
NEWS_NS = {"news": "http://www.google.com/schemas/sitemap-news/0.9"}


@dataclass
class SitemapEntry:
    """A single entry from a sitemap."""
    loc: str  # URL
    lastmod: Optional[str] = None  # Last modified date
    changefreq: Optional[str] = None  # Change frequency
    priority: Optional[float] = None  # Priority
    
    # News sitemap specific
    news_title: Optional[str] = None
    news_publication_date: Optional[str] = None
    news_publication_name: Optional[str] = None
    
    @property
    def lastmod_datetime(self) -> Optional[datetime]:
        """Parse lastmod as datetime."""
        if not self.lastmod:
            return None
        try:
            return parse_date(self.lastmod)
        except (ValueError, TypeError):
            return None


@dataclass
class SitemapIndexEntry:
    """A sitemap reference from a sitemap index."""
    loc: str  # Sitemap URL
    lastmod: Optional[str] = None


class SitemapParser:
    """
    Parser for XML sitemaps.
    Handles both sitemap index and regular urlsets.
    """
    
    def __init__(self):
        pass
    
    def is_sitemap_index(self, xml_content: str) -> bool:
        """Check if content is a sitemap index."""
        return "<sitemapindex" in xml_content
    
    def parse_index(self, xml_content: str) -> List[SitemapIndexEntry]:
        """
        Parse a sitemap index file.
        
        Args:
            xml_content: XML string
            
        Returns:
            List of SitemapIndexEntry with nested sitemap URLs
        """
        entries = []
        
        try:
            root = etree.fromstring(xml_content.encode("utf-8"))
            
            # Find all <sitemap> elements
            for sitemap in root.xpath("//sm:sitemap", namespaces=SITEMAP_NS):
                loc_elem = sitemap.find("sm:loc", namespaces=SITEMAP_NS)
                lastmod_elem = sitemap.find("sm:lastmod", namespaces=SITEMAP_NS)
                
                if loc_elem is not None and loc_elem.text:
                    entries.append(SitemapIndexEntry(
                        loc=loc_elem.text.strip(),
                        lastmod=lastmod_elem.text.strip() if lastmod_elem is not None and lastmod_elem.text else None
                    ))
            
            logger.info(f"Parsed sitemap index with {len(entries)} nested sitemaps")
            
        except etree.XMLSyntaxError as e:
            logger.error(f"XML parsing error in sitemap index: {e}")
        
        return entries
    
    def parse_urlset(self, xml_content: str) -> List[SitemapEntry]:
        """
        Parse a regular sitemap (urlset).
        
        Args:
            xml_content: XML string
            
        Returns:
            List of SitemapEntry
        """
        entries = []
        
        try:
            root = etree.fromstring(xml_content.encode("utf-8"))
            
            # Find all <url> elements
            for url_elem in root.xpath("//sm:url", namespaces=SITEMAP_NS):
                loc = url_elem.find("sm:loc", namespaces=SITEMAP_NS)
                lastmod = url_elem.find("sm:lastmod", namespaces=SITEMAP_NS)
                changefreq = url_elem.find("sm:changefreq", namespaces=SITEMAP_NS)
                priority = url_elem.find("sm:priority", namespaces=SITEMAP_NS)
                
                if loc is None or not loc.text:
                    continue
                
                entry = SitemapEntry(
                    loc=loc.text.strip(),
                    lastmod=lastmod.text.strip() if lastmod is not None and lastmod.text else None,
                    changefreq=changefreq.text.strip() if changefreq is not None and changefreq.text else None,
                    priority=float(priority.text) if priority is not None and priority.text else None,
                )
                
                # Check for news sitemap data
                news_elem = url_elem.find("news:news", namespaces=NEWS_NS)
                if news_elem is not None:
                    title = news_elem.find("news:title", namespaces=NEWS_NS)
                    pub_date = news_elem.find("news:publication_date", namespaces=NEWS_NS)
                    publication = news_elem.find("news:publication", namespaces=NEWS_NS)
                    
                    if title is not None and title.text:
                        entry.news_title = title.text.strip()
                    if pub_date is not None and pub_date.text:
                        entry.news_publication_date = pub_date.text.strip()
                    if publication is not None:
                        name = publication.find("news:name", namespaces=NEWS_NS)
                        if name is not None and name.text:
                            entry.news_publication_name = name.text.strip()
                
                entries.append(entry)
            
            logger.info(f"Parsed urlset with {len(entries)} URLs")
            
        except etree.XMLSyntaxError as e:
            logger.error(f"XML parsing error in urlset: {e}")
        
        return entries
    
    def parse_text_format(self, content: str) -> List[SitemapEntry]:
        """
        Parse text-formatted sitemaps (often from plugins).
        Expected format:
        URL <tab/space> Last Modified
        https://example.com/page 2024-01-01T12:00:00Z
        """
        entries = []
        lines = content.strip().split('\n')
        
        # Flag to start parsing after header
        start_parsing = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Heuristic to detect start of data or just process lines that look like URLs
            if line.startswith('http'):
                parts = line.split()
                if not parts:
                    continue
                    
                loc = parts[0]
                lastmod = parts[1] if len(parts) > 1 else None
                
                # Basic validation
                if "://" not in loc:
                    continue
                    
                entries.append(SitemapEntry(
                    loc=loc,
                    lastmod=lastmod
                ))
            
            # Check for header row to avoid false positives if we want to be stricter
            # "URL Last Modified"
            
        if entries:
            logger.info(f"Parsed text format sitemap with {len(entries)} URLs")
            
        return entries

    def parse(self, xml_content: str) -> tuple[List[SitemapEntry], List[SitemapIndexEntry]]:
        """
        Parse any sitemap content.
        
        Returns:
            Tuple of (url_entries, index_entries)
            One of these will be empty depending on sitemap type.
        """
        # First try parsing as XML
        try:
            if self.is_sitemap_index(xml_content):
                return [], self.parse_index(xml_content)
            else:
                # Try parsing as proper XML urlset
                entries = self.parse_urlset(xml_content)
                if entries:
                    return entries, []
        except etree.XMLSyntaxError:
            # Fallback to text parsing if XML parsing fails
            pass
            
        # If XML parsing failed or returned empty (and wasn't index), try text format
        text_entries = self.parse_text_format(xml_content)
        if text_entries:
            return text_entries, []
            
        # If both failed, re-raise the XML error or return empty
        # For now return empty with error log
        logger.warning("Failed to parse sitemap as XML or text format")
        return [], []

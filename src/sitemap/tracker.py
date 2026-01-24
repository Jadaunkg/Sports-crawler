"""
URL tracker for detecting new articles.
Compares sitemap URLs against database records.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, date
import aiohttp
from dateutil.parser import parse as parse_date

from src.sitemap.fetcher import SitemapFetcher
from src.sitemap.parser import SitemapParser, SitemapEntry
from src.database.repository import get_repository, Site
from src.logging_config import get_logger

logger = get_logger("sitemap.tracker")


class UrlTracker:
    """
    Tracks discovered URLs and identifies new ones.
    Handles recursive sitemap index fetching.
    """
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.fetcher = SitemapFetcher(session)
        self.parser = SitemapParser()
        self.repo = get_repository()
    
    async def get_all_sitemap_urls(self, sitemap_url: str) -> List[SitemapEntry]:
        """
        Fetch all URLs from a sitemap, handling index files recursively.
        
        Args:
            sitemap_url: Root sitemap URL (may be index)
            
        Returns:
            Flat list of all SitemapEntry from all nested sitemaps
        """
        all_entries = []
        urls_to_process = [sitemap_url]
        processed = set()
        
        async with self.fetcher:
            while urls_to_process:
                current_url = urls_to_process.pop(0)
                
                if current_url in processed:
                    continue
                processed.add(current_url)
                
                content = await self.fetcher.fetch(current_url)
                if not content:
                    continue
                
                url_entries, index_entries = self.parser.parse(content)
                
                # Add URL entries to results
                all_entries.extend(url_entries)
                
                # Queue nested sitemaps for processing
                for idx_entry in index_entries:
                    if idx_entry.loc not in processed:
                        urls_to_process.append(idx_entry.loc)
        
        logger.info(
            f"Collected {len(all_entries)} URLs from {len(processed)} sitemaps",
            extra={"url": sitemap_url}
        )
        return all_entries
    
    async def find_new_urls(self, site: Site) -> List[Dict[str, Any]]:
        """
        Find URLs from sitemap that are not in the database.
        
        Args:
            site: Site configuration
            
        Returns:
            List of dicts with 'url' and 'lastmod' for new URLs
        """
        # Fetch all URLs from sitemap(s)
        entries = await self.get_all_sitemap_urls(site.sitemap_url)
        
        if not entries:
            logger.warning(f"No URLs found in sitemap", extra={"site": site.name})
            return []
        
        # Get all URLs as list
        all_urls = [e.loc for e in entries]
        
        # Check which are already known
        known_urls = self.repo.get_known_urls_batch(all_urls)
        
        # Filter to new URLs only
        new_entries = []
        for entry in entries:
            if entry.loc not in known_urls:
                new_entries.append({
                    "url": entry.loc,
                    "lastmod": entry.lastmod or entry.news_publication_date,
                })
        
        logger.info(
            f"Found {len(new_entries)} new URLs out of {len(all_urls)} total",
            extra={"site": site.name, "urls_found": len(all_urls), "new_urls": len(new_entries)}
        )
        
        return new_entries
    
    def _is_today(self, date_str: Optional[str]) -> bool:
        """Check if a date string is from today."""
        if not date_str:
            return False
        try:
            parsed = parse_date(date_str)
            today = date.today()
            return parsed.date() == today
        except (ValueError, TypeError):
            return False
    
    def _is_within_days(self, date_str: Optional[str], days: int = 3) -> bool:
        """Check if a date string is within last N days."""
        if not date_str:
            return False
        try:
            parsed = parse_date(date_str)
            today = date.today()
            diff = (today - parsed.date()).days
            return 0 <= diff <= days
        except (ValueError, TypeError):
            return False
    
    async def find_today_urls(self, site: Site) -> List[Dict[str, Any]]:
        """
        Find URLs from sitemap that are from TODAY only and not in database.
        
        Args:
            site: Site configuration
            
        Returns:
            List of dicts with 'url' and 'lastmod' for today's new URLs
        """
        return await self.find_recent_urls(site, days=0)  # 0 = today only
    
    async def find_recent_urls(self, site: Site, days: int = 3) -> List[Dict[str, Any]]:
        """
        Find URLs from sitemap that are within last N days and not in database.
        
        Args:
            site: Site configuration
            days: Number of days to look back (0 = today only, 3 = last 3 days)
            
        Returns:
            List of dicts with 'url' and 'lastmod' for recent new URLs
        """
        # Fetch all URLs from sitemap(s)
        entries = await self.get_all_sitemap_urls(site.sitemap_url)
        
        if not entries:
            logger.warning(f"No URLs found in sitemap", extra={"site": site.name})
            return []
        
        # Filter to recent entries only using lastmod or news_publication_date
        recent_entries = []
        for entry in entries:
            date_str = entry.lastmod or entry.news_publication_date
            if self._is_within_days(date_str, days):
                recent_entries.append(entry)
        
        logger.info(
            f"Filtered to {len(recent_entries)} URLs from last {days} days out of {len(entries)} total",
            extra={"site": site.name}
        )
        
        if not recent_entries:
            return []
        
        # Get all URLs as list
        all_urls = [e.loc for e in recent_entries]
        
        # Check which are already known
        known_urls = self.repo.get_known_urls_batch(all_urls)
        
        # Filter to new URLs only
        new_entries = []
        for entry in recent_entries:
            if entry.loc not in known_urls:
                new_entries.append({
                    "url": entry.loc,
                    "lastmod": entry.lastmod or entry.news_publication_date,
                })
        
        logger.info(
            f"Found {len(new_entries)} new URLs from last {days} days",
            extra={"site": site.name, "urls_found": len(recent_entries), "new_urls": len(new_entries)}
        )
        
        return new_entries
    
    def record_new_urls(self, site: Site, new_urls: List[Dict[str, Any]]) -> int:
        """
        Record newly discovered URLs in database.
        
        Args:
            site: Site configuration
            new_urls: List of url dicts from find_new_urls
            
        Returns:
            Number of URLs recorded
        """
        if not new_urls:
            return 0
        
        return self.repo.add_discovered_urls_batch(site.id, new_urls)

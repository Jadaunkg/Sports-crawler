"""
URL tracker for detecting new articles.
Compares sitemap URLs against database records.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, date, timedelta
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
        
        # Check which URLs already have articles saved (not just discovered)
        urls_with_articles = self.repo.get_urls_with_articles_batch(all_urls)
        
        # Filter to URLs needing articles
        new_entries = []
        for entry in entries:
            if entry.loc not in urls_with_articles:
                new_entries.append({
                    "url": entry.loc,
                    "lastmod": entry.lastmod or entry.news_publication_date,
                })
        
        logger.info(
            f"Found {len(new_entries)} URLs needing articles out of {len(all_urls)} total",
            extra={"site": site.name, "urls_found": len(all_urls), "urls_needing_articles": len(new_entries)}
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
    
    def is_within_days(self, date_str: Optional[str], days: int = 3) -> bool:
        """Check if a date string is within last N days."""
        if not date_str:
            return False
        try:
            parsed = parse_date(date_str)
            # Ensure timezone awareness for comparison if needed, though date() strips it
            today = date.today()
            diff = (today - parsed.date()).days
            return 0 <= diff <= days
        except (ValueError, TypeError):
            # If date parsing fails, assume it's recent to be safe (avoid missing articles)
            return True
    
    async def find_today_urls(self, site: Site) -> List[Dict[str, Any]]:
        """
        Find URLs from sitemap that are from TODAY only and not in database.
        
        Args:
            site: Site configuration
            
        Returns:
            List of dicts with 'url' and 'lastmod' for today's new URLs
        """
        return await self.find_recent_urls(site, days=0)  # 0 = today only
    
    async def find_recent_urls(self, site: Site, days: int = 7, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Find URLs from sitemap that are recent (since last crawl or within last N days).
        
        Args:
            site: Site configuration
            days: Fallback max days lookback (default 7)
            since: Timestamp of last successful crawl
            
        Returns:
            List of dicts with 'url' and 'lastmod' for recent new URLs
        """
        # Fetch all URLs from sitemap(s)
        entries = await self.get_all_sitemap_urls(site.sitemap_url)
        
        if not entries:
            logger.warning(f"No URLs found in sitemap", extra={"site": site.name})
            return []
        
        # Determine cutoff time
        # Use 'since' if available, but ensure we don't go back further than 'days' (sanity check)
        now = datetime.now(timezone.utc)
        days_ago = now - timedelta(days=days)
        
        cutoff = days_ago
        if since:
             # Ensure 'since' is offset-aware
             if since.tzinfo is None:
                 since = since.replace(tzinfo=timezone.utc)
             
             # Use the more recent of (since, days_ago)
             # This prevents re-crawling excessively old content if 'since' is very old
             # But also respects 'since' if it was just 10 mins ago
             if since > days_ago:
                 cutoff = since
        
        logger.info(f"Filtering URLs since {cutoff}", extra={"site": site.name})

        # Filter to recent entries only
        recent_entries = []
        no_date_entries = []
        
        for entry in entries:
            date_str = entry.lastmod or entry.news_publication_date
            
            if date_str:
                # Parse date
                try:
                    dt = parse_date(date_str)
                    # Ensure dt is offset-aware for comparison
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    
                    if dt > cutoff:
                        recent_entries.append(entry)
                except Exception as e:
                    # Only fallback if parsing failed, not if comparison failed
                    # Check if 'dt' was defined in this scope to distinguish
                    # Actually, just use is_within_days only if parsing really failed
                    # Only fallback if parsing failed, not if comparison failed
                    # Check if 'dt' was defined in this scope to distinguish
                    # Actually, just use is_within_days only if parsing really failed
                    if self.is_within_days(date_str, days):
                         # If we have 'since', we strictly want newer than that.
                         # But is_within_days checks 'days'.
                         # If we failed to parse for precise check, fallback to days is acceptable risk
                         # or we accept it might be duplicate.
                         recent_entries.append(entry)
            else:
                # No date info - collect separately
                no_date_entries.append(entry)
        
        # If we have dated entries, use them. If not, use undated entries (limited)
        if recent_entries:
            logger.info(
                f"Filtered to {len(recent_entries)} URLs from last {days} days out of {len(entries)} total",
                extra={"site": site.name}
            )
        elif no_date_entries:
            # For sitemaps without dates, take a reasonable sample (last 100 URLs in sitemap)
            # Sitemaps typically have newest URLs first or last
            recent_entries = no_date_entries[-100:] if len(no_date_entries) > 100 else no_date_entries
            logger.info(
                f"Sitemap has no dates - using {len(recent_entries)} URLs from {len(entries)} total",
                extra={"site": site.name}
            )
        
        if not recent_entries:
            return []
        
        # Get all URLs as list
        all_urls = [e.loc for e in recent_entries]
        
        # KEY FIX: Check which URLs already have articles saved in article_links
        # This ensures URLs that were discovered but failed article processing
        # will be re-processed on subsequent crawls
        urls_with_articles = self.repo.get_article_links_by_urls(all_urls)
        
        # Filter to URLs that don't have articles yet
        new_entries = []
        for entry in recent_entries:
            if entry.loc not in urls_with_articles:
                new_entries.append({
                    "url": entry.loc,
                    "lastmod": entry.lastmod or entry.news_publication_date,
                })
        
        logger.info(
            f"Found {len(new_entries)} URLs needing articles since {cutoff}",
            extra={"site": site.name, "urls_found": len(recent_entries), "urls_needing_articles": len(new_entries)}
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
        
        # DEPRECATED: We no longer store discovered_urls separately.
        # They are stored directly as article_links when crawled.
        return len(new_urls)

"""
Data access repository for the crawler.
Handles all database operations with Supabase.
"""

import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from src.database.supabase_client import get_supabase
from src.logging_config import get_logger

logger = get_logger("database.repository")


@dataclass
class Site:
    """Site entity."""
    id: Optional[str] = None
    name: str = ""
    domain: str = ""
    sitemap_url: str = ""
    crawl_interval_minutes: int = 15
    is_active: bool = True
    site_type: str = "general"  # 'specific' or 'general'
    sport_focus: Optional[str] = None  # only for specific sites
    created_at: Optional[str] = None


@dataclass
class DiscoveredUrl:
    """Discovered URL entity."""
    id: Optional[str] = None
    site_id: str = ""
    url: str = ""
    url_hash: str = ""
    lastmod: Optional[str] = None
    first_seen_at: Optional[str] = None


@dataclass
class Article:
    """Article entity."""
    id: Optional[str] = None
    url_hash: str = ""
    url: str = ""
    title: str = ""
    author: Optional[str] = None
    publish_date: Optional[str] = None
    content: str = ""
    sport_category: Optional[str] = None
    source_site: str = ""
    crawl_time: Optional[str] = None
    ready_for_analysis: bool = False
    created_at: Optional[str] = None


@dataclass
class CrawlLog:
    """Crawl log entity."""
    id: Optional[str] = None
    site_id: str = ""
    crawl_type: str = ""  # 'sitemap' or 'article'
    status: str = ""  # 'success' or 'failed'
    http_code: Optional[int] = None
    urls_found: int = 0
    new_urls: int = 0
    error_message: Optional[str] = None
    created_at: Optional[str] = None


def url_hash(url: str) -> str:
    """Generate SHA256 hash for URL."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


class Repository:
    """
    Data access layer for crawler operations.
    All database interactions go through this class.
    """
    
    def __init__(self):
        self.db = get_supabase()
    
    # ==================== SITES ====================
    
    def get_all_sites(self) -> List[Site]:
        """Get all registered sites."""
        result = self.db.table("sites").select("*").execute()
        return [Site(**row) for row in result.data]
    
    def get_active_sites(self) -> List[Site]:
        """Get only active sites."""
        result = self.db.table("sites").select("*").eq("is_active", True).execute()
        return [Site(**row) for row in result.data]
    
    def get_site_by_domain(self, domain: str) -> Optional[Site]:
        """Get site by domain."""
        result = self.db.table("sites").select("*").eq("domain", domain).limit(1).execute()
        if result.data:
            return Site(**result.data[0])
        return None
    
    def upsert_site(self, site: Site) -> Site:
        """Insert or update a site."""
        data = {
            "name": site.name,
            "domain": site.domain,
            "sitemap_url": site.sitemap_url,
            "crawl_interval_minutes": site.crawl_interval_minutes,
            "is_active": site.is_active,
            "site_type": site.site_type,
            "sport_focus": site.sport_focus,
        }
        
        result = self.db.table("sites").upsert(
            data, 
            on_conflict="domain"
        ).execute()
        
        if result.data:
            logger.info(f"Upserted site: {site.name}", extra={"site": site.domain})
            return Site(**result.data[0])
        raise RuntimeError(f"Failed to upsert site: {site.name}")
    
    # ==================== DISCOVERED URLS ====================
    
    def is_url_known(self, url: str) -> bool:
        """Check if URL has been discovered before."""
        hash_val = url_hash(url)
        result = self.db.table("discovered_urls").select("id").eq("url_hash", hash_val).limit(1).execute()
        return len(result.data) > 0
    
    def get_known_urls_batch(self, urls: List[str]) -> set:
        """Get set of already known URLs from a batch."""
        if not urls:
            return set()
        
        hashes = [url_hash(u) for u in urls]
        # Query in batches to avoid URL length limits
        known_hashes = set()
        batch_size = 100
        
        for i in range(0, len(hashes), batch_size):
            batch = hashes[i:i + batch_size]
            result = self.db.table("discovered_urls").select("url_hash").in_("url_hash", batch).execute()
            known_hashes.update(row["url_hash"] for row in result.data)
        
        # Return original URLs that are known
        return {u for u in urls if url_hash(u) in known_hashes}
    
    def add_discovered_url(self, site_id: str, url: str, lastmod: Optional[str] = None) -> DiscoveredUrl:
        """Add a newly discovered URL."""
        now = datetime.now(timezone.utc).isoformat()
        data = {
            "site_id": site_id,
            "url": url,
            "url_hash": url_hash(url),
            "lastmod": lastmod,
            "first_seen_at": now,
        }
        
        result = self.db.table("discovered_urls").insert(data).execute()
        
        if result.data:
            return DiscoveredUrl(**result.data[0])
        raise RuntimeError(f"Failed to add URL: {url}")
    
    def add_discovered_urls_batch(self, site_id: str, urls: List[Dict[str, Any]]) -> int:
        """
        Add multiple discovered URLs in batch.
        
        Args:
            site_id: Site ID
            urls: List of dicts with 'url' and optional 'lastmod'
        
        Returns:
            Number of URLs added
        """
        if not urls:
            return 0
        
        now = datetime.now(timezone.utc).isoformat()
        records = []
        
        for item in urls:
            records.append({
                "site_id": site_id,
                "url": item["url"],
                "url_hash": url_hash(item["url"]),
                "lastmod": item.get("lastmod"),
                "first_seen_at": now,
            })
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                result = self.db.table("discovered_urls").insert(batch).execute()
                total_inserted += len(result.data)
            except Exception as e:
                # Handle duplicates gracefully
                logger.warning(f"Batch insert partial failure: {e}")
        
        logger.info(f"Added {total_inserted} new URLs", extra={"site": site_id})
        return total_inserted
    
    # ==================== ARTICLES ====================
    
    def get_article_by_url(self, url: str) -> Optional[Article]:
        """Get article by URL."""
        hash_val = url_hash(url)
        result = self.db.table("articles").select("*").eq("url_hash", hash_val).limit(1).execute()
        if result.data:
            return Article(**result.data[0])
        return None
    
    def save_article(self, article: Article) -> Article:
        """Save an article (insert or update)."""
        now = datetime.now(timezone.utc).isoformat()
        
        data = {
            "url_hash": url_hash(article.url),
            "url": article.url,
            "title": article.title,
            "author": article.author,
            "publish_date": article.publish_date,
            "content": article.content,
            "sport_category": article.sport_category,
            "source_site": article.source_site,
            "crawl_time": now,
            "ready_for_analysis": True,
        }
        
        result = self.db.table("articles").upsert(
            data,
            on_conflict="url_hash"
        ).execute()
        
        if result.data:
            logger.info(f"Saved article: {article.title[:50]}...", extra={"url": article.url})
            return Article(**result.data[0])
        raise RuntimeError(f"Failed to save article: {article.url}")
    
    def get_articles_for_analysis(self, limit: int = 100) -> List[Article]:
        """Get articles ready for analysis."""
        result = self.db.table("articles").select("*").eq("ready_for_analysis", True).limit(limit).execute()
        return [Article(**row) for row in result.data]
    
    def mark_article_analyzed(self, url_hash: str) -> bool:
        """Mark article as analyzed."""
        result = self.db.table("articles").update(
            {"ready_for_analysis": False}
        ).eq("url_hash", url_hash).execute()
        return len(result.data) > 0
    
    # ==================== CRAWL LOGS ====================
    
    def log_crawl(
        self,
        site_id: str,
        crawl_type: str,
        status: str,
        http_code: Optional[int] = None,
        urls_found: int = 0,
        new_urls: int = 0,
        error_message: Optional[str] = None
    ) -> CrawlLog:
        """Log a crawl operation."""
        data = {
            "site_id": site_id,
            "crawl_type": crawl_type,
            "status": status,
            "http_code": http_code,
            "urls_found": urls_found,
            "new_urls": new_urls,
            "error_message": error_message,
        }
        
        result = self.db.table("crawl_logs").insert(data).execute()
        
        if result.data:
            return CrawlLog(**result.data[0])
        raise RuntimeError("Failed to log crawl")
    
    def get_recent_crawl_logs(self, site_id: str, limit: int = 10) -> List[CrawlLog]:
        """Get recent crawl logs for a site."""
        result = self.db.table("crawl_logs").select("*").eq(
            "site_id", site_id
        ).order("created_at", desc=True).limit(limit).execute()
        return [CrawlLog(**row) for row in result.data]
    
    def get_failure_count(self, site_id: str, hours: int = 24) -> int:
        """Get count of failed crawls in last N hours."""
        # Note: Supabase doesn't support date arithmetic in queries easily
        # We'll fetch recent logs and filter in Python
        result = self.db.table("crawl_logs").select("*").eq(
            "site_id", site_id
        ).eq("status", "failed").order("created_at", desc=True).limit(100).execute()
        
        cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
        count = 0
        
        for row in result.data:
            created = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
            if created.timestamp() > cutoff:
                count += 1
        
        return count


# Global repository instance
_repository: Optional[Repository] = None


def get_repository() -> Repository:
    """Get or create the repository singleton."""
    global _repository
    if _repository is None:
        _repository = Repository()
    return _repository

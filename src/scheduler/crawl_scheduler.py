"""
Crawl scheduler using APScheduler.
Manages periodic sitemap checks and article crawling.
"""

import asyncio
from typing import Dict, Optional, List, Any
from datetime import datetime, timezone
import aiohttp

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import get_config
from src.database.repository import get_repository, Site, Article
from src.sitemap.tracker import UrlTracker
from src.crawler.http_client import HttpClient
from src.crawler.backoff import BackoffManager
from src.article.validator import ArticleValidator
from src.article.extractor import ArticleExtractor
from src.article.category_detector import CategoryDetector
from src.pipeline.trigger import TriggerService
from src.logging_config import get_logger

logger = get_logger("scheduler")


class CrawlScheduler:
    """
    Main crawler scheduler.
    Orchestrates sitemap monitoring and article crawling.
    """
    
    def __init__(self):
        self.config = get_config()
        self.repo = get_repository()
        self.scheduler = AsyncIOScheduler()
        self.backoff = BackoffManager()
        self.validator = ArticleValidator()
        self.extractor = ArticleExtractor()
        self.category_detector = CategoryDetector()
        self.trigger_service = TriggerService()
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
    
    async def start(self):
        """Start the scheduler."""
        if self._running:
            return
        
        # Initialize HTTP session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        )
        
        # Get active sites from database
        sites = self.repo.get_active_sites()
        for site in sites:
            self._schedule_site(site)
        
        # Schedule daily cleanup
        self.scheduler.add_job(
            self._cleanup_job,
            IntervalTrigger(hours=24),
            id="daily_cleanup",
            max_instances=1,
            next_run_time=datetime.now(timezone.utc) # Run on start to ensure cleanliness
        )
        logger.info("Scheduled daily cleanup job")

        # Start scheduler
        self.scheduler.start()
        self._running = True
        
        logger.info(f"Scheduler started with {len(sites)} sites")

    async def _cleanup_job(self):
        """Run daily cleanup of old articles."""
        logger.info("Starting daily article cleanup")
        try:
            count = self.repo.cleanup_old_articles(days=2)
            logger.info(f"Cleanup completed: removed {count} old articles")
        except Exception as e:
            logger.error(f"Cleanup job failed: {e}")
    
    async def stop(self):
        """Stop the scheduler gracefully."""
        if not self._running:
            return
        
        self.scheduler.shutdown(wait=True)
        
        if self._session:
            await self._session.close()
            self._session = None
        
        self._running = False
        logger.info("Scheduler stopped")
    

    def _schedule_site(self, site: Site):
        """Schedule crawl job for a site."""
        job_id = f"crawl_{site.domain}"
        
        # Remove existing job if any
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        # Schedule new job
        self.scheduler.add_job(
            self._crawl_site,
            IntervalTrigger(minutes=site.crawl_interval_minutes),
            id=job_id,
            args=[site],
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(timezone.utc)  # Run immediately on start
        )
        
        logger.info(
            f"Scheduled site crawl every {site.crawl_interval_minutes} minutes",
            extra={"site": site.name}
        )
    
    async def _crawl_site(self, site: Site):
        """
        Main crawl job for a site.
        1. Fetch sitemap and find new URLs (last 7 days)
        2. Deduplicate against article_links table
        3. Crawl content for new URLs
        4. Save to article_links (with content/metadata)
        """
        logger.info(f"Starting crawl cycle", extra={"site": site.name})
        
        if self.backoff.is_blocked(site.domain):
            logger.warning(f"Site blocked, skipping", extra={"site": site.name})
            return
        
        try:
            # Phase 1: Sitemap Discovery (Last 7 Days)
            # ----------------------------------------
            tracker = UrlTracker(self._session)
            
            # Fetch URLs strictly from last 7 days
            days_filter = 7
            sitemap_urls = await tracker.find_recent_urls(site, days=days_filter)
            
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="sitemap",
                status="success",
                http_code=200,
                urls_found=len(sitemap_urls),
                new_urls=0 
            )
            
            if not sitemap_urls:
                logger.info(f"No recent URLs found (last {days_filter} days)", extra={"site": site.name})
                return

            # Phase 2: Deduplication against article_links
            # --------------------------------------------
            # Extract just the URL strings to check
            found_urls = [u["url"] for u in sitemap_urls]
            
            # Check which are already in article_links
            known_urls = self.repo.get_article_links_by_urls(found_urls)
            
            # Filter to truly new URLs
            new_items = []
            for item in sitemap_urls:
                if item["url"] not in known_urls:
                     new_items.append(item)
            
            logger.info(
                f"Found {len(new_items)} new links to process out of {len(sitemap_urls)} recent items",
                extra={"site": site.name}
            )
            
            if not new_items:
                return

            # Phase 3: Crawl & Save (Direct to article_links)
            # -----------------------------------------------
            saved_count, failed_count = await self._process_new_articles(site, new_items)
            
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="article",
                status="success" if failed_count == 0 else "partial_failure",
                http_code=200,
                urls_found=len(new_items),
                new_urls=saved_count,
                error_message=f"Saved: {saved_count}, Failed: {failed_count}"
            )

            # Reset backoff
            self.backoff.record_success(site.domain)

        except Exception as e:
            logger.error(f"Crawl cycle failed: {e}", extra={"site": site.name})
            self.backoff.record_failure(site.domain)
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="sitemap",
                status="failed",
                error_message=str(e)
            )
    
    async def _process_new_articles(self, site: Site, new_urls: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process, crawl, and store new articles in article_links table.
        Returns (saved_count, failed_count).
        """
        # Filter valid URLs first
        valid_urls = [
            url_info for url_info in new_urls
            if self.validator.quick_validate_url(url_info["url"])
        ]
        
        if not valid_urls:
            return 0, 0
        
        # Use semaphore to limit concurrent requests (5 at a time)
        semaphore = asyncio.Semaphore(5)
        
        # Import ArticleLink locally to avoid circular dependency issues if any
        from src.database.repository import ArticleLink, url_hash
        
        async def process_single_article(url_info: Dict[str, Any], client: HttpClient) -> bool:
            """Process a single article. Returns True if saved successfully."""
            async with semaphore:
                url = url_info["url"]
                
                if self.backoff.is_blocked(site.domain):
                    return False
                
                try:
                    # Fetch article content
                    content, http_code, error = await client.get(url)
                    
                    if error or not content:
                        self.backoff.record_failure(site.domain, http_code)
                        return False
                    
                    # Validate content
                    is_valid, rejection_reason = self.validator.validate(url, content)
                    if not is_valid:
                        logger.warning(
                            f"Article validation failed: {rejection_reason}",
                            extra={"url": url, "site": site.name}
                        )
                        return False
                    
                    # Extract article details (metadata + content)
                    extracted = self.extractor.extract(url, content, site.name)
                    
                    # Detect sport category
                    site_type_lower = (site.site_type or "").lower()
                    if site_type_lower == "specific" and site.sport_focus:
                        category = site.sport_focus
                    else:
                        category = self.category_detector.detect(
                            url, extracted.title, extracted.content, site
                        )
                    
                    # Create ArticleLink object with full content
                    link = ArticleLink(
                        site_id=site.id,
                        url=extracted.url,
                        url_hash=url_hash(extracted.url),
                        title=extracted.title,
                        author=extracted.author,
                        content=extracted.content, # Saving full content
                        sport_category=category,
                        last_modified=url_info.get("lastmod"),
                        published_at=extracted.publish_date or url_info.get("news_publication_date") or url_info.get("lastmod"),
                        source_site=extracted.source_site,
                        # first_seen_at handled by DB
                    )
                    
                    # Save to article_links
                    saved = self.repo.save_article_link(link)
                    
                    # Trigger analysis pipeline
                    # TriggerService likely expects 'id' and other fields which ArticleLink now has
                    await self.trigger_service.trigger_analysis(saved)
                    
                    self.backoff.record_success(site.domain)
                    return True
                    
                except Exception as e:
                    logger.error(f"Article processing failed: {e}", extra={"url": url})
                    self.backoff.record_failure(site.domain)
                    return False
        
        # Process all articles in parallel
        async with HttpClient(self._session) as client:
            tasks = [process_single_article(url_info, client) for url_info in valid_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes
            success_count = sum(1 for r in results if r is True)
            failed_count = len(valid_urls) - success_count
            
            logger.info(
                f"Processed {success_count}/{len(valid_urls)} articles successfully",
                extra={"site": site.name, "failed": failed_count}
            )
            return success_count, failed_count

    
    async def run_once(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Run a single crawl cycle for all sites.
        Useful for testing.
        
        Args:
            dry_run: If True, don't save to database
            
        Returns:
            Statistics dict
        """
        results = {
            "sites_processed": 0,
            "urls_found": 0,
            "articles_saved": 0,
            "errors": []
        }
        
        async with aiohttp.ClientSession() as session:
            self._session = session
            
            # Get active sites from database
            sites = self.repo.get_active_sites()
            
            for site in sites:
                try:
                    tracker = UrlTracker(session)
                    # Find URLs from last 7 days from sitemap
                    discovered_urls = await tracker.find_recent_urls(site, days=7)
                    
                    results["sites_processed"] += 1
                    results["urls_found"] += len(discovered_urls)
                    
                    if not dry_run and discovered_urls:
                        # Record discovered URLs
                        tracker.record_new_urls(site, discovered_urls)
                        
                        # Filter for processing (last 2 days)
                        urls_to_process = []
                        for u in discovered_urls:
                            lastmod = u.get("lastmod")
                            if not lastmod or tracker.is_within_days(lastmod, days=2):
                                urls_to_process.append(u)
                        
                        if urls_to_process:
                             start_saved = results["articles_saved"]
                             saved, failed = await self._process_new_articles(site, urls_to_process)
                             results["articles_saved"] += saved
                             
                             logger.info(
                                f"Site processed: {len(discovered_urls)} URLs found, {saved} saved",
                                extra={"site": site.name}
                             )
                        else:
                             logger.info(
                                f"Site processed: {len(discovered_urls)} URLs found (0 processed)",
                                extra={"site": site.name}
                             )
                    
                except Exception as e:
                    results["errors"].append({
                        "site": site.name,
                        "error": str(e)
                    })
        
        self._session = None
        return results
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None
            })
        
        return {
            "running": self._running,
            "jobs": jobs,
            "blocked_sites": self.backoff.get_all_blocked()
        }

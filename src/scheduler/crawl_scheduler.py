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
        1. Fetch sitemap and find new URLs
        2. Crawl each new URL
        3. Extract and store articles
        """
        logger.info(f"Starting crawl cycle", extra={"site": site.name})
        
        # Check if site is blocked
        if self.backoff.is_blocked(site.domain):
            logger.warning(f"Site blocked, skipping", extra={"site": site.name})
            return
        
        try:
            # Phase 1: Find URLs from last N days from sitemap
            # Use configured days_to_crawl + a buffer (e.g. 5 days) for discovery to ensure we don't miss anything
            # The stricter filtering happens before processing
            discovery_days = self.config.days_to_crawl + 5
            tracker = UrlTracker(self._session)
            discovered_urls = await tracker.find_recent_urls(site, days=discovery_days)
            
            # Log sitemap check
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="sitemap",
                status="success",
                http_code=200,
                urls_found=len(discovered_urls) + len(tracker.repo.get_known_urls_batch([u["url"] for u in discovered_urls])),
                new_urls=len(discovered_urls)
            )
            
            if not discovered_urls:
                logger.info(f"No new URLs found in last 7 days", extra={"site": site.name})
                return
            
            # Record discovered URLs in database
            tracker.record_new_urls(site, discovered_urls)
            
            # Filter for processing: "save all the articles of last 2 days"
            # We also include URLs with no date to be safe
            urls_to_process = []
            for u in discovered_urls:
                lastmod = u.get("lastmod")
                # CRITICAL CHANGE: Use discovery_days (7 days) instead of strict config limit (2 days)
                # This ensures we catch up on ANY pending articles found in the discovery window
                if not lastmod or tracker.is_within_days(lastmod, days=discovery_days):
                    urls_to_process.append(u)
            
            logger.info(
                f"Queuing {len(urls_to_process)} URLs for processing (last {discovery_days} days backfill)",
                extra={"site": site.name, "total_discovered_7d": len(discovered_urls)}
            )
            
            # Phase 2: Crawl and process recent/new articles
            saved_count = 0
            failed_count = 0
            
            if urls_to_process:
                saved_count, failed_count = await self._process_new_articles(site, urls_to_process)
            
            # Phase 3: Retry previously discovered URLs that weren't saved as articles
            # These are URLs that made it to discovered_urls but failed during article processing
            retry_saved = 0
            retry_failed = 0
            
            unprocessed_urls = self.repo.get_unprocessed_discovered_urls(site.id, limit=50)
            # Filter to recent dates only
            retry_urls = []
            for u in unprocessed_urls:
                lastmod = u.get("lastmod")
                if not lastmod or tracker.is_within_days(lastmod, days=self.config.days_to_crawl):
                    retry_urls.append(u)
            
            if retry_urls:
                logger.info(
                    f"Retrying {len(retry_urls)} previously failed URLs",
                    extra={"site": site.name}
                )
                retry_saved, retry_failed = await self._process_new_articles(site, retry_urls)
            
            # Combine stats
            total_saved = saved_count + retry_saved
            total_failed = failed_count + retry_failed
            
            # Log final status with details
            status_msg = "success"
            error_details = None
            
            if total_failed > 0:
                # If we had failures but also successes, still mark as success but log warning
                # If ALL failed, maybe mark as warning/failed?
                # User wants to know why things match.
                error_details = f"Saved: {total_saved}, Failed: {total_failed}"
                if total_saved == 0 and (len(urls_to_process) + len(retry_urls)) > 0:
                    status_msg = "partial_failure" # Or keep success but rely on error_details
            
            # Update log with processing stats (upsert or update last log?)
            # The current log implementation creates a NEW log entry.
            # We already logged "sitemap" success at line 147. 
            # We should probably log a separate "article" crawl log OR update the previous one?
            # The repository `log_crawl` creates a new entry.
            # Let's log an "article" stage log.
            
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="article",
                status=status_msg,
                http_code=200,
                urls_found=len(urls_to_process) + len(retry_urls), # URLs attempted
                new_urls=total_saved,            # URLs saved
                error_message=error_details
            )
            
            # Reset backoff on success
            self.backoff.record_success(site.domain)
            
        except Exception as e:
            logger.error(f"Crawl cycle failed: {e}", extra={"site": site.name})
            
            should_retry, wait_time = self.backoff.record_failure(site.domain)
            
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="sitemap",
                status="failed",
                error_message=str(e)
            )
    
    async def _process_new_articles(self, site: Site, new_urls: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process and store new articles in parallel.
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
        
        async def process_single_article(url_info: Dict[str, Any], client: HttpClient) -> bool:
            """Process a single article. Returns True if saved successfully."""
            async with semaphore:
                url = url_info["url"]
                
                # Check backoff
                if self.backoff.is_blocked(site.domain):
                    return False
                
                try:
                    # Fetch article
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
                    
                    # Extract article
                    extracted = self.extractor.extract(url, content, site.name)
                    
                    # Detect sport category
                    # Use case-insensitive comparison for site_type (DB might store 'Specific' or 'specific')
                    site_type_lower = (site.site_type or "").lower()
                    if site_type_lower == "specific" and site.sport_focus:
                        category = site.sport_focus
                    else:
                        category = self.category_detector.detect(
                            url, extracted.title, extracted.content, None
                        )
                    extracted.sport_category = category
                    
                    # Save to database
                    article = Article(
                        url=extracted.url,
                        title=extracted.title,
                        author=extracted.author,
                        publish_date=extracted.publish_date,
                        content=extracted.content,
                        sport_category=extracted.sport_category,
                        source_site=extracted.source_site,
                        ready_for_analysis=True
                    )
                    
                    saved = self.repo.save_article(article)
                    
                    # Trigger analysis pipeline
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

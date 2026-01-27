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
        
        # Start scheduler
        self.scheduler.start()
        self._running = True
        
        logger.info(f"Scheduler started with {len(sites)} sites")
    
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
            # Phase 1: Find new URLs from sitemap
            tracker = UrlTracker(self._session)
            new_urls = await tracker.find_recent_urls(site, days=2)  # Only last 2 days
            
            # Log sitemap check
            self.repo.log_crawl(
                site_id=site.id,
                crawl_type="sitemap",
                status="success",
                http_code=200,
                urls_found=len(new_urls) + len(tracker.repo.get_known_urls_batch([u["url"] for u in new_urls])),
                new_urls=len(new_urls)
            )
            
            if not new_urls:
                logger.info(f"No new URLs found", extra={"site": site.name})
                return
            
            # Record new URLs in database
            tracker.record_new_urls(site, new_urls)
            
            # Phase 2: Crawl and process each new article
            await self._process_new_articles(site, new_urls)
            
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
    
    async def _process_new_articles(self, site: Site, new_urls: List[Dict[str, Any]]):
        """Process and store new articles in parallel for speed."""
        # Filter valid URLs first
        valid_urls = [
            url_info for url_info in new_urls
            if self.validator.quick_validate_url(url_info["url"])
        ]
        
        if not valid_urls:
            return
        
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
                        return False
                    
                    # Extract article
                    extracted = self.extractor.extract(url, content, site.name)
                    
                    # Detect sport category
                    if site.site_type == "specific" and site.sport_focus:
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
            logger.info(
                f"Processed {success_count}/{len(valid_urls)} articles successfully",
                extra={"site": site.name}
            )

    
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
                    new_urls = await tracker.find_recent_urls(site, days=2)  # Only last 2 days
                    
                    results["sites_processed"] += 1
                    results["urls_found"] += len(new_urls)
                    
                    if not dry_run and new_urls:
                        tracker.record_new_urls(site, new_urls)
                        await self._process_new_articles(site, new_urls)
                        # Note: articles_saved would need counting from process
                    
                    logger.info(
                        f"Site processed: {len(new_urls)} new URLs",
                        extra={"site": site.name, "new_urls": len(new_urls)}
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

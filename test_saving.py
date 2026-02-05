
import asyncio
import logging
import sys

from src.logging_config import setup_logging, get_logger
from src.scheduler.crawl_scheduler import CrawlScheduler
from src.database.repository import get_repository

async def test_crawl():
    # Setup logging to see what's happening
    setup_logging(level="INFO")
    logger = get_logger("test_saving")
    
    logger.info("Starting test crawl...")
    
    # Initialize scheduler
    scheduler = CrawlScheduler()
    
    # We need to manually initialize the session if we use internal methods, 
    # but run_once handles its own session.
    
    # Check what sites are active
    repo = get_repository()
    sites = repo.get_active_sites()
    logger.info(f"Found {len(sites)} active sites in database.")
    for s in sites:
        logger.info(f" - {s.name} ({s.domain})")
    
    if not sites:
        logger.error("No active sites found! Add sites via frontend first.")
        return

    logger.info("\nRunning crawl cycle...")
    
    # Run once
    metrics = await scheduler.run_once(dry_run=False)
    
    logger.info("\nCrawl finished.")
    logger.info(f"Metrics: {metrics}")
    
    # Check articles count in DB
    # We can invoke a direct DB check here if we want, but logs should show "Saved article"
    
if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_crawl())

"""
Sports News Crawler - Main Entry Point

Starts the scheduler and runs continuously.
"""

import asyncio
import signal
import sys

from src.config import get_config
from src.logging_config import setup_logging, get_logger
from src.scheduler import CrawlScheduler


async def main():
    """Main entry point."""
    # Setup logging
    config = get_config()
    setup_logging(level=config.log_level)
    logger = get_logger("main")
    
    logger.info("=" * 50)
    logger.info("Sports News Crawler Starting")
    logger.info("=" * 50)
    
    # Create scheduler
    scheduler = CrawlScheduler()
    
    # Graceful shutdown handler
    shutdown_event = asyncio.Event()
    
    def signal_handler(signum, frame):
        logger.info("Shutdown signal received")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start scheduler
        await scheduler.start()
        
        logger.info("Crawler running. Press Ctrl+C to stop.")
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        # Stop scheduler
        await scheduler.stop()
        logger.info("Crawler stopped")


if __name__ == "__main__":
    if sys.platform == "win32":
        # Windows-specific event loop policy
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())

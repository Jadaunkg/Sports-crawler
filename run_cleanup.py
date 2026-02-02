import asyncio
import sys
import argparse
from src.database.repository import get_repository
from src.logging_config import setup_logging, get_logger

logger = get_logger("cleanup_script")

async def run_cleanup(days: int = 2):
    """Run manual cleanup of old articles."""
    setup_logging(level="INFO")
    repo = get_repository()
    
    logger.info(f"Starting cleanup of articles older than {days} days...")
    
    try:
        count = repo.cleanup_old_articles(days=days)
        logger.info(f"Cleanup finished. Removed {count} articles.")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup old articles from database.")
    parser.add_argument("--days", type=int, default=2, help="Delete articles older than N days (default: 2)")
    
    args = parser.parse_args()
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_cleanup(days=args.days))

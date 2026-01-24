"""
Sports News Crawler - CLI

Command-line interface for crawler operations.
"""

import asyncio
import argparse
import sys
import json

from src.config import get_config
from src.logging_config import setup_logging, get_logger
from src.scheduler import CrawlScheduler
from src.database.repository import get_repository


def run_once(args):
    """Run a single crawl cycle."""
    setup_logging(level="INFO")
    logger = get_logger("cli")
    
    async def _run():
        scheduler = CrawlScheduler()
        results = await scheduler.run_once(dry_run=args.dry_run)
        
        print("\n" + "=" * 50)
        print("CRAWL RESULTS")
        print("=" * 50)
        print(f"Sites processed: {results['sites_processed']}")
        print(f"New URLs found:  {results['urls_found']}")
        print(f"Articles saved:  {results['articles_saved']}")
        
        if results['errors']:
            print("\nErrors:")
            for err in results['errors']:
                print(f"  - {err['site']}: {err['error']}")
        
        print("=" * 50)
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(_run())


def status(args):
    """Show crawler status."""
    setup_logging(level="WARNING")
    
    scheduler = CrawlScheduler()
    status = scheduler.get_status()
    
    print("\n" + "=" * 50)
    print("CRAWLER STATUS")
    print("=" * 50)
    print(f"Running: {status['running']}")
    
    print("\nScheduled Jobs:")
    if status['jobs']:
        for job in status['jobs']:
            print(f"  - {job['id']}: next run at {job['next_run']}")
    else:
        print("  (none)")
    
    print("\nBlocked Sites:")
    if status['blocked_sites']:
        for site in status['blocked_sites']:
            print(f"  - {site}")
    else:
        print("  (none)")
    
    print("=" * 50)


def show_sites(args):
    """Show configured sites."""
    setup_logging(level="WARNING")
    config = get_config()
    
    print("\n" + "=" * 50)
    print("CONFIGURED SITES")
    print("=" * 50)
    
    for site in config.sites:
        status = "✓ Active" if site.is_active else "✗ Inactive"
        print(f"\n{site.name} ({status})")
        print(f"  Domain:    {site.domain}")
        print(f"  Sitemap:   {site.sitemap_url}")
        print(f"  Interval:  {site.crawl_interval_minutes} minutes")
    
    print("\n" + "=" * 50)


def init_db(args):
    """Initialize database tables."""
    from src.database.init_tables import init_tables
    init_tables()


def main():
    """CLI main entry point."""
    parser = argparse.ArgumentParser(
        description="Sports News Crawler CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # run-once command
    run_once_parser = subparsers.add_parser(
        "run-once",
        help="Run a single crawl cycle"
    )
    run_once_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save to database, just check sitemaps"
    )
    run_once_parser.set_defaults(func=run_once)
    
    # status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show crawler status"
    )
    status_parser.set_defaults(func=status)
    
    # sites command
    sites_parser = subparsers.add_parser(
        "sites",
        help="Show configured sites"
    )
    sites_parser.set_defaults(func=show_sites)
    
    # init-db command
    init_parser = subparsers.add_parser(
        "init-db",
        help="Show database initialization SQL"
    )
    init_parser.set_defaults(func=init_db)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()

"""
Fresh crawl test - finds NEW articles from sitemaps and tests if they get saved.
This ignores all existing articles and looks for brand new ones.
"""
import asyncio
import sys
import aiohttp
from datetime import datetime, timezone

from src.config import get_config
from src.logging_config import setup_logging, get_logger
from src.database.repository import get_repository, Site, Article
from src.sitemap.tracker import UrlTracker
from src.crawler.http_client import HttpClient
from src.article.validator import ArticleValidator
from src.article.extractor import ArticleExtractor
from src.article.category_detector import CategoryDetector


async def fresh_crawl_test():
    """Run a fresh crawl and report exactly what was found and saved."""
    setup_logging(level="INFO")
    logger = get_logger("fresh_crawl")
    
    config = get_config()
    repo = get_repository()
    validator = ArticleValidator()
    extractor = ArticleExtractor()
    category_detector = CategoryDetector()
    
    print("\n" + "="*60)
    print("FRESH CRAWL TEST")
    print("="*60)
    print(f"Time: {datetime.now().isoformat()}")
    
    # Get starting count
    before_count = len(repo.db.table('articles').select('id').execute().data)
    print(f"Articles before crawl: {before_count}")
    
    sites = repo.get_active_sites()
    print(f"Active sites: {len(sites)}")
    
    total_stats = {
        'urls_needing_articles': 0,
        'urls_within_2_days': 0,
        'urls_attempted': 0,
        'urls_pattern_rejected': 0,
        'urls_fetch_failed': 0,
        'urls_validation_failed': 0,
        'articles_saved': 0,
    }
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            print(f"\n--- {site.name} ---")
            
            # Find URLs needing articles
            tracker = UrlTracker(session)
            urls_needing_articles = await tracker.find_recent_urls(site, days=7)
            
            total_stats['urls_needing_articles'] += len(urls_needing_articles)
            print(f"  URLs needing articles: {len(urls_needing_articles)}")
            
            if not urls_needing_articles:
                print("  (All URLs already have articles)")
                continue
            
            # Filter to last 2 days for processing
            urls_to_process = []
            for u in urls_needing_articles:
                lastmod = u.get("lastmod")
                if not lastmod or tracker.is_within_days(lastmod, days=2):
                    urls_to_process.append(u)
            
            total_stats['urls_within_2_days'] += len(urls_to_process)
            print(f"  URLs within 2 days: {len(urls_to_process)}")
            
            if not urls_to_process:
                print("  (No recent URLs to process)")
                continue
            
            # Process articles (limit to 20 per site for this test)
            test_urls = urls_to_process[:20]
            saved_this_site = 0
            
            async with HttpClient(session) as client:
                for url_info in test_urls:
                    url = url_info["url"]
                    total_stats['urls_attempted'] += 1
                    
                    # Validate URL pattern
                    if not validator.quick_validate_url(url):
                        total_stats['urls_pattern_rejected'] += 1
                        continue
                    
                    # Fetch content
                    content, http_code, error = await client.get(url)
                    if error or not content:
                        total_stats['urls_fetch_failed'] += 1
                        continue
                    
                    # Validate content
                    is_valid, rejection_reason = validator.validate(url, content)
                    if not is_valid:
                        total_stats['urls_validation_failed'] += 1
                        continue
                    
                    # Extract and save
                    try:
                        extracted = extractor.extract(url, content, site.name)
                        
                        # Detect category
                        site_type_lower = (site.site_type or "").lower()
                        if site_type_lower == "specific" and site.sport_focus:
                            category = site.sport_focus
                        else:
                            category = category_detector.detect(url, extracted.title, extracted.content, None)
                        extracted.sport_category = category
                        
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
                        
                        saved = repo.save_article(article)
                        total_stats['articles_saved'] += 1
                        saved_this_site += 1
                        print(f"    SAVED: {extracted.title[:50]}...")
                        
                    except Exception as e:
                        print(f"    ERROR saving: {e}")
            
            print(f"  Saved from this site: {saved_this_site}")
    
    # Final counts
    after_count = len(repo.db.table('articles').select('id').execute().data)
    
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"URLs needing articles (found): {total_stats['urls_needing_articles']}")
    print(f"URLs within 2 days:            {total_stats['urls_within_2_days']}")
    print(f"URLs attempted:                {total_stats['urls_attempted']}")
    print(f"URLs pattern rejected:         {total_stats['urls_pattern_rejected']}")
    print(f"URLs fetch failed:             {total_stats['urls_fetch_failed']}")
    print(f"URLs validation failed:        {total_stats['urls_validation_failed']}")
    print(f"Articles saved:                {total_stats['articles_saved']}")
    print()
    print(f"Articles BEFORE: {before_count}")
    print(f"Articles AFTER:  {after_count}")
    print(f"NET NEW:         {after_count - before_count}")
    print("="*60)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(fresh_crawl_test())

"""
Test script to fetch TODAY's articles only.
"""

import asyncio
import sys
import aiohttp

from src.config import get_config
from src.logging_config import setup_logging, get_logger
from src.database.repository import get_repository, Site, Article
from src.sitemap.tracker import UrlTracker
from src.crawler.http_client import HttpClient
from src.article.validator import ArticleValidator
from src.article.extractor import ArticleExtractor
from src.article.category_detector import CategoryDetector


async def fetch_today_articles():
    """Fetch and store today's articles from all sites."""
    setup_logging(level="INFO")
    logger = get_logger("test_today")
    
    config = get_config()
    repo = get_repository()
    validator = ArticleValidator()
    extractor = ArticleExtractor()
    category_detector = CategoryDetector()
    
    # Sync sites to database
    for site_cfg in config.get_active_sites():
        site = Site(
            name=site_cfg.name,
            domain=site_cfg.domain,
            sitemap_url=site_cfg.sitemap_url,
            crawl_interval_minutes=site_cfg.crawl_interval_minutes,
            is_active=site_cfg.is_active
        )
        repo.upsert_site(site)
    
    sites = repo.get_active_sites()
    
    total_urls = 0
    total_articles = 0
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            print(f"\n{'='*60}")
            print(f"Processing: {site.name}")
            print(f"Sitemap: {site.sitemap_url}")
            print(f"{'='*60}")
            
            # Find today's new URLs
            tracker = UrlTracker(session)
            new_urls = await tracker.find_today_urls(site)
            
            print(f"Found {len(new_urls)} new URLs from today")
            total_urls += len(new_urls)
            
            if not new_urls:
                continue
            
            # Record URLs first
            tracker.record_new_urls(site, new_urls)
            
            # Fetch and process each article
            async with HttpClient(session) as client:
                for i, url_info in enumerate(new_urls[:20]):  # Limit to 20 per site for testing
                    url = url_info["url"]
                    print(f"\n[{i+1}/{min(len(new_urls), 20)}] Fetching: {url[:80]}...")
                    
                    # Quick URL validation
                    if not validator.quick_validate_url(url):
                        print(f"  ❌ Rejected by URL pattern")
                        continue
                    
                    # Fetch article
                    content, http_code, error = await client.get(url)
                    
                    if error or not content:
                        print(f"  ❌ Fetch failed: {error or 'No content'}")
                        continue
                    
                    # Validate content
                    is_valid, rejection_reason = validator.validate(url, content)
                    if not is_valid:
                        print(f"  ❌ Validation failed: {rejection_reason}")
                        continue
                    
                    # Extract article
                    extracted = extractor.extract(url, content, site.name)
                    
                    # Detect sport category
                    category = category_detector.detect(url, extracted.title, extracted.content)
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
                    
                    saved = repo.save_article(article)
                    total_articles += 1
                    
                    print(f"  ✅ Saved: {extracted.title[:50]}...")
                    print(f"     Category: {category}")
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total new URLs found today: {total_urls}")
    print(f"Total articles saved: {total_articles}")
    print(f"{'='*60}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(fetch_today_articles())

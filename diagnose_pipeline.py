"""
Diagnostic script to identify why articles are not being saved.
This script traces through the entire pipeline and reports exactly where failures occur.
"""

import asyncio
import sys
import aiohttp
from datetime import datetime, date
from collections import defaultdict

from src.config import get_config
from src.logging_config import setup_logging, get_logger
from src.database.repository import get_repository, Site, Article, url_hash
from src.sitemap.tracker import UrlTracker
from src.crawler.http_client import HttpClient
from src.article.validator import ArticleValidator
from src.article.extractor import ArticleExtractor
from src.article.category_detector import CategoryDetector


async def diagnose_site(session: aiohttp.ClientSession, site: Site, limit: int = 10):
    """Diagnose article saving pipeline for a single site."""
    print(f"\n{'='*70}")
    print(f"🔍 DIAGNOSING: {site.name}")
    print(f"   Domain: {site.domain}")
    print(f"   Sitemap: {site.sitemap_url}")
    print(f"   Site Type: {site.site_type}, Sport Focus: {site.sport_focus}")
    print(f"{'='*70}")
    
    config = get_config()
    repo = get_repository()
    tracker = UrlTracker(session)
    validator = ArticleValidator()
    extractor = ArticleExtractor()
    category_detector = CategoryDetector()
    
    stats = {
        'sitemap_entries_total': 0,
        'sitemap_entries_with_date': 0,
        'sitemap_entries_without_date': 0,
        'entries_within_days': 0,
        'entries_outside_days': 0,
        'already_in_discovered_urls': 0,
        'new_urls_found': 0,
        'url_pattern_rejected': 0,
        'fetch_failed': 0,
        'validation_failed': 0,
        'extraction_succeeded': 0,
        'save_attempted': 0,
        'save_succeeded': 0,
        'already_in_articles': 0,
    }
    
    failure_reasons = defaultdict(list)

    # Step 1: Fetch sitemap entries
    print("\n📄 Step 1: Fetching sitemap entries...")
    try:
        entries = await tracker.get_all_sitemap_urls(site.sitemap_url)
        stats['sitemap_entries_total'] = len(entries)
        print(f"   Total entries in sitemap: {len(entries)}")
    except Exception as e:
        print(f"   ❌ FAILED to fetch sitemap: {e}")
        return stats, failure_reasons
    
    if not entries:
        print("   ⚠️ No entries found in sitemap!")
        return stats, failure_reasons
    
    # Step 2: Analyze dates
    print("\n📅 Step 2: Analyzing dates...")
    today = date.today()
    days_to_crawl = config.days_to_crawl
    
    for entry in entries:
        date_str = entry.lastmod or entry.news_publication_date
        if date_str:
            stats['sitemap_entries_with_date'] += 1
            if tracker.is_within_days(date_str, days_to_crawl):
                stats['entries_within_days'] += 1
            else:
                stats['entries_outside_days'] += 1
        else:
            stats['sitemap_entries_without_date'] += 1
    
    print(f"   Entries with date: {stats['sitemap_entries_with_date']}")
    print(f"   Entries without date: {stats['sitemap_entries_without_date']}")
    print(f"   Entries within {days_to_crawl} days: {stats['entries_within_days']}")
    print(f"   Entries outside {days_to_crawl} days: {stats['entries_outside_days']}")
    
    # Step 3: Check known URLs
    print("\n🔎 Step 3: Checking for already discovered URLs...")
    all_urls = [e.loc for e in entries]
    
    # Option A: What tracker.find_recent_urls would filter to
    recent_entries = [e for e in entries if tracker.is_within_days(e.lastmod or e.news_publication_date, days_to_crawl + 5)]
    if not recent_entries:
        # If no dated entries, take sample
        no_date_entries = [e for e in entries if not (e.lastmod or e.news_publication_date)]
        recent_entries = no_date_entries[-100:] if len(no_date_entries) > 100 else no_date_entries
    
    all_recent_urls = [e.loc for e in recent_entries]
    known_urls = repo.get_known_urls_batch(all_recent_urls)
    
    stats['already_in_discovered_urls'] = len(known_urls)
    stats['new_urls_found'] = len(all_recent_urls) - len(known_urls)
    
    print(f"   Recent entries for checking: {len(all_recent_urls)}")
    print(f"   Already in discovered_urls: {stats['already_in_discovered_urls']}")
    print(f"   New URLs (not in discovered_urls): {stats['new_urls_found']}")
    
    # Step 4: Check articles table
    print("\n📰 Step 4: Checking articles table...")
    sample_urls = all_recent_urls[:min(50, len(all_recent_urls))]
    article_hashes = set()
    for batch_start in range(0, len(sample_urls), 100):
        batch = [url_hash(u) for u in sample_urls[batch_start:batch_start+100]]
        result = repo.db.table("articles").select("url_hash").in_("url_hash", batch).execute()
        article_hashes.update(row["url_hash"] for row in result.data)
    
    stats['already_in_articles'] = len(article_hashes)
    print(f"   Checked {len(sample_urls)} sample URLs")
    print(f"   Already in articles table: {stats['already_in_articles']}")
    
    # Step 5: Get unprocessed URLs (in discovered but not in articles)
    print("\n🔄 Step 5: Finding unprocessed discovered URLs...")
    unprocessed = repo.get_unprocessed_discovered_urls(site.id, limit=50)
    print(f"   Unprocessed URLs found: {len(unprocessed)}")
    
    # Step 6: Test article processing on sample URLs
    print(f"\n🧪 Step 6: Testing article processing on {limit} URLs...")
    
    # Get new URLs that aren't in discovered_urls yet
    new_url_entries = [e for e in recent_entries if e.loc not in known_urls]
    test_urls = [{"url": e.loc, "lastmod": e.lastmod or e.news_publication_date} for e in new_url_entries[:limit]]
    
    # If no new URLs, test with unprocessed URLs
    if not test_urls and unprocessed:
        test_urls = unprocessed[:limit]
        print(f"   (Using {len(test_urls)} unprocessed URLs instead)")
    
    if not test_urls:
        print("   ⚠️ No URLs available to test!")
        return stats, failure_reasons
    
    async with HttpClient(session) as client:
        for i, url_info in enumerate(test_urls):
            url = url_info["url"]
            print(f"\n   [{i+1}/{len(test_urls)}] Testing: {url[:70]}...")
            
            # Check URL pattern
            if not validator.quick_validate_url(url):
                stats['url_pattern_rejected'] += 1
                failure_reasons['url_pattern'].append(url)
                print(f"      ❌ Rejected by URL pattern")
                continue
            
            # Fetch content
            content, http_code, error = await client.get(url)
            if error or not content:
                stats['fetch_failed'] += 1
                failure_reasons['fetch_failed'].append(f"{url}: {error or 'No content'} (HTTP {http_code})")
                print(f"      ❌ Fetch failed: {error or 'No content'} (HTTP {http_code})")
                continue
            
            # Validate content
            is_valid, rejection_reason = validator.validate(url, content)
            if not is_valid:
                stats['validation_failed'] += 1
                failure_reasons['validation_failed'].append(f"{url}: {rejection_reason}")
                print(f"      ❌ Validation failed: {rejection_reason}")
                continue
            
            # Extract article
            try:
                extracted = extractor.extract(url, content, site.name)
                stats['extraction_succeeded'] += 1
                
                # Detect category
                site_type_lower = (site.site_type or "").lower()
                if site_type_lower == "specific" and site.sport_focus:
                    category = site.sport_focus
                else:
                    category = category_detector.detect(url, extracted.title, extracted.content, None)
                
                extracted.sport_category = category
                
                print(f"      ✅ Extracted: {extracted.title[:50]}...")
                print(f"         Category: {category}")
                print(f"         Author: {extracted.author}")
                print(f"         Publish Date: {extracted.publish_date}")
                print(f"         Content length: {len(extracted.content)} chars")
                
            except Exception as e:
                failure_reasons['extraction_failed'].append(f"{url}: {e}")
                print(f"      ❌ Extraction failed: {e}")
                continue
            
            # Try to save
            stats['save_attempted'] += 1
            try:
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
                stats['save_succeeded'] += 1
                print(f"      ✅ SAVED to database! ID: {saved.id}")
                
            except Exception as e:
                failure_reasons['save_failed'].append(f"{url}: {e}")
                print(f"      ❌ Save failed: {e}")
    
    return stats, failure_reasons


async def main():
    """Run diagnostic on all active sites."""
    setup_logging(level="WARNING")  # Reduce noise
    logger = get_logger("diagnose")
    
    print("\n" + "="*70)
    print("🔬 ARTICLE SAVING PIPELINE DIAGNOSTIC")
    print("="*70)
    print(f"Time: {datetime.now().isoformat()}")
    
    repo = get_repository()
    sites = repo.get_active_sites()
    
    print(f"\nFound {len(sites)} active sites:")
    for s in sites:
        print(f"  - {s.name} ({s.domain})")
    
    # Get counts from database
    discovered_count = repo.db.table("discovered_urls").select("id", count="exact").execute()
    articles_count = repo.db.table("articles").select("id", count="exact").execute()
    
    print(f"\nDatabase status:")
    print(f"  Total discovered URLs: {discovered_count.count if hasattr(discovered_count, 'count') else 'Unknown'}")
    print(f"  Total articles: {articles_count.count if hasattr(articles_count, 'count') else 'Unknown'}")
    
    all_stats = {}
    all_failures = {}
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            stats, failures = await diagnose_site(session, site, limit=5)
            all_stats[site.name] = stats
            all_failures[site.name] = failures
    
    # Summary
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    
    for site_name, stats in all_stats.items():
        print(f"\n{site_name}:")
        print(f"  Sitemap entries: {stats['sitemap_entries_total']}")
        print(f"  New URLs found: {stats['new_urls_found']}")
        print(f"  URL pattern rejected: {stats['url_pattern_rejected']}")
        print(f"  Fetch failed: {stats['fetch_failed']}")
        print(f"  Validation failed: {stats['validation_failed']}")
        print(f"  Extraction succeeded: {stats['extraction_succeeded']}")
        print(f"  Save attempted: {stats['save_attempted']}")
        print(f"  Save succeeded: {stats['save_succeeded']}")
    
    # Identify issues
    print("\n" + "="*70)
    print("🐛 IDENTIFIED ISSUES")
    print("="*70)
    
    for site_name, failures in all_failures.items():
        if any(failures.values()):
            print(f"\n{site_name}:")
            for reason, urls in failures.items():
                if urls:
                    print(f"  {reason} ({len(urls)} URLs):")
                    for url in urls[:3]:
                        print(f"    - {url[:80]}...")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())

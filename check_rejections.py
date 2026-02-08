"""Check which specific URLs are being rejected."""
import asyncio
import sys
import aiohttp
from src.database.repository import get_repository
from src.sitemap.tracker import UrlTracker
from src.article.validator import ArticleValidator

async def check():
    repo = get_repository()
    validator = ArticleValidator()
    sites = repo.get_active_sites()
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            print(f"\n=== {site.name} ===")
            tracker = UrlTracker(session)
            urls = await tracker.find_recent_urls(site, days=7)
            
            # Filter to 2 days
            recent = [u for u in urls if not u.get("lastmod") or tracker.is_within_days(u["lastmod"], 2)]
            print(f"URLs within 2 days: {len(recent)}")
            
            for u in recent[:5]:
                url = u["url"]
                is_valid = validator.quick_validate_url(url)
                status = "PASS" if is_valid else "REJECTED"
                print(f"  {status}: {url[:80]}")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(check())

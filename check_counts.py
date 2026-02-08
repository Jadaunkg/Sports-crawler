"""Quick check of article counts and recent saves."""
from src.database.repository import get_repository
from datetime import datetime, timedelta, timezone

repo = get_repository()

# Count articles added in last 2 hours
cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()

recent = repo.db.table('articles').select('source_site, title, crawl_time').gt('crawl_time', cutoff).execute()

print('=== ARTICLES SAVED IN LAST 2 HOURS ===')
print(f'Count: {len(recent.data)}')
for a in recent.data[:15]:
    site = a["source_site"]
    title = a["title"][:60] if a["title"] else "No title"
    print(f'  {site}: {title}...')

# Total counts by site
result = repo.db.table('articles').select('source_site').execute()
sites = {}
for row in result.data:
    site = row['source_site']
    sites[site] = sites.get(site, 0) + 1

print('\n=== TOTAL ARTICLES BY SITE ===')
for site, count in sorted(sites.items(), key=lambda x: -x[1]):
    print(f'{site}: {count}')

print(f'\nGRAND TOTAL: {len(result.data)} articles')

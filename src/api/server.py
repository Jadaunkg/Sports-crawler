"""
FastAPI server for the Sports Crawler dashboard.
Provides API endpoints for site management and crawl control.
"""

import asyncio
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import aiohttp

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

from src.config import get_config, reload_config
from src.database.repository import get_repository, Site, Article
from src.sitemap.tracker import UrlTracker
from src.crawler.http_client import HttpClient
from src.crawler.backoff import BackoffManager
from src.article.validator import ArticleValidator
from src.article.extractor import ArticleExtractor
from src.article.category_detector import CategoryDetector
from src.logging_config import setup_logging, get_logger

logger = get_logger("api.server")

# Global state
crawl_status = {
    "is_running": False,
    "current_site": None,
    "progress": 0,
    "total": 0,
    "articles_saved": 0,
    "errors": [],
    "last_run": None
}


# Pydantic models
class SiteCreate(BaseModel):
    name: str
    domain: str
    sitemap_url: str
    site_type: str = "specific"  # specific or general
    sport_focus: Optional[str] = None
    crawl_interval_minutes: int = 15


class SiteResponse(BaseModel):
    id: str
    name: str
    domain: str
    sitemap_url: str
    site_type: Optional[str] = "specific"
    sport_focus: Optional[str] = None
    crawl_interval_minutes: int
    is_active: bool


class ArticleResponse(BaseModel):
    id: str
    url: str
    title: str
    author: Optional[str]
    publish_date: Optional[str]
    sport_category: Optional[str]
    source_site: str
    crawl_time: Optional[str]


class CrawlRequest(BaseModel):
    site_ids: Optional[List[str]] = None  # None = all sites
    days: int = 3


class CrawlStatus(BaseModel):
    is_running: bool
    current_site: Optional[str]
    progress: int
    total: int
    articles_saved: int
    errors: List[str]
    last_run: Optional[str]


# Auto-scheduler state
scheduler_state = {
    "last_auto_crawl": None,
    "auto_crawl_interval_minutes": 15,
    "days_to_crawl": 2
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events with auto-scheduler."""
    setup_logging(level="INFO")
    logger.info("API server starting")
    
    # Start background scheduler
    scheduler_task = asyncio.create_task(auto_scheduler_loop())
    
    yield
    
    # Cleanup
    scheduler_task.cancel()
    logger.info("API server stopping")


async def auto_scheduler_loop():
    """Background loop that runs auto-crawl every 15 minutes."""
    global scheduler_state
    
    # Wait 30 seconds after startup before first run
    await asyncio.sleep(30)
    
    while True:
        try:
            logger.info("Auto-scheduler: Starting scheduled crawl")
            await run_auto_crawl()
            scheduler_state["last_auto_crawl"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.error(f"Auto-scheduler error: {e}")
        
        # Wait for next interval
        await asyncio.sleep(scheduler_state["auto_crawl_interval_minutes"] * 60)


async def run_auto_crawl():
    """Run automatic crawl for all active sites (last 2 days)."""
    global crawl_status
    
    if crawl_status["is_running"]:
        logger.info("Auto-scheduler: Crawl already in progress, skipping")
        return
    
    # Run crawl with 2-day lookback
    await run_parallel_crawl(None, days=scheduler_state["days_to_crawl"])


app = FastAPI(
    title="Sports Crawler API",
    description="API for managing sports news crawler",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== HEALTH & SCHEDULER ENDPOINTS ====================

@app.get("/health")
async def health_check():
    """Health check endpoint for Render and GitHub Actions pings."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "crawler_running": crawl_status["is_running"],
        "last_auto_crawl": scheduler_state["last_auto_crawl"]
    }


@app.post("/api/scheduler/trigger")
async def trigger_scheduler(background_tasks: BackgroundTasks):
    """Manually trigger the auto-scheduler (called by GitHub Actions)."""
    global crawl_status
    
    if crawl_status["is_running"]:
        return {"status": "skipped", "message": "Crawl already in progress"}
    
    background_tasks.add_task(run_auto_crawl)
    return {"status": "triggered", "message": "Auto crawl started"}


@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get auto-scheduler status."""
    return {
        "interval_minutes": scheduler_state["auto_crawl_interval_minutes"],
        "days_to_crawl": scheduler_state["days_to_crawl"],
        "last_run": scheduler_state["last_auto_crawl"],
        "crawler_running": crawl_status["is_running"]
    }


# ==================== SITE ENDPOINTS ====================

@app.get("/api/sites", response_model=List[SiteResponse])
async def get_sites():
    """Get all configured sites."""
    repo = get_repository()
    sites = repo.get_all_sites()
    return [
        SiteResponse(
            id=str(s.id),
            name=s.name,
            domain=s.domain,
            sitemap_url=s.sitemap_url,
            site_type=getattr(s, 'site_type', 'specific'),
            sport_focus=getattr(s, 'sport_focus', None),
            crawl_interval_minutes=s.crawl_interval_minutes,
            is_active=s.is_active
        )
        for s in sites
    ]


@app.post("/api/sites", response_model=SiteResponse)
async def add_site(site: SiteCreate):
    """Add a new site to crawl."""
    repo = get_repository()
    
    # Check if domain already exists
    existing = repo.get_site_by_domain(site.domain)
    if existing:
        raise HTTPException(status_code=400, detail="Site with this domain already exists")
    
    new_site = Site(
        name=site.name,
        domain=site.domain,
        sitemap_url=site.sitemap_url,
        crawl_interval_minutes=site.crawl_interval_minutes,
        is_active=True
    )
    
    saved = repo.upsert_site(new_site)
    
    return SiteResponse(
        id=str(saved.id),
        name=saved.name,
        domain=saved.domain,
        sitemap_url=saved.sitemap_url,
        site_type=site.site_type,
        sport_focus=site.sport_focus,
        crawl_interval_minutes=saved.crawl_interval_minutes,
        is_active=saved.is_active
    )


@app.delete("/api/sites/{site_id}")
async def delete_site(site_id: str):
    """Delete a site."""
    repo = get_repository()
    try:
        repo.db.table("sites").delete().eq("id", site_id).execute()
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/sites/{site_id}/toggle")
async def toggle_site(site_id: str):
    """Toggle site active status."""
    repo = get_repository()
    try:
        # Get current status
        result = repo.db.table("sites").select("is_active").eq("id", site_id).execute()
        if not result.data:
            raise HTTPException(status_code=404, detail="Site not found")
        
        current = result.data[0]["is_active"]
        
        # Toggle
        repo.db.table("sites").update({"is_active": not current}).eq("id", site_id).execute()
        
        return {"status": "toggled", "is_active": not current}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ARTICLES ENDPOINTS ====================

@app.get("/api/articles", response_model=List[ArticleResponse])
async def get_articles(
    limit: int = 50,
    offset: int = 0,
    source: Optional[str] = None,
    category: Optional[str] = None
):
    """Get articles with optional filtering."""
    repo = get_repository()
    
    query = repo.db.table("articles").select("*").order("crawl_time", desc=True)
    
    if source:
        query = query.eq("source_site", source)
    if category:
        query = query.eq("sport_category", category)
    
    result = query.range(offset, offset + limit - 1).execute()
    
    return [
        ArticleResponse(
            id=str(a["id"]),
            url=a["url"],
            title=a["title"],
            author=a.get("author"),
            publish_date=a.get("publish_date"),
            sport_category=a.get("sport_category"),
            source_site=a["source_site"],
            crawl_time=a.get("crawl_time")
        )
        for a in result.data
    ]


@app.get("/api/articles/count")
async def get_article_count():
    """Get total article count."""
    repo = get_repository()
    result = repo.db.table("articles").select("id", count="exact").execute()
    return {"count": result.count}


# ==================== CRAWL ENDPOINTS ====================

@app.get("/api/crawl/status", response_model=CrawlStatus)
async def get_crawl_status():
    """Get current crawl status."""
    return CrawlStatus(**crawl_status)


@app.post("/api/crawl/start")
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """Start a crawl job in the background."""
    global crawl_status
    
    if crawl_status["is_running"]:
        raise HTTPException(status_code=400, detail="Crawl already in progress")
    
    # Start crawl in background
    background_tasks.add_task(run_parallel_crawl, request.site_ids, request.days)
    
    return {"status": "started", "message": "Crawl started in background"}


@app.post("/api/crawl/stop")
async def stop_crawl():
    """Stop the current crawl."""
    global crawl_status
    crawl_status["is_running"] = False
    return {"status": "stopping"}


async def run_parallel_crawl(site_ids: Optional[List[str]], days: int):
    """
    Run parallel crawl for specified sites.
    Uses asyncio.gather for parallel article fetching.
    """
    global crawl_status
    
    crawl_status = {
        "is_running": True,
        "current_site": "Initializing...",
        "progress": 0,
        "total": 0,
        "articles_saved": 0,
        "errors": [],
        "last_run": datetime.now(timezone.utc).isoformat()
    }
    
    repo = get_repository()
    validator = ArticleValidator()
    extractor = ArticleExtractor()
    category_detector = CategoryDetector()
    backoff = BackoffManager()
    
    # Get sites
    if site_ids:
        sites = [s for s in repo.get_all_sites() if str(s.id) in site_ids]
    else:
        sites = repo.get_active_sites()
    
    try:
        async with aiohttp.ClientSession() as session:
            for site in sites:
                if not crawl_status["is_running"]:
                    break
                
                crawl_status["current_site"] = site.name
                logger.info(f"Crawling site: {site.name}")
                
                # Get recent URLs
                tracker = UrlTracker(session)
                new_urls = await tracker.find_recent_urls(site, days=days)
                
                if not new_urls:
                    continue
                
                # Record URLs
                tracker.record_new_urls(site, new_urls)
                
                crawl_status["total"] += len(new_urls)
                
                # Get site type for categorization decision
                site_type = getattr(site, 'site_type', 'specific')
                sport_focus = getattr(site, 'sport_focus', None)
                
                # Process articles in parallel batches - high concurrency for speed
                async with HttpClient(session, use_delays=False) as client:
                    # Process in batches of 20 for faster crawling
                    batch_size = 20
                    
                    for i in range(0, len(new_urls), batch_size):
                        if not crawl_status["is_running"]:
                            break
                        
                        batch = new_urls[i:i + batch_size]
                        
                        # Create tasks for parallel processing
                        tasks = [
                            process_single_article(
                                url_info,
                                site,
                                client,
                                validator,
                                extractor,
                                category_detector,
                                backoff,
                                site_type,
                                sport_focus
                            )
                            for url_info in batch
                        ]
                        
                        # Run batch in parallel
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for result in results:
                            if isinstance(result, Exception):
                                crawl_status["errors"].append(str(result))
                            elif result:
                                crawl_status["articles_saved"] += 1
                            crawl_status["progress"] += 1
                
    except Exception as e:
        logger.error(f"Crawl error: {e}")
        crawl_status["errors"].append(str(e))
    finally:
        crawl_status["is_running"] = False
        crawl_status["current_site"] = None


async def process_single_article(
    url_info: Dict[str, Any],
    site: Site,
    client: HttpClient,
    validator: ArticleValidator,
    extractor: ArticleExtractor,
    category_detector: CategoryDetector,
    backoff: BackoffManager,
    site_type: str,
    sport_focus: Optional[str]
) -> bool:
    """Process a single article. Returns True if saved successfully."""
    url = url_info["url"]
    repo = get_repository()
    
    try:
        # Quick URL validation
        if not validator.quick_validate_url(url):
            return False
        
        # Check backoff
        if backoff.is_blocked(site.domain):
            return False
        
        # Fetch article
        content, http_code, error = await client.get(url)
        
        if error or not content:
            backoff.record_failure(site.domain, http_code)
            return False
        
        # Validate content
        is_valid, _ = validator.validate(url, content)
        if not is_valid:
            return False
        
        # Extract article
        extracted = extractor.extract(url, content, site.name)
        
        # Determine category based on site type
        if site_type == "specific" and sport_focus:
            # Specific sport site - use the sport_focus
            category = sport_focus
        else:
            # General site - detect category
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
        
        repo.save_article(article)
        backoff.record_success(site.domain)
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing {url}: {e}")
        return False


# ==================== STATIC FILES ====================

# Serve static files from frontend directory
frontend_path = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard."""
    index_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>Frontend not found. Run: python -m src.api.server</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

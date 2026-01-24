"""
Database table initialization script for Supabase.
Run this to create or verify tables exist.
"""

from src.database.supabase_client import get_supabase
from src.logging_config import setup_logging, get_logger

logger = get_logger("database.init")

# SQL statements to create tables
CREATE_SITES_TABLE = """
CREATE TABLE IF NOT EXISTS sites (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT UNIQUE NOT NULL,
    sitemap_url TEXT NOT NULL,
    crawl_interval_minutes INTEGER DEFAULT 15,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_DISCOVERED_URLS_TABLE = """
CREATE TABLE IF NOT EXISTS discovered_urls (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    site_id UUID REFERENCES sites(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    url_hash TEXT UNIQUE NOT NULL,
    lastmod TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discovered_urls_hash ON discovered_urls(url_hash);
CREATE INDEX IF NOT EXISTS idx_discovered_urls_site ON discovered_urls(site_id);
"""

CREATE_ARTICLES_TABLE = """
CREATE TABLE IF NOT EXISTS articles (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    url_hash TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    title TEXT NOT NULL,
    author TEXT,
    publish_date TIMESTAMPTZ,
    content TEXT,
    sport_category TEXT,
    source_site TEXT NOT NULL,
    crawl_time TIMESTAMPTZ DEFAULT NOW(),
    ready_for_analysis BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(url_hash);
CREATE INDEX IF NOT EXISTS idx_articles_analysis ON articles(ready_for_analysis);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_site);
"""

CREATE_CRAWL_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_logs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    site_id UUID REFERENCES sites(id) ON DELETE CASCADE,
    crawl_type TEXT NOT NULL,
    status TEXT NOT NULL,
    http_code INTEGER,
    urls_found INTEGER DEFAULT 0,
    new_urls INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_logs_site ON crawl_logs(site_id);
CREATE INDEX IF NOT EXISTS idx_crawl_logs_status ON crawl_logs(status);
"""


def init_tables():
    """
    Initialize database tables.
    
    Note: This requires running SQL directly. In Supabase, you typically
    create tables via the dashboard or migrations. This script prints
    the SQL for manual execution if RPC is not available.
    """
    print("=" * 60)
    print("DATABASE INITIALIZATION")
    print("=" * 60)
    print()
    print("Please run the following SQL in your Supabase SQL Editor:")
    print()
    print("-" * 60)
    print("-- SITES TABLE")
    print("-" * 60)
    print(CREATE_SITES_TABLE)
    print()
    print("-" * 60)
    print("-- DISCOVERED URLS TABLE")  
    print("-" * 60)
    print(CREATE_DISCOVERED_URLS_TABLE)
    print()
    print("-" * 60)
    print("-- ARTICLES TABLE")
    print("-" * 60)
    print(CREATE_ARTICLES_TABLE)
    print()
    print("-" * 60)
    print("-- CRAWL LOGS TABLE")
    print("-" * 60)
    print(CREATE_CRAWL_LOGS_TABLE)
    print()
    print("=" * 60)
    print("After running the SQL, your tables will be ready.")
    print("=" * 60)


if __name__ == "__main__":
    setup_logging()
    init_tables()

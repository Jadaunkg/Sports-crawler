-- Create article_links table to store discovered URLs and metadata independently
-- This table is lighter than 'articles' and serves as the primary deduplication source
-- REFACTORED: Now serves as the ONLY storage for articles (includes content/author)

DROP TABLE IF EXISTS article_links;

CREATE TABLE article_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_id UUID REFERENCES sites(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    url_hash TEXT NOT NULL,
    title TEXT,
    author TEXT,
    content TEXT,
    sport_category TEXT,
    last_modified TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    source_site TEXT,
    
    CONSTRAINT article_links_url_hash_key UNIQUE (url_hash)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_article_links_site_id ON article_links(site_id);
CREATE INDEX IF NOT EXISTS idx_article_links_first_seen ON article_links(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_article_links_published_at ON article_links(published_at);
CREATE INDEX IF NOT EXISTS idx_article_links_sport_category ON article_links(sport_category);

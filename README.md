# Sports News Crawler

Production-grade crawler for sports news sitemap monitoring.

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your Supabase credentials

# Initialize database tables
python -m src.database.init_tables

# Run crawler
python main.py
```

## CLI Commands

```bash
# Single crawl cycle (dry run)
python cli.py run-once --dry-run

# Single crawl cycle (live)
python cli.py run-once

# Check status
python cli.py status
```

## Configuration

Edit `config/sites.yaml` to add/modify sites.

## Environment Variables

| Variable | Description |
|----------|-------------|
| SUPABASE_URL | Supabase project URL |
| SUPABASE_ANON_KEY | Supabase anon key |
| SUPABASE_SERVICE_KEY | Supabase service role key |
| CRAWL_DELAY_MIN | Minimum delay between requests (seconds) |
| CRAWL_DELAY_MAX | Maximum delay between requests (seconds) |
| DEFAULT_CRAWL_INTERVAL_MINUTES | Default sitemap check interval |

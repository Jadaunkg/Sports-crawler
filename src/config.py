"""
Configuration loader for the sports news crawler.
Handles environment variables and YAML site configurations.
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class SiteConfig:
    """Configuration for a single site to crawl."""
    name: str
    domain: str
    sitemap_url: str
    crawl_interval_minutes: int = 15
    is_active: bool = True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SiteConfig":
        return cls(
            name=data["name"],
            domain=data["domain"],
            sitemap_url=data["sitemap_url"],
            crawl_interval_minutes=data.get("crawl_interval_minutes", 15),
            is_active=data.get("is_active", True)
        )


@dataclass
class CrawlerConfig:
    """Main crawler configuration."""
    # Supabase settings
    supabase_url: str
    supabase_anon_key: str
    supabase_service_key: str
    
    # Crawl settings
    crawl_delay_min: int = 2
    crawl_delay_max: int = 5
    default_crawl_interval_minutes: int = 15
    max_retries: int = 3
    backoff_factor: int = 2
    
    # Logging
    log_level: str = "INFO"
    
    # Sites
    sites: List[SiteConfig] = field(default_factory=list)
    
    # Rejection patterns
    reject_patterns: List[str] = field(default_factory=list)
    
    # Sport categories
    sport_categories: List[str] = field(default_factory=list)
    
    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "CrawlerConfig":
        """Load configuration from environment and YAML file."""
        # Get environment variables
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_anon_key = os.getenv("SUPABASE_ANON_KEY")
        supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY")
        
        if not all([supabase_url, supabase_anon_key, supabase_service_key]):
            raise ValueError("Missing required Supabase environment variables")
        
        crawl_delay_min = int(os.getenv("CRAWL_DELAY_MIN", "2"))
        crawl_delay_max = int(os.getenv("CRAWL_DELAY_MAX", "5"))
        default_interval = int(os.getenv("DEFAULT_CRAWL_INTERVAL_MINUTES", "15"))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        
        # Load YAML config
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "sites.yaml"
        else:
            config_path = Path(config_path)
        
        sites = []
        reject_patterns = []
        sport_categories = []
        max_retries = 3
        backoff_factor = 2
        
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_config = yaml.safe_load(f)
            
            # Load sites
            for site_data in yaml_config.get("sites", []):
                sites.append(SiteConfig.from_dict(site_data))
            
            # Load defaults
            defaults = yaml_config.get("defaults", {})
            max_retries = defaults.get("max_retries", 3)
            backoff_factor = defaults.get("backoff_factor", 2)
            
            # Load patterns and categories
            reject_patterns = yaml_config.get("reject_patterns", [])
            sport_categories = yaml_config.get("sport_categories", [])
        
        return cls(
            supabase_url=supabase_url,
            supabase_anon_key=supabase_anon_key,
            supabase_service_key=supabase_service_key,
            crawl_delay_min=crawl_delay_min,
            crawl_delay_max=crawl_delay_max,
            default_crawl_interval_minutes=default_interval,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            log_level=log_level,
            sites=sites,
            reject_patterns=reject_patterns,
            sport_categories=sport_categories
        )
    
    def get_active_sites(self) -> List[SiteConfig]:
        """Return only active sites."""
        return [s for s in self.sites if s.is_active]


# Global config instance
_config: Optional[CrawlerConfig] = None


def get_config(config_path: Optional[str] = None) -> CrawlerConfig:
    """Get or create the global config instance."""
    global _config
    if _config is None:
        _config = CrawlerConfig.load(config_path)
    return _config


def reload_config(config_path: Optional[str] = None) -> CrawlerConfig:
    """Force reload the configuration."""
    global _config
    _config = CrawlerConfig.load(config_path)
    return _config

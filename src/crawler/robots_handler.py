"""
robots.txt handler for crawl compliance.
"""

import asyncio
from typing import Dict, Optional
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import aiohttp

from src.logging_config import get_logger

logger = get_logger("crawler.robots")


class RobotsHandler:
    """
    Handles robots.txt parsing and URL allowance checking.
    Caches robots.txt per domain.
    """
    
    # Crawl as a generic bot
    USER_AGENT = "*"
    
    def __init__(self, cache_ttl: int = 3600):
        self._cache: Dict[str, RobotFileParser] = {}
        self._cache_times: Dict[str, float] = {}
        self._crawl_delays: Dict[str, float] = {}
        self.cache_ttl = cache_ttl
        self._lock = asyncio.Lock()
    
    def _get_robots_url(self, url: str) -> str:
        """Get robots.txt URL for a given page URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc
    
    async def _fetch_robots(
        self,
        domain: str,
        session: aiohttp.ClientSession
    ) -> Optional[RobotFileParser]:
        """Fetch and parse robots.txt for domain."""
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            async with session.get(
                robots_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    # Parse robots.txt
                    rp = RobotFileParser()
                    rp.parse(content.split("\n"))
                    
                    # Extract crawl-delay if present
                    for line in content.split("\n"):
                        if line.lower().startswith("crawl-delay:"):
                            try:
                                delay = float(line.split(":")[1].strip())
                                self._crawl_delays[domain] = delay
                            except (ValueError, IndexError):
                                pass
                    
                    logger.debug(f"Fetched robots.txt for {domain}")
                    return rp
                else:
                    # No robots.txt or error - allow all
                    logger.debug(f"No robots.txt for {domain} (HTTP {response.status})")
                    rp = RobotFileParser()
                    rp.allow_all = True
                    return rp
                    
        except Exception as e:
            logger.debug(f"Failed to fetch robots.txt for {domain}: {e}")
            # On error, allow crawling (be optimistic)
            rp = RobotFileParser()
            rp.allow_all = True
            return rp
    
    async def get_parser(
        self,
        domain: str,
        session: aiohttp.ClientSession
    ) -> RobotFileParser:
        """Get cached or fetch robots.txt parser."""
        import time
        
        async with self._lock:
            now = time.time()
            
            # Check cache
            if domain in self._cache:
                if now - self._cache_times.get(domain, 0) < self.cache_ttl:
                    return self._cache[domain]
            
            # Fetch fresh
            parser = await self._fetch_robots(domain, session)
            if parser:
                self._cache[domain] = parser
                self._cache_times[domain] = now
                return parser
            
            # Fallback - allow all
            rp = RobotFileParser()
            rp.allow_all = True
            return rp
    
    async def is_allowed(
        self,
        url: str,
        session: aiohttp.ClientSession
    ) -> bool:
        """
        Check if URL is allowed by robots.txt.
        
        Args:
            url: URL to check
            session: aiohttp session for fetching robots.txt
            
        Returns:
            True if crawling is allowed
        """
        domain = self._get_domain(url)
        parser = await self.get_parser(domain, session)
        
        # Check if URL is allowed
        try:
            allowed = parser.can_fetch(self.USER_AGENT, url)
            if not allowed:
                logger.debug(f"URL disallowed by robots.txt: {url}")
            return allowed
        except Exception:
            # On parse error, allow
            return True
    
    def get_crawl_delay(self, domain: str) -> Optional[float]:
        """Get crawl-delay for domain if specified in robots.txt."""
        return self._crawl_delays.get(domain)
    
    def clear_cache(self):
        """Clear the robots.txt cache."""
        self._cache.clear()
        self._cache_times.clear()
        self._crawl_delays.clear()

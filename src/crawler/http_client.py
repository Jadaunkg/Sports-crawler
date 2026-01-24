"""
Async HTTP client with user-agent rotation and polite crawling.
"""

import asyncio
import random
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import aiohttp
from fake_useragent import UserAgent

from src.config import get_config
from src.logging_config import get_logger
from src.crawler.rate_limiter import RateLimiter
from src.crawler.robots_handler import RobotsHandler

logger = get_logger("crawler.http_client")


class HttpClient:
    """
    Production-grade HTTP client for crawling.
    Features:
    - User-agent rotation
    - Random delays between requests (optional)
    - Rate limiting per domain
    - robots.txt compliance
    """
    
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        respect_robots: bool = True,
        use_delays: bool = True
    ):
        self._session = session
        self._own_session = session is None
        self.config = get_config()
        self.respect_robots = respect_robots
        self.use_delays = use_delays
        
        # User agent rotation
        try:
            self.ua = UserAgent(browsers=["chrome", "firefox", "edge"])
        except Exception:
            # Fallback if fake-useragent fails
            self.ua = None
            self._fallback_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            ]
        
        # Rate limiter and robots handler
        self.rate_limiter = RateLimiter()
        self.robots_handler = RobotsHandler()
    
    async def __aenter__(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=15)  # Faster timeout
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)  # More connections
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._own_session and self._session:
            await self._session.close()
    
    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Session not initialized. Use async context manager.")
        return self._session
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        if self.ua:
            try:
                return self.ua.random
            except Exception:
                pass
        return random.choice(self._fallback_agents)
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with random user agent."""
        return {
            "User-Agent": self._get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    
    async def _random_delay(self):
        """Apply random delay between requests (if enabled)."""
        if not self.use_delays:
            return
        delay = random.uniform(
            self.config.crawl_delay_min,
            self.config.crawl_delay_max
        )
        await asyncio.sleep(delay)
    
    async def get(
        self,
        url: str,
        skip_robots_check: bool = False
    ) -> tuple[Optional[str], int, Optional[str]]:
        """
        Fetch a URL with all safety measures.
        
        Args:
            url: URL to fetch
            skip_robots_check: Skip robots.txt verification
            
        Returns:
            Tuple of (content, http_code, error_message)
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Check robots.txt
        if self.respect_robots and not skip_robots_check:
            allowed = await self.robots_handler.is_allowed(url, self.session)
            if not allowed:
                logger.warning(f"URL blocked by robots.txt", extra={"url": url})
                return None, 0, "Blocked by robots.txt"
        
        # Apply rate limiting
        await self.rate_limiter.acquire(domain)
        
        # Random delay for human-like behavior
        await self._random_delay()
        
        try:
            async with self.session.get(url, headers=self._get_headers()) as response:
                http_code = response.status
                
                if http_code == 429:
                    # Rate limited - back off
                    self.rate_limiter.mark_rate_limited(domain)
                    logger.warning(
                        f"Rate limited by server",
                        extra={"url": url, "http_code": http_code}
                    )
                    return None, http_code, "Rate limited"
                
                if http_code == 403:
                    logger.warning(
                        f"Access forbidden",
                        extra={"url": url, "http_code": http_code}
                    )
                    return None, http_code, "Forbidden"
                
                if http_code >= 400:
                    return None, http_code, f"HTTP {http_code}"
                
                # Read content
                content = await response.text()
                
                logger.debug(
                    f"Fetched successfully",
                    extra={"url": url, "http_code": http_code}
                )
                return content, http_code, None
                
        except asyncio.TimeoutError:
            logger.error(f"Request timeout", extra={"url": url})
            return None, 0, "Timeout"
        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {e}", extra={"url": url})
            return None, 0, str(e)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", extra={"url": url})
            return None, 0, str(e)
    
    async def head(self, url: str) -> tuple[int, Optional[str]]:
        """
        HEAD request to check URL availability.
        
        Returns:
            Tuple of (http_code, error_message)
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        
        await self.rate_limiter.acquire(domain)
        
        try:
            async with self.session.head(
                url,
                headers=self._get_headers(),
                allow_redirects=True
            ) as response:
                return response.status, None
        except Exception as e:
            return 0, str(e)

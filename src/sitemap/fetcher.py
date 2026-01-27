"""
Sitemap fetcher with async HTTP and gzip support.
Handles sitemap index files recursively.
"""

import asyncio
import gzip
from typing import List, Optional
from io import BytesIO
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.logging_config import get_logger
from src.config import get_config

logger = get_logger("sitemap.fetcher")


class SitemapFetcher:
    """
    Async sitemap fetcher with retry logic and gzip support.
    """
    
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session
        self._own_session = session is None
        self.config = get_config()
    
    async def __aenter__(self):
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._own_session and self._session:
            await self._session.close()
    
    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Session not initialized. Use async context manager.")
        return self._session
    
    def _get_headers(self) -> dict:
        """Get request headers."""
        return {
            "User-Agent": "Mozilla/5.0 (compatible; SportsCrawler/1.0; +https://example.com/bot)",
            "Accept": "application/xml, text/xml, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def fetch(self, url: str) -> Optional[str]:
        """
        Fetch sitemap content from URL.
        
        Args:
            url: Sitemap URL
            
        Returns:
            XML content as string, or None on failure
        """
        logger.info(f"Fetching sitemap", extra={"url": url})
        
        try:
            async with self.session.get(url, headers=self._get_headers()) as response:
                # Accept any 2xx status code as success
                if not (200 <= response.status < 300):
                    logger.warning(
                        f"Sitemap fetch failed",
                        extra={"url": url, "http_code": response.status}
                    )
                    return None
                
                content = await response.read()
                
                # Handle gzip compressed content
                if url.endswith(".gz") or response.headers.get("Content-Encoding") == "gzip":
                    try:
                        content = gzip.decompress(content)
                    except gzip.BadGzipFile:
                        pass  # Not actually gzipped
                
                # Decode to string
                try:
                    xml_content = content.decode("utf-8")
                except UnicodeDecodeError:
                    xml_content = content.decode("latin-1")
                
                logger.info(
                    f"Sitemap fetched successfully",
                    extra={"url": url, "http_code": response.status}
                )
                return xml_content
                
        except asyncio.TimeoutError:
            logger.error(f"Sitemap fetch timeout", extra={"url": url})
            raise
        except aiohttp.ClientError as e:
            logger.error(f"Sitemap fetch error: {e}", extra={"url": url})
            raise
    
    async def fetch_with_status(self, url: str) -> tuple[Optional[str], int, Optional[str]]:
        """
        Fetch sitemap with detailed status.
        
        Returns:
            Tuple of (content, http_code, error_message)
        """
        try:
            async with self.session.get(url, headers=self._get_headers()) as response:
                # Accept any 2xx status code as success
                if not (200 <= response.status < 300):
                    return None, response.status, f"HTTP {response.status}"
                
                content = await response.read()
                
                if url.endswith(".gz") or response.headers.get("Content-Encoding") == "gzip":
                    try:
                        content = gzip.decompress(content)
                    except gzip.BadGzipFile:
                        pass
                
                try:
                    xml_content = content.decode("utf-8")
                except UnicodeDecodeError:
                    xml_content = content.decode("latin-1")
                
                return xml_content, response.status, None
                
        except asyncio.TimeoutError:
            return None, 0, "Timeout"
        except aiohttp.ClientError as e:
            return None, 0, str(e)
        except Exception as e:
            return None, 0, str(e)

"""
Per-domain rate limiter with adaptive throttling.
Uses token bucket algorithm.
"""

import asyncio
import time
from typing import Dict
from dataclasses import dataclass, field

from src.logging_config import get_logger

logger = get_logger("crawler.rate_limiter")


@dataclass
class DomainBucket:
    """Token bucket for a single domain."""
    tokens: float = 1.0
    max_tokens: float = 1.0
    refill_rate: float = 0.5  # tokens per second (1 request per 2 seconds)
    last_refill: float = field(default_factory=time.time)
    rate_limited: bool = False
    rate_limit_until: float = 0.0


class RateLimiter:
    """
    Per-domain rate limiter using token bucket algorithm.
    Supports adaptive throttling when rate limited.
    """
    
    def __init__(
        self,
        default_refill_rate: float = 0.5,
        max_tokens: float = 1.0
    ):
        self.buckets: Dict[str, DomainBucket] = {}
        self.default_refill_rate = default_refill_rate
        self.max_tokens = max_tokens
        self._lock = asyncio.Lock()
    
    def _get_bucket(self, domain: str) -> DomainBucket:
        """Get or create bucket for domain."""
        if domain not in self.buckets:
            self.buckets[domain] = DomainBucket(
                tokens=self.max_tokens,
                max_tokens=self.max_tokens,
                refill_rate=self.default_refill_rate,
                last_refill=time.time()
            )
        return self.buckets[domain]
    
    def _refill(self, bucket: DomainBucket):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket.last_refill
        bucket.tokens = min(
            bucket.max_tokens,
            bucket.tokens + elapsed * bucket.refill_rate
        )
        bucket.last_refill = now
    
    async def acquire(self, domain: str) -> float:
        """
        Acquire a token for the domain.
        Blocks until a token is available.
        
        Returns:
            Wait time in seconds
        """
        async with self._lock:
            bucket = self._get_bucket(domain)
            
            # Check if we're in rate limit cooldown
            now = time.time()
            if bucket.rate_limited and now < bucket.rate_limit_until:
                wait_time = bucket.rate_limit_until - now
                logger.debug(f"Domain in cooldown, waiting {wait_time:.1f}s", extra={"site": domain})
                await asyncio.sleep(wait_time)
                bucket.rate_limited = False
            
            # Refill tokens
            self._refill(bucket)
            
            # Wait if no tokens available
            wait_time = 0.0
            if bucket.tokens < 1.0:
                wait_time = (1.0 - bucket.tokens) / bucket.refill_rate
                await asyncio.sleep(wait_time)
                self._refill(bucket)
            
            # Consume token
            bucket.tokens -= 1.0
            return wait_time
    
    def mark_rate_limited(self, domain: str, cooldown: float = 60.0):
        """
        Mark domain as rate limited.
        Activates cooldown period.
        """
        bucket = self._get_bucket(domain)
        bucket.rate_limited = True
        bucket.rate_limit_until = time.time() + cooldown
        
        # Reduce refill rate for this domain (adaptive throttling)
        bucket.refill_rate = max(0.1, bucket.refill_rate * 0.5)
        
        logger.warning(
            f"Domain marked as rate limited, cooldown {cooldown}s",
            extra={"site": domain}
        )
    
    def reset_domain(self, domain: str):
        """Reset rate limiting for a domain."""
        if domain in self.buckets:
            self.buckets[domain] = DomainBucket(
                tokens=self.max_tokens,
                max_tokens=self.max_tokens,
                refill_rate=self.default_refill_rate,
                last_refill=time.time()
            )
    
    def get_stats(self, domain: str) -> Dict:
        """Get rate limiter stats for domain."""
        if domain not in self.buckets:
            return {"status": "not_tracked"}
        
        bucket = self.buckets[domain]
        return {
            "tokens": bucket.tokens,
            "refill_rate": bucket.refill_rate,
            "rate_limited": bucket.rate_limited,
        }

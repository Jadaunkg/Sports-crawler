"""
Backoff and retry management for failed requests.
Tracks consecutive failures per site.
"""

import time
from typing import Dict, Optional
from dataclasses import dataclass, field

from src.config import get_config
from src.logging_config import get_logger

logger = get_logger("crawler.backoff")


@dataclass
class SiteBackoffState:
    """Backoff state for a single site."""
    consecutive_failures: int = 0
    last_failure_time: float = 0.0
    backoff_until: float = 0.0
    is_blocked: bool = False
    failure_codes: list = field(default_factory=list)


class BackoffManager:
    """
    Manages retry backoff and site blocking.
    Implements exponential backoff on failures.
    Blocks sites with repeated 403/429 responses.
    """
    
    # Threshold for blocking site
    BLOCK_THRESHOLD = 5
    
    # Error codes that count toward blocking
    BLOCKING_CODES = {403, 429}
    
    def __init__(self, max_retries: int = 3, backoff_factor: int = 2):
        self.config = get_config()
        self.max_retries = max_retries or self.config.max_retries
        self.backoff_factor = backoff_factor or self.config.backoff_factor
        self._states: Dict[str, SiteBackoffState] = {}
    
    def _get_state(self, site_domain: str) -> SiteBackoffState:
        """Get or create state for site."""
        if site_domain not in self._states:
            self._states[site_domain] = SiteBackoffState()
        return self._states[site_domain]
    
    def record_success(self, site_domain: str):
        """Record successful request - resets failure count."""
        state = self._get_state(site_domain)
        state.consecutive_failures = 0
        state.failure_codes = []
        state.is_blocked = False
    
    def record_failure(
        self,
        site_domain: str,
        http_code: Optional[int] = None
    ) -> tuple[bool, float]:
        """
        Record a failed request.
        
        Args:
            site_domain: Domain that failed
            http_code: HTTP response code if available
            
        Returns:
            Tuple of (should_retry, wait_seconds)
        """
        state = self._get_state(site_domain)
        state.consecutive_failures += 1
        state.last_failure_time = time.time()
        
        if http_code:
            state.failure_codes.append(http_code)
        
        # Check for blocking conditions
        if http_code in self.BLOCKING_CODES:
            blocking_count = sum(1 for c in state.failure_codes[-10:] if c in self.BLOCKING_CODES)
            if blocking_count >= self.BLOCK_THRESHOLD:
                state.is_blocked = True
                logger.warning(
                    f"Site blocked due to repeated {http_code}s",
                    extra={"site": site_domain}
                )
                return False, 0.0
        
        # Check if we should retry
        if state.consecutive_failures >= self.max_retries:
            logger.warning(
                f"Max retries reached",
                extra={"site": site_domain}
            )
            return False, 0.0
        
        # Calculate backoff time
        wait_time = self.backoff_factor ** state.consecutive_failures
        state.backoff_until = time.time() + wait_time
        
        logger.debug(
            f"Backoff {wait_time}s after failure {state.consecutive_failures}",
            extra={"site": site_domain}
        )
        
        return True, wait_time
    
    def is_blocked(self, site_domain: str) -> bool:
        """Check if site is blocked."""
        state = self._get_state(site_domain)
        return state.is_blocked
    
    def get_wait_time(self, site_domain: str) -> float:
        """Get remaining wait time before retry."""
        state = self._get_state(site_domain)
        remaining = state.backoff_until - time.time()
        return max(0.0, remaining)
    
    def unblock_site(self, site_domain: str):
        """Manually unblock a site."""
        state = self._get_state(site_domain)
        state.is_blocked = False
        state.consecutive_failures = 0
        state.failure_codes = []
        logger.info(f"Site unblocked", extra={"site": site_domain})
    
    def get_stats(self, site_domain: str) -> Dict:
        """Get backoff stats for site."""
        if site_domain not in self._states:
            return {"status": "not_tracked"}
        
        state = self._states[site_domain]
        return {
            "consecutive_failures": state.consecutive_failures,
            "is_blocked": state.is_blocked,
            "recent_codes": state.failure_codes[-10:],
        }
    
    def get_all_blocked(self) -> list:
        """Get list of all blocked sites."""
        return [
            domain for domain, state in self._states.items()
            if state.is_blocked
        ]

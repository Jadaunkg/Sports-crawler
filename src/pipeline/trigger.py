"""
Trigger service for analysis pipeline.
Marks articles as ready and can push to queue/webhook.
"""

import os
import json
from typing import Optional, Union
import httpx

from src.database.repository import Article, ArticleLink, get_repository
from src.logging_config import get_logger

logger = get_logger("pipeline.trigger")


class TriggerService:
    """
    Triggers analysis pipeline for new articles.
    Can push to webhook or queue (Redis).
    """
    
    def __init__(self):
        self.repo = get_repository()
        self.webhook_url = os.getenv("ANALYSIS_WEBHOOK_URL")
        self.redis_url = os.getenv("REDIS_URL")
        self._redis_client = None
    
    async def trigger_analysis(self, article: Union[Article, ArticleLink]) -> bool:
        """
        Trigger analysis for a new article.
        
        Args:
            article: Article or ArticleLink to trigger analysis for
            
        Returns:
            True if trigger succeeded
        """
        # Article is already marked as ready_for_analysis
        # Now push notification
        
        success = True
        
        # Try webhook if configured
        if self.webhook_url:
            success = await self._send_webhook(article)
        
        # Try Redis queue if configured
        if self.redis_url and success:
            success = await self._push_to_queue(article)
        
        if success:
            logger.info(
                f"Analysis triggered for article",
                extra={"url": article.url}
            )
        
        return success
    
    async def _send_webhook(self, article: Article) -> bool:
        """Send article to webhook."""
        try:
            payload = {
                "event": "new_article",
                "article_id": article.id,
                "url": article.url,
                "url_hash": article.url_hash,
                "title": article.title,
                "source_site": article.source_site,
                "sport_category": article.sport_category,
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0
                )
                
                if response.status_code >= 400:
                    logger.warning(
                        f"Webhook failed with {response.status_code}",
                        extra={"url": article.url}
                    )
                    return False
                
                return True
                
        except Exception as e:
            logger.error(f"Webhook error: {e}", extra={"url": article.url})
            return False
    
    async def _push_to_queue(self, article: Article) -> bool:
        """Push article ID to Redis queue."""
        try:
            # Lazy import redis
            import redis.asyncio as redis
            
            if self._redis_client is None:
                self._redis_client = redis.from_url(self.redis_url)
            
            message = json.dumps({
                "article_id": article.id,
                "url_hash": article.url_hash,
                "source_site": article.source_site,
            })
            
            await self._redis_client.lpush("sports_articles_queue", message)
            return True
            
        except ImportError:
            logger.debug("Redis not installed, skipping queue")
            return True
        except Exception as e:
            logger.error(f"Redis error: {e}", extra={"url": article.url})
            return False
    
    def get_pending_count(self) -> int:
        """Get count of articles pending analysis."""
        articles = self.repo.get_articles_for_analysis(limit=1000)
        return len(articles)
    
    def mark_analyzed(self, url_hash: str) -> bool:
        """Mark an article as analyzed."""
        return self.repo.mark_article_analyzed(url_hash)

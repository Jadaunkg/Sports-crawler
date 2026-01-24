"""
Supabase client wrapper for the crawler.
Provides connection management and common operations.
"""

from typing import Optional
from supabase import create_client, Client
from src.config import get_config
from src.logging_config import get_logger

logger = get_logger("database.supabase")


class SupabaseClient:
    """Singleton wrapper for Supabase client."""
    
    _instance: Optional["SupabaseClient"] = None
    _client: Optional[Client] = None
    
    def __new__(cls) -> "SupabaseClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            config = get_config()
            self._client = create_client(
                config.supabase_url,
                config.supabase_service_key  # Use service key for full access
            )
            logger.info("Supabase client initialized")
    
    @property
    def client(self) -> Client:
        """Get the Supabase client instance."""
        if self._client is None:
            raise RuntimeError("Supabase client not initialized")
        return self._client
    
    def table(self, name: str):
        """Get a table reference."""
        return self.client.table(name)


def get_supabase() -> SupabaseClient:
    """Get the Supabase client singleton."""
    return SupabaseClient()

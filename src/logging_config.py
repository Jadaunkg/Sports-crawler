"""
Structured logging configuration for the crawler.
"""

import logging
import sys
from datetime import datetime
from typing import Optional
import json


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields if present
        if hasattr(record, "site"):
            log_data["site"] = record.site
        if hasattr(record, "url"):
            log_data["url"] = record.url
        if hasattr(record, "http_code"):
            log_data["http_code"] = record.http_code
        if hasattr(record, "crawl_type"):
            log_data["crawl_type"] = record.crawl_type
        if hasattr(record, "urls_found"):
            log_data["urls_found"] = record.urls_found
        if hasattr(record, "new_urls"):
            log_data["new_urls"] = record.new_urls
            
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class ReadableFormatter(logging.Formatter):
    """Human-readable formatter for console output."""
    
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        reset = self.RESET
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base = f"{color}[{timestamp}] [{record.levelname:8}]{reset} {record.name}: {record.getMessage()}"
        
        # Add context if present
        extras = []
        if hasattr(record, "site"):
            extras.append(f"site={record.site}")
        if hasattr(record, "url"):
            # Truncate long URLs
            url = record.url
            if len(url) > 60:
                url = url[:57] + "..."
            extras.append(f"url={url}")
        if hasattr(record, "http_code"):
            extras.append(f"http={record.http_code}")
        
        if extras:
            base += f" [{', '.join(extras)}]"
        
        if record.exc_info:
            base += f"\n{self.formatException(record.exc_info)}"
        
        return base


def setup_logging(
    level: str = "INFO",
    json_format: bool = False,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the crawler.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON formatter for machine parsing
        log_file: Optional file path for log output
    
    Returns:
        Root logger instance
    """
    root_logger = logging.getLogger("crawler")
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    
    if json_format:
        console_handler.setFormatter(JsonFormatter())
    else:
        console_handler.setFormatter(ReadableFormatter())
    
    root_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(file_handler)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name."""
    return logging.getLogger(f"crawler.{name}")

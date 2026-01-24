"""
Sport category detector.
Identifies the sport category from URL and content.
"""

import re
from typing import Optional, List
from urllib.parse import urlparse

from src.config import get_config
from src.logging_config import get_logger

logger = get_logger("article.category")


class CategoryDetector:
    """
    Detects sport category from article URL and content.
    Uses URL patterns and keyword matching.
    """
    
    # Keyword to category mapping for common sports
    CATEGORY_KEYWORDS = {
        "football": ["football", "nfl", "touchdown", "quarterback", "gridiron"],
        "soccer": ["soccer", "premier league", "la liga", "bundesliga", "serie a", 
                   "champions league", "fifa", "goal", "striker", "midfielder"],
        "basketball": ["basketball", "nba", "wnba", "ncaa basketball", "three-pointer", "dunk"],
        "cricket": ["cricket", "ipl", "test match", "odi", "t20", "wicket", "bowler", "batsman"],
        "tennis": ["tennis", "wimbledon", "us open", "french open", "australian open", 
                   "grand slam", "serve", "ace"],
        "baseball": ["baseball", "mlb", "home run", "pitcher", "batting"],
        "hockey": ["hockey", "nhl", "puck", "goalie", "stanley cup"],
        "golf": ["golf", "pga", "masters", "birdie", "par", "hole-in-one"],
        "rugby": ["rugby", "try", "scrum", "six nations"],
        "boxing": ["boxing", "heavyweight", "knockout", "round", "bout"],
        "mma": ["mma", "ufc", "mixed martial arts", "submission", "octagon"],
        "f1": ["formula 1", "f1", "grand prix", "pit stop", "pole position"],
        "motorsport": ["motorsport", "racing", "nascar", "motogp", "indycar"],
        "athletics": ["athletics", "track and field", "marathon", "sprint", "olympics"],
        "swimming": ["swimming", "freestyle", "butterfly", "backstroke", "breaststroke"],
        "olympics": ["olympics", "olympic games", "gold medal", "silver medal", "bronze medal"],
    }
    
    def __init__(self):
        self.config = get_config()
        self.custom_categories = self.config.sport_categories
    
    def detect_from_url(self, url: str) -> Optional[str]:
        """
        Detect category from URL path.
        
        Args:
            url: Article URL
            
        Returns:
            Category name or None
        """
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Check each category for URL matches
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            # Check if category name is in path
            if f"/{category}" in path or f"-{category}" in path:
                return category
            
            # Check keywords in path
            for keyword in keywords:
                keyword_pattern = keyword.replace(" ", "[-_/]")
                if re.search(keyword_pattern, path):
                    return category
        
        return None
    
    def detect_from_content(self, title: str, content: str) -> Optional[str]:
        """
        Detect category from article title and content.
        
        Args:
            title: Article title
            content: Article body text
            
        Returns:
            Category name or None
        """
        # Combine title and first part of content for analysis
        text = f"{title} {content[:2000]}".lower()
        
        # Count keyword matches per category
        scores = {}
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            score = 0
            for keyword in keywords:
                # Count occurrences
                count = len(re.findall(r"\b" + re.escape(keyword) + r"\b", text))
                score += count
            
            if score > 0:
                scores[category] = score
        
        if not scores:
            return None
        
        # Return category with highest score
        best_category = max(scores, key=scores.get)
        
        # Only return if we have reasonable confidence (at least 2 matches)
        if scores[best_category] >= 2:
            return best_category
        
        return None
    
    def detect(self, url: str, title: str, content: str) -> Optional[str]:
        """
        Detect sport category using all available signals.
        
        Args:
            url: Article URL
            title: Article title
            content: Article body
            
        Returns:
            Best detected category or None
        """
        # Try URL first (most reliable)
        category = self.detect_from_url(url)
        if category:
            logger.debug(f"Category detected from URL: {category}", extra={"url": url})
            return category
        
        # Fall back to content analysis
        category = self.detect_from_content(title, content)
        if category:
            logger.debug(f"Category detected from content: {category}", extra={"url": url})
            return category
        
        # Default to "sports" if nothing specific found
        return "sports"

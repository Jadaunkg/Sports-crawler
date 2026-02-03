"""
Sport category detector.
Identifies the sport category from URL and content.
"""

import re
from typing import Optional, List, Any
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
    # Restricted to specifically requested categories: Cricket, Basketball, Soccer, NFL, MLB
    CATEGORY_KEYWORDS = {
        "nfl": [
            "nfl", "football", "touchdown", "quarterback", "gridiron", "super bowl", 
            "afc", "nfc", "cowboys", "chiefs", "eagles", "49ers", "packers", "bears", "giants", "steelers",
            "patriots", "bills", "dolphins", "jets", "ravens", "bengals", "browns", "titans", "colts",
            "jaguars", "texans", "broncos", "raiders", "chargers", "vikings", "lions", "buccaneers",
            "saints", "panthers", "falcons", "seahawks", "rams", "cardinals", "commanders",
            "interception", "sack", "fumble", "end zone", "linebacker", "wide receiver", "tight end",
            "running back", "nfl draft", "combine", "playoffs", "wild card"
        ],
        "soccer": [
            "soccer", "premier league", "la liga", "bundesliga", "serie a", "ligue 1", "mls",
            "champions league", "europa league", "fifa", "uefa", "world cup", "euro", "copa america",
            "goal", "striker", "midfielder", "defender", "goalkeeper", "clean sheet", "hat-trick",
            "penalty", "var", "offside", "free kick", "corner kick", "red card", "yellow card",
            "messi", "ronaldo", "mbappe", "haaland", "liverpool", "arsenal", "manchester united",
            "manchester city", "chelsea", "tottenham", "real madrid", "barcelona", "bayern munich",
            "juventus", "psg", "inter miami", "al nassr"
        ],
        "basketball": [
            "basketball", "nba", "wnba", "ncaa", "euroleague", "fiba",
            "three-pointer", "dunk", "layout", "rebound", "assist", "steal", "block", "free throw",
            "point guard", "shooting guard", "small forward", "power forward", "center",
            "lakers", "warriors", "celtics", "bulls", "knicks", "heat", "spurs", "mavericks",
            "suns", "nuggets", "bucks", "sixers", "nets", "clippers", "rockets",
            "lebron", "curry", "durant", "giannis", "jokic", "doncic", "tatum", "embiid",
            "playoffs", "finals", "march madness", "draft"
        ],
        "cricket": [
            "cricket", "ipl", "bbl", "psl", "cpl", "icc", "bcci", "ecb", "ca",
            "test match", "odi", "t20", "twenty20", "ashes", "world cup",
            "wicket", "bowler", "batsman", "all-rounder", "century", "fifty", "sixer", "four",
            "lbw", "drs", "stumped", "run out", "spin", "pace", "seam", "googly", "yorker",
            "kohli", "rohit", "dhoni", "babar", "smith", "cummins", "stokes", "williamson",
            "mumbai indians", "csk", "rcb", "kkr"
        ],
        "mlb": [
            "baseball", "mlb", "milb", "world series", "al", "nl",
            "home run", "pitcher", "catcher", "batter", "hitter", "infielder", "outfielder",
            "strikeout", "era", "rbi", "whip", "ops", "inning", "bullpen", "dugout",
            "yankees", "dodgers", "red sox", "cubs", "cardinals", "giants", "mets", "phillies",
            "braves", "astros", "padres", "blue jays", "rays", "rangers", "orioles",
            "ohtani", "judge", "trout", "harper", "betts", "soto", "acuna"
        ],
    }
    
    
    # Minimum keyword matches required to confirm a category
    MIN_CONFIDENCE_SCORE = 3

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
        
        # Only return if we have reasonable confidence
        if scores[best_category] >= self.MIN_CONFIDENCE_SCORE:
            logger.debug(f"Category detected from content: {best_category} (score: {scores[best_category]})")
            return best_category
        
        return None
    
    def detect(self, url: str, title: str, content: str, site_config: Optional[Any] = None) -> Optional[str]:
        """
        Detect sport category using all available signals.
        
        Args:
            url: Article URL
            title: Article title
            content: Article body
            site_config: Optional site configuration object with site_type and sport_focus attributes
            
        Returns:
            Best detected category or None
        """
        # 1. Check site configuration (Specific sites)
        # Use case-insensitive comparison for site_type
        if site_config:
            site_type_lower = (getattr(site_config, 'site_type', '') or '').lower()
            if site_type_lower == "specific" and site_config.sport_focus:
                return site_config.sport_focus

        # 2. Check URL segments (General sites)
        # Split path into segments and check against sports
        parsed = urlparse(url)
        path = parsed.path.lower()
        segments = [s for s in path.split('/') if s]
        
        # Check if any path segment matches a category directly
        for segment in segments:
            # Direct match with category name
            if segment in self.CATEGORY_KEYWORDS:
                logger.debug(f"Category detected from URL segment: {segment}", extra={"url": url})
                return segment
                
            # Check against keywords for each category
            for category, keywords in self.CATEGORY_KEYWORDS.items():
                if segment in keywords:
                    logger.debug(f"Category detected from URL segment keyword '{segment}' -> '{category}'", extra={"url": url})
                    return category

        # 3. Existing URL pattern matching (fallback for URLs)
        category = self.detect_from_url(url)
        if category:
            logger.debug(f"Category detected from URL pattern: {category}", extra={"url": url})
            return category
        
        # 4. Content analysis
        category = self.detect_from_content(title, content)
        if category:
            logger.debug(f"Category detected from content: {category}", extra={"url": url})
            return category
        
        # Default to "sports" if nothing specific found
        return "sports"

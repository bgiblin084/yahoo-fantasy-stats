"""
Cache Manager for Yahoo Fantasy Sports Data

This module handles local caching of fantasy league data to avoid
unnecessary API calls for prior seasons.

Author: Braedon Giblin
"""

import json
import os
from typing import Dict, Optional, Any
from datetime import datetime
import hashlib


class CacheManager:
    """Manages local caching of fantasy league data."""
    
    CACHE_DIR = "yahoo_fantasy_cache"
    
    def __init__(self, cache_dir: str = None):
        """
        Initialize the cache manager.
        
        Args:
            cache_dir: Optional custom cache directory (default: yahoo_fantasy_cache)
        """
        self.cache_dir = cache_dir or self.CACHE_DIR
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _get_cache_key(self, league_key: str, data_type: str) -> str:
        """
        Generate a cache key from league_key and data_type.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            data_type: Type of data (e.g., 'league_info', 'teams', 'weekly', etc.)
            
        Returns:
            str: Cache key (sanitized filename)
        """
        # Create a hash of the league_key to avoid filesystem issues
        key_hash = hashlib.md5(league_key.encode()).hexdigest()[:8]
        # Sanitize data_type
        safe_data_type = data_type.replace('/', '_').replace('\\', '_')
        return f"{key_hash}_{safe_data_type}.json"
    
    def _get_cache_path(self, league_key: str, data_type: str) -> str:
        """
        Get the full path to a cache file.
        
        Args:
            league_key: League key
            data_type: Type of data
            
        Returns:
            str: Full path to cache file
        """
        cache_key = self._get_cache_key(league_key, data_type)
        return os.path.join(self.cache_dir, cache_key)
    
    def get(self, league_key: str, data_type: str) -> Optional[Any]:
        """
        Retrieve data from cache.
        
        Args:
            league_key: League key
            data_type: Type of data to retrieve
            
        Returns:
            Cached data if available, None otherwise
        """
        cache_path = self._get_cache_path(league_key, data_type)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check if cache is valid (has data and timestamp)
            if 'data' in cache_data and 'cached_at' in cache_data:
                return cache_data['data']
            
            return None
        except (json.JSONDecodeError, IOError, Exception) as e:
            # If cache is corrupted, return None
            return None
    
    def set(self, league_key: str, data_type: str, data: Any) -> bool:
        """
        Store data in cache.
        
        Args:
            league_key: League key
            data_type: Type of data
            data: Data to cache (must be JSON serializable)
            
        Returns:
            bool: True if successful, False otherwise
        """
        cache_path = self._get_cache_path(league_key, data_type)
        
        try:
            cache_data = {
                'league_key': league_key,
                'data_type': data_type,
                'cached_at': datetime.now().isoformat(),
                'data': data
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, default=str)
            
            return True
        except (TypeError, IOError, Exception) as e:
            # If data can't be serialized or file can't be written, return False
            return False
    
    def is_cached(self, league_key: str, data_type: str) -> bool:
        """
        Check if data is cached.
        
        Args:
            league_key: League key
            data_type: Type of data
            
        Returns:
            bool: True if cached, False otherwise
        """
        cache_path = self._get_cache_path(league_key, data_type)
        return os.path.exists(cache_path)
    
    def clear(self, league_key: str = None, data_type: str = None) -> int:
        """
        Clear cache entries.
        
        Args:
            league_key: Optional league key to clear (if None, clears all)
            data_type: Optional data type to clear (if None, clears all types)
            
        Returns:
            int: Number of files deleted
        """
        deleted = 0
        
        if league_key and data_type:
            # Clear specific cache entry
            cache_path = self._get_cache_path(league_key, data_type)
            if os.path.exists(cache_path):
                try:
                    os.remove(cache_path)
                    deleted = 1
                except:
                    pass
        else:
            # Clear all or filtered cache entries
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.cache_dir, filename)
                    try:
                        # If filtering by league_key, check the file
                        if league_key:
                            with open(file_path, 'r') as f:
                                cache_data = json.load(f)
                                if cache_data.get('league_key') == league_key:
                                    os.remove(file_path)
                                    deleted += 1
                        elif data_type:
                            with open(file_path, 'r') as f:
                                cache_data = json.load(f)
                                if cache_data.get('data_type') == data_type:
                                    os.remove(file_path)
                                    deleted += 1
                        else:
                            # Clear all
                            os.remove(file_path)
                            deleted += 1
                    except:
                        continue
        
        return deleted
    
    def is_prior_season(self, league_info: Dict[str, Any]) -> bool:
        """
        Determine if a league is from a prior (completed) season.
        
        Args:
            league_info: League information dictionary
            
        Returns:
            bool: True if prior season, False if current season
        """
        if not league_info:
            return False
        
        # Get current year
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Get season from league info
        season = league_info.get('season')
        if not season:
            return False
        
        try:
            season_year = int(season)
        except (ValueError, TypeError):
            return False
        
        # NFL season typically ends in January/February
        # If we're past February and the season is from last year, it's a prior season
        # If season is more than 1 year old, it's definitely prior
        if season_year < current_year - 1:
            return True
        
        if season_year == current_year - 1 and current_month > 2:
            return True
        
        # Check if season has ended (current_week >= end_week)
        current_week = league_info.get('current_week')
        end_week = league_info.get('end_week')
        
        if current_week and end_week:
            try:
                if int(current_week) >= int(end_week):
                    return True
            except (ValueError, TypeError):
                pass
        
        return False


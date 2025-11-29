"""
Yahoo Fantasy Sports API Client

This module contains a class for interacting with the Yahoo Fantasy Sports API,
including API calls and data parsing functions.

Author: Braedon Giblin
"""

import json
import os
from typing import Dict, List, Optional, Any
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

# Import cache manager if available
try:
    from cache_manager import CacheManager
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    CacheManager = None

# Import manager nickname mapper if available
try:
    from manager_nickname_mapper import ManagerNicknameMapper
    NICKNAME_MAPPER_AVAILABLE = True
except ImportError:
    NICKNAME_MAPPER_AVAILABLE = False
    ManagerNicknameMapper = None


class YahooFantasyAPI:
    """
    Client class for Yahoo Fantasy Sports API interactions.
    
    This class provides methods for making API calls and parsing
    Yahoo's complex nested JSON response structures.
    """
    
    BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"
    
    def __init__(self, oauth_session, oauth_client=None, use_cache=True):
        """
        Initialize the Yahoo Fantasy API client.
        
        Args:
            oauth_session: Authenticated OAuth2Session from requests_oauthlib
            oauth_client: Optional YahooOAuth instance for automatic token refresh
            use_cache: Whether to use local cache for prior seasons (default: True)
        """
        self.oauth_session = oauth_session
        self.oauth_client = oauth_client
        self.use_cache = use_cache and CACHE_AVAILABLE
        self.cache_manager = CacheManager() if self.use_cache else None
        self.nickname_mapper = ManagerNicknameMapper() if NICKNAME_MAPPER_AVAILABLE else None
    
    def _apply_nickname_mapping_to_df(self, df: 'pd.DataFrame', league_key: str) -> 'pd.DataFrame':
        """
        Apply nickname mapping to a DataFrame containing team data.
        
        Args:
            df: DataFrame with team data (must have 'manager_nickname' and 'team_name' columns)
            league_key: League key for extracting season
            
        Returns:
            pd.DataFrame: DataFrame with mapped nicknames applied
        """
        if not PANDAS_AVAILABLE or df is None or df.empty:
            return df
        
        if not self.nickname_mapper:
            return df
        
        # Extract season from league_key (format: {game_id}.l.{league_id})
        season = league_key.split('.')[0] if '.' in league_key and league_key else ''
        if not season or not season.isdigit():
            return df
        
        # Check if DataFrame has the required columns
        if 'manager_nickname' not in df.columns or 'team_name' not in df.columns:
            return df
        
        # Apply mapping to rows with "--hidden--" nickname
        for idx in df.index:
            nickname = df.loc[idx, 'manager_nickname']
            if nickname == "--hidden--" or nickname == "N/A" or not nickname:
                team_name = df.loc[idx, 'team_name']
                if team_name and team_name != 'N/A':
                    mapped_nickname = self.nickname_mapper.apply_mapping(
                        team_name, league_key, season, nickname
                    )
                    df.loc[idx, 'manager_nickname'] = mapped_nickname
        
        return df
    
    def _apply_nickname_mapping_to_list(self, data_list: List[Dict[str, Any]], league_key: str) -> List[Dict[str, Any]]:
        """
        Apply nickname mapping to a list of team data dictionaries.
        
        Args:
            data_list: List of team data dictionaries
            league_key: League key for extracting season
            
        Returns:
            list: List with mapped nicknames applied
        """
        if not data_list or not self.nickname_mapper:
            return data_list
        
        # Extract season from league_key (format: {game_id}.l.{league_id})
        season = league_key.split('.')[0] if '.' in league_key and league_key else ''
        if not season or not season.isdigit():
            return data_list
        
        # Apply mapping to each item
        for item in data_list:
            if isinstance(item, dict):
                nickname = item.get('manager_nickname', '')
                if nickname == "--hidden--" or nickname == "N/A" or not nickname:
                    team_name = item.get('team_name', '')
                    if team_name and team_name != 'N/A':
                        mapped_nickname = self.nickname_mapper.apply_mapping(
                            team_name, league_key, season, nickname
                        )
                        item['manager_nickname'] = mapped_nickname
        
        return data_list
    
    def _make_request(self, url, params=None, retry=True):
        """
        Make an API request with automatic token refresh on expiration.
        
        Args:
            url: API endpoint URL
            params: Optional query parameters
            retry: Whether to retry after token refresh (prevents infinite loops)
            
        Returns:
            requests.Response: API response
            
        Raises:
            Exception: If API call fails after token refresh attempt
        """
        try:
            response = self.oauth_session.get(url, params=params)
            # Check for token expiration (401 Unauthorized)
            if response.status_code == 401 and retry and self.oauth_client:
                # Try to refresh the token
                try:
                    self.oauth_client.refresh_access_token()
                    self.oauth_client.save_tokens()
                    # Recreate the session with the new token
                    self.oauth_session = self.oauth_client.create_authenticated_session()
                    # Retry the request once
                    response = self.oauth_session.get(url, params=params)
                except Exception as refresh_error:
                    raise Exception(f"Token expired and refresh failed: {refresh_error}")
        except Exception as e:
            # Check if it's a token expiration error (common error messages)
            error_str = str(e).lower()
            if retry and self.oauth_client and ('token' in error_str and 'expired' in error_str or 
                                                'unauthorized' in error_str or 
                                                '401' in error_str):
                try:
                    # Try to refresh the token
                    self.oauth_client.refresh_access_token()
                    self.oauth_client.save_tokens()
                    # Recreate the session with the new token
                    self.oauth_session = self.oauth_client.create_authenticated_session()
                    # Retry the request once (without retry flag to prevent infinite loops)
                    response = self.oauth_session.get(url, params=params)
                except Exception as refresh_error:
                    raise Exception(f"Token expired and refresh failed: {refresh_error}")
            else:
                # Re-raise if it's not a token expiration error or we can't retry
                raise
        
        response.raise_for_status()
        return response
    
    # ==================== API Call Methods ====================
    
    def get_user_games(self) -> List[Dict[str, Any]]:
        """
        Get and parse games for the authenticated user.
        
        Returns:
            list: List of parsed game dictionaries
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/users;use_login=1/games"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            games_data = response.json()
        except ValueError as e:
            if response.text.strip().startswith('<?xml') or response.text.strip().startswith('<'):
                raise Exception(f"API returned XML instead of JSON. Check API format parameter.")
            else:
                raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Error: {e}")
        
        # Parse and return games
        return self._parse_games(games_data)
    
    def get_leagues(self, game_key: str) -> List[Dict[str, Any]]:
        """
        Get and parse leagues for a specific game.
        
        Args:
            game_key: Game key (e.g., '461' for NFL 2025)
            
        Returns:
            list: List of parsed league dictionaries
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/users;use_login=1/games;game_keys={game_key}/leagues"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            leagues_data = response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
        
        # Parse and return leagues
        return self._parse_leagues(leagues_data)
    
    def get_league_info(self, league_key: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get and parse basic league information.
        Uses cache for prior seasons if available.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            force_refresh: If True, bypass cache and fetch from API
            
        Returns:
            dict: Parsed league information dictionary, or None if not found
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        # Check cache first (unless forcing refresh)
        if self.use_cache and not force_refresh:
            cached_data = self.cache_manager.get(league_key, 'league_info')
            if cached_data is not None:
                return cached_data
        
        url = f"{self.BASE_URL}/league/{league_key}"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            league_data = response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
        
        # Parse and return league info
        league_info = self._parse_league_info(league_data)
        
        # Cache the result if it's a prior season
        if self.use_cache and league_info:
            is_prior = self.cache_manager.is_prior_season(league_info)
            if is_prior:
                self.cache_manager.set(league_key, 'league_info', league_info)
        
        return league_info
    
    def get_league_teams(self, league_key: str) -> List[Dict[str, Any]]:
        """
        Get and parse teams in a league.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            list: List of parsed team dictionaries
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/league/{league_key}/teams"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            teams_data = response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
        
        # Parse and return teams
        return self._parse_teams(teams_data)
    
    def get_team_roster(self, team_key: str) -> Dict[str, Any]:
        """
        Get roster for a specific team.
        
        Args:
            team_key: Team key (e.g., '461.l.621700.t.1')
            
        Returns:
            dict: Team roster data from API
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/team/{team_key}/roster"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
    
    def get_team_stats(self, team_key: str, week: Optional[int] = None) -> Dict[str, Any]:
        """
        Get stats for a specific team.
        
        Args:
            team_key: Team key (e.g., '461.l.621700.t.1')
            week: Optional week number (default: current week)
            
        Returns:
            dict: Team stats data from API
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        if week:
            url = f"{self.BASE_URL}/team/{team_key}/stats;week={week}"
        else:
            url = f"{self.BASE_URL}/team/{team_key}/stats"
        
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
    
    def get_team_stats_by_week(self, team_key: str, week: int) -> Optional[Dict[str, Any]]:
        """
        Get team stats for a specific week, including moves, trades, and FAAB.
        
        Args:
            team_key: Team key (e.g., '461.l.621700.t.1')
            week: Week number
            
        Returns:
            dict: Parsed team stats for the week, or None if not found
        """
        try:
            stats_data = self.get_team_stats(team_key, week)
            return self._parse_team_stats_for_week(stats_data, team_key, week)
        except Exception as e:
            import logging
            logging.debug(f"Failed to get team stats for {team_key} week {week}: {e}")
            return None
    
    def get_transactions_by_week(self, league_key: str) -> Dict[int, Dict[str, Dict[str, int]]]:
        """
        Get transactions grouped by week and team, calculating cumulative moves and trades.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            dict: Dictionary mapping week -> team_key -> {'moves': count, 'trades': count}
        """
        import time
        from datetime import datetime
        
        # Get league info to determine week boundaries
        league_info = self.get_league_info(league_key)
        if not league_info:
            return {}
        
        # Get transactions
        transactions_data = self.get_league_transactions(league_key)
        transactions_list = self._parse_transactions(transactions_data)
        
        # Get start date to calculate week numbers from timestamps
        start_date_str = league_info.get('start_date', '')
        if not start_date_str:
            return {}
        
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        except:
            return {}
        
        # Initialize weekly transaction counts
        weekly_transactions = {}
        teams = self.get_league_teams(league_key)
        team_keys = [t.get('team_key') for t in teams if isinstance(t, dict) and t.get('team_key')]
        
        # Process each transaction
        for transaction in transactions_list:
            timestamp = transaction.get('timestamp')
            if not timestamp:
                continue
            
            # Convert timestamp to datetime
            try:
                trans_date = datetime.fromtimestamp(int(timestamp))
            except:
                continue
            
            # Calculate week number (weeks since start_date)
            days_diff = (trans_date - start_date).days
            week = (days_diff // 7) + 1  # Week 1 starts at day 0
            
            if week < 1:
                continue
            
            trans_type = transaction.get('type', '')
            team_key = None
            
            # Extract team key from transaction
            if trans_type in ['add', 'drop', 'add/drop']:
                # For add/drop, get team from players
                players = transaction.get('players', [])
                if players and len(players) > 0:
                    # Try to get destination team from transaction data
                    # This might need to be parsed from the raw transaction
                    pass
            
            # For now, we'll need to parse team from transaction data differently
            # Let's update the parsing to include team information
        
        return weekly_transactions
    
    def get_all_teams_weekly_stats(self, league_key: str, start_week: Optional[int] = None, end_week: Optional[int] = None, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Get weekly stats for all teams in a league.
        Uses transaction data to calculate actual week-by-week cumulative moves and trades.
        Uses cache for prior seasons if available.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            start_week: Starting week number (default: 1)
            end_week: Ending week number (default: current week)
            force_refresh: If True, bypass cache and fetch from API
            
        Returns:
            list: List of weekly team stats dictionaries
        """
        import time
        from datetime import datetime, timedelta
        
        # Get league info to check if prior season
        league_info = self.get_league_info(league_key, force_refresh=force_refresh)
        is_prior_season = self.cache_manager.is_prior_season(league_info) if self.use_cache and league_info else False
        
        # Check cache first for prior seasons (unless forcing refresh)
        if self.use_cache and not force_refresh and is_prior_season:
            cached_data = self.cache_manager.get(league_key, 'weekly_stats')
            if cached_data is not None:
                # Apply nickname mapping to cached data
                cached_data = self._apply_nickname_mapping_to_list(cached_data, league_key)
                # Filter by week range if specified
                if start_week is not None or end_week is not None:
                    filtered_data = []
                    for item in cached_data:
                        week = item.get('week', 0)
                        if start_week is not None and week < start_week:
                            continue
                        if end_week is not None and week > end_week:
                            continue
                        filtered_data.append(item)
                    return filtered_data
                return cached_data
        
        if not league_info:
            return []
        
        current_week = int(league_info.get('current_week', 1))
        start = int(league_info.get('start_week', 1))
        end = int(league_info.get('end_week', current_week))
        start_date_str = league_info.get('start_date', '')
        
        # Override with provided parameters if specified
        if start_week is not None:
            start = start_week
        if end_week is not None:
            end = end_week
        
        # Get all teams
        teams = self.get_league_teams(league_key)
        if not teams:
            return []
        
        # Get transactions to calculate actual week-by-week data
        transactions_data = self.get_league_transactions(league_key)
        transactions_list = self._parse_transactions(transactions_data)
        
        # Calculate week from timestamps using Tuesday boundaries
        # Fantasy weeks run Tuesday to Tuesday
        start_date = None
        first_tuesday = None
        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                # Find the Tuesday of the week containing start_date
                # Monday = 0, Tuesday = 1, ..., Sunday = 6
                days_until_tuesday = (1 - start_date.weekday()) % 7
                if days_until_tuesday == 0 and start_date.weekday() == 1:
                    # Already a Tuesday
                    first_tuesday = start_date
                else:
                    # Move to next Tuesday if not already Tuesday
                    first_tuesday = start_date + timedelta(days=days_until_tuesday)
            except:
                pass
        
        # Initialize cumulative counts per team per week
        team_weekly_counts = {}
        for team in teams:
            if isinstance(team, dict):
                team_key = team.get('team_key')
                if team_key:
                    team_weekly_counts[team_key] = {'moves': 0, 'trades': 0}
        
        # Process transactions to build week-by-week cumulative counts
        # Initialize cumulative counts per team per week
        team_weekly_counts = {}  # week -> team_key -> {'moves': count, 'trades': count}
        
        # Get starting FAAB balance (default 100, but check league settings)
        # Try to get from current team stats to determine starting amount
        starting_faab = 100
        try:
            # Get current week stats to see if we can determine starting FAAB
            # We'll calculate starting FAAB by looking at total spent + current balance
            sample_team = teams[0] if teams else None
            if sample_team and isinstance(sample_team, dict):
                sample_team_key = sample_team.get('team_key')
                if sample_team_key:
                    current_stats = self.get_team_stats_by_week(sample_team_key, current_week)
                    if current_stats:
                        # This won't give us starting balance directly, but we'll use 100 as default
                        # and calculate from current balance + total spent
                        pass
        except:
            pass
        
        # Initialize all weeks and teams
        for week_num in range(start, min(end + 1, current_week + 1)):
            team_weekly_counts[week_num] = {}
            for team in teams:
                if isinstance(team, dict):
                    team_key = team.get('team_key')
                    if team_key:
                        team_weekly_counts[week_num][team_key] = {'moves': 0, 'trades': 0, 'faab_spent': 0}
        
        # Sort transactions by timestamp
        sorted_transactions = sorted(transactions_list, key=lambda t: int(t.get('timestamp', '0')))
        
        # Process each transaction and accumulate counts
        for transaction in sorted_transactions:
            timestamp = transaction.get('timestamp')
            if not timestamp or not first_tuesday:
                continue
            
            try:
                trans_date = datetime.fromtimestamp(int(timestamp))
                
                # Calculate which Tuesday-aligned week this transaction belongs to
                # Weeks run Tuesday to Tuesday (Tuesday is the start of the week)
                # Find the Tuesday that starts the week containing this transaction
                # If transaction is on Tuesday or later in the week, go back to that Tuesday
                # If transaction is on Monday, go back to previous Tuesday
                if trans_date.weekday() == 1:  # Tuesday
                    trans_tuesday = trans_date
                elif trans_date.weekday() == 0:  # Monday
                    # Go back to previous Tuesday (6 days)
                    trans_tuesday = trans_date - timedelta(days=6)
                else:  # Wednesday-Sunday (weekday 2-6)
                    # Go back to Tuesday of this week
                    days_since_tuesday = trans_date.weekday() - 1
                    trans_tuesday = trans_date - timedelta(days=days_since_tuesday)
                
                # Calculate weeks since first Tuesday
                days_diff = (trans_tuesday - first_tuesday).days
                week = (days_diff // 7) + 1
                
                # Ensure week is at least 1
                if week < 1:
                    week = 1
                
                if week < start or week > end:
                    continue
            except:
                continue
            
            trans_type = transaction.get('type', '')
            faab_bid = transaction.get('faab_bid', 0)
            
            # For trades, use trade_teams to avoid duplicates
            # For other transactions, use team_keys
            if trans_type == 'trade':
                team_keys = transaction.get('trade_teams', [])
                # Remove duplicates and ensure we have valid team keys
                team_keys = list(set([tk for tk in team_keys if tk and tk.startswith(league_key)]))
            else:
                team_keys = transaction.get('team_keys', [])
                # Remove duplicates
                team_keys = list(set([tk for tk in team_keys if tk and tk.startswith(league_key)]))
            
            # Count move or trade, and track FAAB spending
            for team_key in team_keys:
                if team_key and team_key.startswith(league_key):
                    # Update cumulative counts for this week and all subsequent weeks
                    for week_num in range(week, min(end + 1, current_week + 1) + 1):
                        if week_num in team_weekly_counts and team_key in team_weekly_counts[week_num]:
                            if trans_type in ['add', 'drop', 'add/drop']:
                                team_weekly_counts[week_num][team_key]['moves'] += 1
                            elif trans_type == 'trade':
                                # Count trade once per team (each trade involves 2 teams)
                                team_weekly_counts[week_num][team_key]['trades'] += 1
                            
                            # Add FAAB spending (only for successful transactions with FAAB bids)
                            if faab_bid > 0 and transaction.get('status') == 'successful':
                                team_weekly_counts[week_num][team_key]['faab_spent'] += faab_bid
        
        # All teams start with 100 FAAB
        team_starting_faab = {}
        for team in teams:
            if not isinstance(team, dict):
                continue
            team_key = team.get('team_key')
            if not team_key:
                continue
            
            # Always use 100 as starting FAAB
            team_starting_faab[team_key] = 100
        
        # Build weekly stats list from cumulative counts
        weekly_stats_list = []
        for week_num in range(start, min(end + 1, current_week + 1)):
            for team in teams:
                if not isinstance(team, dict):
                    continue
                
                team_key = team.get('team_key')
                if not team_key:
                    continue
                
                # Get team name
                team_name = team.get('name', 'N/A')
                
                # Get cumulative counts for this week
                moves = 0
                trades = 0
                faab_spent = 0
                if week_num in team_weekly_counts and team_key in team_weekly_counts[week_num]:
                    moves = team_weekly_counts[week_num][team_key]['moves']
                    trades = team_weekly_counts[week_num][team_key]['trades']
                    faab_spent = team_weekly_counts[week_num][team_key]['faab_spent']
                
                # Calculate balance for this week: starting - spent up to this week
                starting_faab = team_starting_faab.get(team_key, 100)
                faab_balance = starting_faab - faab_spent
                
                weekly_stats_list.append({
                    'team_key': team_key,
                    'week': week_num,
                    'team_name': team_name,
                    'number_of_moves': moves,
                    'number_of_trades': trades,
                    'faab_balance': faab_balance
                })
        
        # Cache the result if it's a prior season
        if self.use_cache and is_prior_season:
            self.cache_manager.set(league_key, 'weekly_stats', weekly_stats_list)
        
        return weekly_stats_list
    
    def _parse_team_stats_for_week(self, stats_data: Dict[str, Any], team_key: str, week: int) -> Optional[Dict[str, Any]]:
        """
        Parse team stats from API response for a specific week.
        
        Args:
            stats_data: Raw team stats data from get_team_stats()
            team_key: Team key for reference
            week: Week number
            
        Returns:
            dict: Parsed team stats dictionary for the week
        """
        fantasy_content = stats_data.get('fantasy_content', {})
        team_obj = fantasy_content.get('team', {})
        
        # Handle team as list or dict with numeric keys
        team_info = {}
        
        if isinstance(team_obj, list):
            # Team is a list where team[0] is a list of single-key dicts with team info
            if len(team_obj) > 0 and isinstance(team_obj[0], list):
                for item in team_obj[0]:
                    if isinstance(item, dict):
                        # Extract key-value pairs from single-key dicts
                        for k, v in item.items():
                            if isinstance(v, dict):
                                team_info.update(v)
                            elif not isinstance(v, list):
                                team_info[k] = v
        elif isinstance(team_obj, dict):
            # Numeric key structure
            for key in team_obj.keys():
                if key != 'count' and key.isdigit():
                    team_data = team_obj.get(key, {})
                    if isinstance(team_data, dict):
                        if 'team' in team_data:
                            team_nested = team_data.get('team', {})
                            if isinstance(team_nested, list):
                                if len(team_nested) > 0 and isinstance(team_nested[0], list):
                                    for item in team_nested[0]:
                                        if isinstance(item, dict):
                                            for k, v in item.items():
                                                if isinstance(v, dict):
                                                    team_info.update(v)
                                                elif not isinstance(v, list):
                                                    team_info[k] = v
                            elif isinstance(team_nested, dict):
                                team_info = team_nested
                        else:
                            team_info = team_data
                        break
        
        if not team_info:
            return None
        
        # Extract team info
        parsed_stats = {
            'team_key': team_key,
            'week': week,
            'team_name': team_info.get('name', 'N/A'),
            'number_of_moves': int(team_info.get('number_of_moves', 0)) if team_info.get('number_of_moves') else 0,
            'number_of_trades': int(team_info.get('number_of_trades', 0)) if team_info.get('number_of_trades') else 0,
            'faab_balance': int(team_info.get('faab_balance', 100)) if team_info.get('faab_balance') else 100,
        }
        
        return parsed_stats
    
    def get_transactions_dataframe(self, league_key: str) -> Optional['pd.DataFrame']:
        """
        Get league transactions and return as a pandas DataFrame.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            pd.DataFrame: DataFrame containing transactions, or None if pandas is not available
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for this functionality. Install it with: pip install pandas")
        
        try:
            transactions_data = self.get_league_transactions(league_key)
            transactions_list = self._parse_transactions(transactions_data)
            if transactions_list:
                df = pd.DataFrame(transactions_list)
                return df
        except Exception as e:
            import logging
            logging.debug(f"Failed to get transactions: {e}")
        
        return None
    
    def _parse_transactions(self, transactions_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse transactions from API response.
        
        Args:
            transactions_data: Raw transactions data from get_league_transactions()
            
        Returns:
            list: List of transaction dictionaries
        """
        fantasy_content = transactions_data.get('fantasy_content', {})
        league_obj = fantasy_content.get('league', {})
        
        # Handle league as list or dict with numeric keys
        transactions = None
        
        if isinstance(league_obj, list):
            # League is a list - find transactions
            for item in league_obj:
                if isinstance(item, dict) and 'transactions' in item:
                    transactions = item.get('transactions', {})
                    break
        elif isinstance(league_obj, dict):
            # Numeric key structure
            for key in league_obj.keys():
                if key != 'count' and key.isdigit():
                    league_data = league_obj.get(key, {})
                    if isinstance(league_data, dict):
                        if 'league' in league_data:
                            league_nested = league_data.get('league', {})
                            if isinstance(league_nested, list):
                                for item in league_nested:
                                    if isinstance(item, dict) and 'transactions' in item:
                                        transactions = item.get('transactions', {})
                                        break
                            elif isinstance(league_nested, dict) and 'transactions' in league_nested:
                                transactions = league_nested.get('transactions', {})
                        elif 'transactions' in league_data:
                            transactions = league_data.get('transactions', {})
                        break
        
        if not isinstance(transactions, dict):
            return []
        
        # Extract transactions
        transactions_list = []
        for key in transactions.keys():
            if key != 'count' and key.isdigit():
                transaction_data = transactions.get(key, {})
                if isinstance(transaction_data, dict) and 'transaction' in transaction_data:
                    transaction_raw = transaction_data.get('transaction', {})
                    
                    # Parse transaction
                    parsed_transaction = self._parse_single_transaction(transaction_raw)
                    if parsed_transaction:
                        transactions_list.append(parsed_transaction)
        
        return transactions_list
    
    def _parse_single_transaction(self, transaction_raw: Any) -> Optional[Dict[str, Any]]:
        """
        Parse a single transaction from the transactions list.
        
        Args:
            transaction_raw: Transaction data (can be list or dict)
            
        Returns:
            dict: Parsed transaction dictionary
        """
        transaction_info = {}
        
        if isinstance(transaction_raw, list):
            # Transaction is a list of single-key dicts or mixed structure
            for item in transaction_raw:
                if isinstance(item, dict):
                    for k, v in item.items():
                        if k == 'players':
                            # Preserve players structure for later processing
                            transaction_info[k] = v
                        elif isinstance(v, dict):
                            transaction_info.update(v)
                        elif not isinstance(v, list):
                            transaction_info[k] = v
                elif isinstance(item, list):
                    # Nested list structure - extract dicts
                    for sub_item in item:
                        if isinstance(sub_item, dict):
                            for k, v in sub_item.items():
                                if isinstance(v, dict):
                                    transaction_info.update(v)
                                elif not isinstance(v, list):
                                    transaction_info[k] = v
        elif isinstance(transaction_raw, dict):
            transaction_info = transaction_raw.copy()
        
        if not transaction_info:
            return None
        
        # Extract key fields
        parsed = {
            'transaction_key': transaction_info.get('transaction_key', 'N/A'),
            'transaction_id': transaction_info.get('transaction_id', 'N/A'),
            'type': transaction_info.get('type', 'N/A'),
            'status': transaction_info.get('status', 'N/A'),
            'timestamp': transaction_info.get('timestamp', 'N/A'),
            'faab_bid': int(transaction_info.get('faab_bid', 0)) if transaction_info.get('faab_bid') else 0,
        }
        
        # Extract team info from players/transaction_data
        team_keys_involved = set()
        player_transactions = []
        
        if 'players' in transaction_info:
            players = transaction_info.get('players', {})
            if isinstance(players, dict):
                # Extract player transactions (adds/drops)
                for key in players.keys():
                    if key != 'count' and key.isdigit():
                        player_data = players.get(key, {})
                        if isinstance(player_data, dict) and 'player' in player_data:
                            player_obj = player_data.get('player', {})
                            player_info, team_key = self._extract_player_and_team_from_transaction(player_obj)
                            if player_info:
                                player_transactions.append(player_info)
                            if team_key:
                                team_keys_involved.add(team_key)
        
        parsed['players'] = player_transactions
        parsed['team_keys'] = list(team_keys_involved)
        
        # Extract trade info if it's a trade
        if parsed['type'] == 'trade':
            trade_info = transaction_info.get('trader_team_key') or transaction_info.get('tradee_team_key')
            if trade_info:
                parsed['trade_teams'] = [
                    transaction_info.get('trader_team_key', 'N/A'),
                    transaction_info.get('tradee_team_key', 'N/A')
                ]
                # Add trade team keys to team_keys
                trader_key = transaction_info.get('trader_team_key')
                tradee_key = transaction_info.get('tradee_team_key')
                if trader_key:
                    parsed['team_keys'].append(trader_key)
                if tradee_key:
                    parsed['team_keys'].append(tradee_key)
        
        return parsed
    
    def _extract_player_and_team_from_transaction(self, player_raw: Any) -> tuple:
        """
        Extract player info and team key from transaction player structure.
        
        Args:
            player_raw: Player data from transaction (can be list or dict)
            
        Returns:
            tuple: (player_info dict, team_key string) or (None, None)
        """
        player_info = {}
        team_key = None
        
        if isinstance(player_raw, list):
            # Handle list structure: [player_info_list, transaction_data_dict]
            for item in player_raw:
                if isinstance(item, dict):
                    # Check if this is transaction_data dict
                    if 'transaction_data' in item:
                        trans_data_list = item.get('transaction_data', [])
                        if isinstance(trans_data_list, list):
                            for trans_data in trans_data_list:
                                if isinstance(trans_data, dict):
                                    # Check for destination_team_key (add) or source_team_key (drop)
                                    team_key = trans_data.get('destination_team_key') or trans_data.get('source_team_key')
                                    if team_key:
                                        break
                    else:
                        # Regular player info dict
                        player_info.update(item)
                elif isinstance(item, list):
                    # Nested list - extract player info
                    for sub_item in item:
                        if isinstance(sub_item, dict):
                            for k, v in sub_item.items():
                                if isinstance(v, dict):
                                    player_info.update(v)
                                elif not isinstance(v, list):
                                    player_info[k] = v
        elif isinstance(player_raw, dict):
            player_info = player_raw.copy()
            # Check for transaction_data in dict structure
            if 'transaction_data' in player_raw:
                trans_data_list = player_raw.get('transaction_data', [])
                if isinstance(trans_data_list, list):
                    for trans_data in trans_data_list:
                        if isinstance(trans_data, dict):
                            team_key = trans_data.get('destination_team_key') or trans_data.get('source_team_key')
                            if team_key:
                                break
        
        return player_info, team_key
    
    def get_league_standings(self, league_key: str) -> Dict[str, Any]:
        """
        Get league standings which includes team stats.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            dict: League standings data from API
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/league/{league_key}/standings"
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
    
    def get_league_transactions(self, league_key: str, transaction_type: Optional[str] = None, count: Optional[int] = None) -> Dict[str, Any]:
        """
        Get league transactions (adds, drops, trades, etc.).
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            transaction_type: Optional filter for transaction type (add, drop, trade, commish, etc.)
            count: Optional limit on number of transactions to return
            
        Returns:
            dict: Transactions data from API
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        url = f"{self.BASE_URL}/league/{league_key}/transactions"
        params = {'format': 'json'}
        if transaction_type:
            params['type'] = transaction_type
        if count:
            params['count'] = count
        
        response = self._make_request(url, params=params)
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
    
    def get_league_scoreboard(self, league_key: str, week: Optional[int] = None) -> Dict[str, Any]:
        """
        Get league scoreboard/matchups for a specific week.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            week: Week number (default: current week)
            
        Returns:
            dict: Scoreboard/matchup data from API
            
        Raises:
            Exception: If API call fails or response cannot be parsed
        """
        if week:
            url = f"{self.BASE_URL}/league/{league_key}/scoreboard;week={week}"
        else:
            url = f"{self.BASE_URL}/league/{league_key}/scoreboard"
        
        response = self._make_request(url, params={'format': 'json'})
        
        if not response.text or not response.text.strip():
            raise Exception(f"Empty response from API. Status: {response.status_code}")
        
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f"Failed to parse JSON response. Status: {response.status_code}, Response: {response.text[:200]}")
    
    def _calculate_expected_wins_losses(self, league_key: str) -> Dict[str, Dict[str, float]]:
        """
        Calculate expected wins and losses for each team based on record percentage vs all.
        An expected win is when record_percentage_vs_all > 50%, expected loss when < 50%.
        Only includes completed weeks (excludes current week and future weeks).
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            dict: Dictionary mapping team_key -> {'expected_wins': float, 'expected_losses': float}
        """
        expected_stats = {}
        
        # Get league info to determine current week (incomplete weeks)
        league_info = self.get_league_info(league_key)
        current_week = None
        if league_info:
            try:
                current_week = int(league_info.get('current_week', 1))
            except (ValueError, TypeError):
                current_week = None
        
        # Get weekly performance data
        weekly_performance_df = self.get_weekly_team_performance_dataframe(league_key)
        if weekly_performance_df is None or weekly_performance_df.empty:
            return expected_stats
        
        # Filter out incomplete weeks (current week and future weeks)
        if current_week is not None:
            weekly_performance_df = weekly_performance_df[weekly_performance_df['week'] < current_week]
        
        if weekly_performance_df.empty:
            return expected_stats
        
        # Initialize expected stats for all teams
        for _, row in weekly_performance_df.iterrows():
            team_key = row.get('team_key')
            if team_key and team_key not in expected_stats:
                expected_stats[team_key] = {'expected_wins': 0.0, 'expected_losses': 0.0}
        
        # Calculate expected wins/losses per week (only for completed weeks)
        for _, row in weekly_performance_df.iterrows():
            team_key = row.get('team_key')
            record_pct = row.get('record_percentage_vs_all', 0.0)
            
            try:
                record_pct = float(record_pct)
            except (ValueError, TypeError):
                record_pct = 0.0
            
            if team_key and team_key in expected_stats:
                if record_pct > 50.0:
                    expected_stats[team_key]['expected_wins'] += 1.0
                elif record_pct < 50.0:
                    expected_stats[team_key]['expected_losses'] += 1.0
                # If exactly 50%, don't count as either (or could count as 0.5 each)
                # For now, we'll leave it as 0 for both
        
        return expected_stats
    
    def get_teams_stats_dataframe(self, league_key: str, force_refresh: bool = False) -> Optional['pd.DataFrame']:
        """
        Get all team stats for a league and return as a pandas DataFrame.
        Uses standings endpoint which includes team stats and standings information.
        Also includes expected wins and losses based on record percentage vs all.
        Uses cache for prior seasons if available.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            force_refresh: If True, bypass cache and fetch from API
            
        Returns:
            pd.DataFrame: DataFrame containing team stats, or None if pandas is not available
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for this functionality. Install it with: pip install pandas")
        
        # Check cache first (unless forcing refresh)
        if self.use_cache and not force_refresh:
            cached_data = self.cache_manager.get(league_key, 'teams_stats')
            if cached_data is not None:
                try:
                    df = pd.DataFrame(cached_data)
                    # Apply nickname mapping to cached data
                    df = self._apply_nickname_mapping_to_df(df, league_key)
                    return df
                except Exception:
                    # If cached data is invalid, continue to fetch from API
                    pass
        
        # Get league info to check if prior season
        league_info = self.get_league_info(league_key, force_refresh=force_refresh)
        is_prior_season = self.cache_manager.is_prior_season(league_info) if self.use_cache and league_info else False
        
        # Try to get standings first (includes team stats)
        try:
            standings_data = self.get_league_standings(league_key)
            team_stats_list = self._parse_standings(standings_data, league_key)
            if team_stats_list:
                df = pd.DataFrame(team_stats_list)
                
                # Add expected wins and losses
                expected_stats = self._calculate_expected_wins_losses(league_key)
                # Initialize columns with 0.0 for all teams
                df['expected_wins'] = 0.0
                df['expected_losses'] = 0.0
                df['expected_win_percentage'] = 0.0
                df['win_percentage_difference'] = 0.0
                
                if expected_stats:
                    # Update with calculated values
                    for team_key, stats in expected_stats.items():
                        mask = df['team_key'] == team_key
                        expected_wins = round(stats.get('expected_wins', 0.0), 1)
                        expected_losses = round(stats.get('expected_losses', 0.0), 1)
                        
                        df.loc[mask, 'expected_wins'] = expected_wins
                        df.loc[mask, 'expected_losses'] = expected_losses
                        
                        # Calculate expected win percentage
                        total_expected_games = expected_wins + expected_losses
                        if total_expected_games > 0:
                            expected_win_pct = (expected_wins / total_expected_games) * 100
                            df.loc[mask, 'expected_win_percentage'] = round(expected_win_pct, 3)
                        
                        # Calculate difference between actual and expected win percentage
                        actual_win_pct = df.loc[mask, 'win_percentage'].values
                        if len(actual_win_pct) > 0:
                            try:
                                actual_win_pct_val = float(actual_win_pct[0])
                                # Convert to percentage if stored as decimal (0.0-1.0)
                                if actual_win_pct_val <= 1.0:
                                    actual_win_pct_val = actual_win_pct_val * 100
                                
                                if total_expected_games > 0:
                                    expected_win_pct_val = (expected_wins / total_expected_games) * 100
                                    difference = actual_win_pct_val - expected_win_pct_val
                                    df.loc[mask, 'win_percentage_difference'] = round(difference, 3)
                            except (ValueError, TypeError):
                                pass
                
                # Cache the result if it's a prior season
                if self.use_cache and is_prior_season:
                    # Convert DataFrame to dict for caching
                    df_dict = df.to_dict(orient='records')
                    self.cache_manager.set(league_key, 'teams_stats', df_dict)
                
                return df
            else:
                import logging
                logging.debug(f"Standings parsed but returned empty list, trying individual team stats")
        except Exception as e:
            import logging
            logging.debug(f"Failed to get standings, trying individual team stats: {e}")
        
        # Fallback: get individual team stats
        teams = self.get_league_teams(league_key)
        if not teams:
            return None
        
        # Collect team stats
        team_stats_list = []
        
        for team in teams:
            if not isinstance(team, dict):
                continue
            
            team_key = team.get('team_key')
            if not team_key:
                continue
            
            try:
                stats_data = self.get_team_stats(team_key)
                team_stats = self._parse_team_stats(stats_data, team_key)
                if team_stats:
                    team_stats_list.append(team_stats)
            except Exception as e:
                # Log error but continue with other teams
                import logging
                logging.debug(f"Failed to get stats for team {team_key}: {e}")
        
        if not team_stats_list:
            return None
        
        # Create DataFrame
        df = pd.DataFrame(team_stats_list)
        
        # Add expected wins and losses
        expected_stats = self._calculate_expected_wins_losses(league_key)
        # Initialize columns with 0.0 for all teams
        df['expected_wins'] = 0.0
        df['expected_losses'] = 0.0
        
        if expected_stats:
            # Update with calculated values
            for team_key, stats in expected_stats.items():
                mask = df['team_key'] == team_key
                df.loc[mask, 'expected_wins'] = round(stats.get('expected_wins', 0.0), 1)
                df.loc[mask, 'expected_losses'] = round(stats.get('expected_losses', 0.0), 1)
        
        return df
    
    # ==================== Private Parsing Methods ====================
    
    def _parse_games(self, games_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse games from Yahoo API response.
        
        Args:
            games_data: Raw games data from get_user_games()
            
        Returns:
            list: List of game dictionaries
        """
        fantasy_content = games_data.get('fantasy_content', {})
        if not fantasy_content:
            return []
        
        # Extract user from response
        user = self._extract_user(fantasy_content)
        if not user:
            return []
        
        games_obj = user.get('games', {})
        if not games_obj:
            return []
        
        # Extract games from numeric keys
        games = []
        if isinstance(games_obj, dict):
            for key in games_obj.keys():
                if key != 'count' and key.isdigit():
                    game_data = games_obj.get(key, {})
                    if isinstance(game_data, dict):
                        if 'game' in game_data:
                            game = game_data.get('game')
                            if isinstance(game, dict):
                                games.append(game)
                            elif isinstance(game, list) and len(game) > 0:
                                games.extend([g for g in game if isinstance(g, dict)])
                        else:
                            games.append(game_data)
        
        return games
    
    def parse_leagues_from_games(self, games: List[Dict[str, Any]], game_key: str) -> List[Dict[str, Any]]:
        """
        Parse leagues from parsed games list (when games endpoint includes leagues).
        
        Args:
            games: List of parsed game dictionaries
            game_key: Game key to filter leagues for
            
        Returns:
            list: List of league dictionaries
        """
        for game in games:
            if str(game.get('game_key')) == str(game_key) and 'leagues' in game:
                return self.extract_leagues_from_dict(game.get('leagues', {}))
        return []
    
    def _parse_leagues(self, leagues_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse leagues from leagues API response.
        
        Args:
            leagues_data: Raw leagues data from get_leagues()
            
        Returns:
            list: List of league dictionaries
        """
        fantasy_content = leagues_data.get('fantasy_content', {})
        if not fantasy_content:
            return []
        
        users = fantasy_content.get('users', {})
        if not isinstance(users, dict):
            return []
        
        # Find user key
        user_key = None
        for key in users.keys():
            if key != 'count' and key.isdigit():
                user_key = key
                break
        
        if not user_key:
            return []
        
        user_data = users.get(user_key, {})
        if not isinstance(user_data, dict) or 'user' not in user_data:
            return []
        
        user_list = user_data.get('user', [])
        if not isinstance(user_list, list) or len(user_list) <= 1:
            return []
        
        user = user_list[1]
        games_obj = user.get('games', {})
        if not isinstance(games_obj, dict):
            return []
        
        # Find the game with leagues
        for key in games_obj.keys():
            if key != 'count' and key.isdigit():
                game_data = games_obj.get(key, {})
                if isinstance(game_data, dict) and 'game' in game_data:
                    game_raw = game_data.get('game', {})
                    
                    # Game can be a list where game[1] contains leagues
                    if isinstance(game_raw, list):
                        for game_item in game_raw:
                            if isinstance(game_item, dict) and 'leagues' in game_item:
                                return self.extract_leagues_from_dict(game_item.get('leagues', {}))
                    elif isinstance(game_raw, dict) and 'leagues' in game_raw:
                        return self.extract_leagues_from_dict(game_raw.get('leagues', {}))
        
        return []
    
    def _parse_league_info(self, league_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse league information from league API response.
        
        Args:
            league_data: Raw league data from get_league_info()
            
        Returns:
            dict: Parsed league information, or None if not found
        """
        fantasy_content = league_data.get('fantasy_content', {})
        league_raw = fantasy_content.get('league', {})
        
        # Handle league as list or dict with numeric keys
        league_info = None
        if isinstance(league_raw, list) and len(league_raw) > 0:
            league_info = league_raw[0] if isinstance(league_raw[0], dict) else {}
        elif isinstance(league_raw, dict):
            for key in league_raw.keys():
                if key != 'count' and key.isdigit():
                    league_data_item = league_raw.get(key, {})
                    if isinstance(league_data_item, dict):
                        if 'league' in league_data_item:
                            league_nested = league_data_item.get('league', {})
                            if isinstance(league_nested, list) and len(league_nested) > 0:
                                league_info = league_nested[0] if isinstance(league_nested[0], dict) else {}
                            elif isinstance(league_nested, dict):
                                league_info = league_nested
                        else:
                            league_info = league_data_item
                        break
        
        return league_info if league_info else None
    
    def _parse_teams(self, teams_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse teams from teams API response.
        
        Args:
            teams_data: Raw teams data from get_league_teams()
            
        Returns:
            list: List of team dictionaries
        """
        teams_fantasy = teams_data.get('fantasy_content', {})
        teams_league_raw = teams_fantasy.get('league', {})
        
        # Handle league as list or dict with numeric keys
        teams_obj = None
        
        if isinstance(teams_league_raw, list):
            # Check all elements in the list for teams
            for item in teams_league_raw:
                if isinstance(item, dict) and 'teams' in item:
                    teams_obj = item.get('teams', {})
                    break
        elif isinstance(teams_league_raw, dict):
            # Numeric key structure
            for key in teams_league_raw.keys():
                if key != 'count' and key.isdigit():
                    league_data = teams_league_raw.get(key, {})
                    if isinstance(league_data, dict):
                        if 'league' in league_data:
                            league_nested = league_data.get('league', {})
                            if isinstance(league_nested, list):
                                for item in league_nested:
                                    if isinstance(item, dict) and 'teams' in item:
                                        teams_obj = item.get('teams', {})
                                        break
                            elif isinstance(league_nested, dict) and 'teams' in league_nested:
                                teams_obj = league_nested.get('teams', {})
                        elif 'teams' in league_data:
                            teams_obj = league_data.get('teams', {})
                        break
        
        if not isinstance(teams_obj, dict):
            return []
        
        # Extract teams from numeric keys
        teams = []
        for key in teams_obj.keys():
            if key != 'count' and key.isdigit():
                team_data = teams_obj.get(key, {})
                if isinstance(team_data, dict) and 'team' in team_data:
                    team_raw = team_data.get('team', {})
                    # Yahoo API returns teams as nested lists of single-key dicts
                    if isinstance(team_raw, list) and len(team_raw) > 0:
                        team_data_list = team_raw[0] if isinstance(team_raw[0], list) else team_raw
                        # Combine all single-key dictionaries into one team dict
                        team = {}
                        for item in team_data_list:
                            if isinstance(item, dict):
                                team.update(item)
                        if team:
                            teams.append(team)
                    elif isinstance(team_raw, dict):
                        teams.append(team_raw)
        
        return teams
    
    def _parse_team_stats(self, stats_data: Dict[str, Any], team_key: str) -> Optional[Dict[str, Any]]:
        """
        Parse team stats from API response.
        
        Args:
            stats_data: Raw team stats data from get_team_stats()
            team_key: Team key for reference
            
        Returns:
            dict: Parsed team stats dictionary
        """
        fantasy_content = stats_data.get('fantasy_content', {})
        team_obj = fantasy_content.get('team', {})
        
        # Handle team as list or dict with numeric keys
        team_info = None
        if isinstance(team_obj, list) and len(team_obj) > 0:
            # Find the element with stats
            for item in team_obj:
                if isinstance(item, dict) and 'team_stats' in item:
                    team_info = item
                    break
            if not team_info and len(team_obj) > 0:
                team_info = team_obj[0] if isinstance(team_obj[0], dict) else {}
        elif isinstance(team_obj, dict):
            # Numeric key structure
            for key in team_obj.keys():
                if key != 'count' and key.isdigit():
                    team_data = team_obj.get(key, {})
                    if isinstance(team_data, dict):
                        if 'team' in team_data:
                            team_nested = team_data.get('team', {})
                            if isinstance(team_nested, list):
                                for item in team_nested:
                                    if isinstance(item, dict) and 'team_stats' in item:
                                        team_info = item
                                        break
                            elif isinstance(team_nested, dict) and 'team_stats' in team_nested:
                                team_info = team_nested
                        elif 'team_stats' in team_data:
                            team_info = team_data
                        break
        
        if not team_info:
            return None
        
        # Extract basic team info
        parsed_stats = {
            'team_key': team_key,
            'team_name': team_info.get('name', 'N/A'),
            'team_id': team_info.get('team_id', 'N/A'),
        }
        
        # Extract team stats if available
        team_stats = team_info.get('team_stats', {})
        if isinstance(team_stats, dict):
            # Extract stats from team_stats structure
            stats = team_stats.get('stats', {})
            if isinstance(stats, dict):
                for key in stats.keys():
                    if key != 'count' and key.isdigit():
                        stat_data = stats.get(key, {})
                        if isinstance(stat_data, dict):
                            stat = stat_data.get('stat', {})
                            if isinstance(stat, dict):
                                stat_id = stat.get('stat_id', '')
                                value = stat.get('value', '')
                                if stat_id:
                                    parsed_stats[f'stat_{stat_id}'] = value
        
        return parsed_stats
    
    def _parse_standings(self, standings_data: Dict[str, Any], league_key: str = '') -> List[Dict[str, Any]]:
        """
        Parse team standings/stats from league standings API response.
        
        Args:
            standings_data: Raw standings data from get_league_standings()
            league_key: League key for nickname mapping (optional)
            
        Returns:
            list: List of team stats dictionaries
        """
        fantasy_content = standings_data.get('fantasy_content', {})
        league_obj = fantasy_content.get('league', {})
        
        # Handle league as list or dict with numeric keys
        standings = None
        teams_obj = None
        
        if isinstance(league_obj, list):
            # League is a list - index 0 is league info, index 1 has standings
            if len(league_obj) > 1:
                standings_item = league_obj[1]
                if isinstance(standings_item, dict) and 'standings' in standings_item:
                    standings = standings_item.get('standings', {})
                    # Standings is also a list, first element has teams
                    if isinstance(standings, list) and len(standings) > 0:
                        teams_obj = standings[0].get('teams', {}) if isinstance(standings[0], dict) else {}
        elif isinstance(league_obj, dict):
            # Numeric key structure
            for key in league_obj.keys():
                if key != 'count' and key.isdigit():
                    league_data = league_obj.get(key, {})
                    if isinstance(league_data, dict):
                        if 'league' in league_data:
                            league_nested = league_data.get('league', {})
                            if isinstance(league_nested, list) and len(league_nested) > 1:
                                standings_item = league_nested[1]
                                if isinstance(standings_item, dict) and 'standings' in standings_item:
                                    standings = standings_item.get('standings', {})
                                    if isinstance(standings, list) and len(standings) > 0:
                                        teams_obj = standings[0].get('teams', {}) if isinstance(standings[0], dict) else {}
                        elif 'standings' in league_data:
                            standings = league_data.get('standings', {})
                            if isinstance(standings, list) and len(standings) > 0:
                                teams_obj = standings[0].get('teams', {}) if isinstance(standings[0], dict) else {}
                        break
        
        if not isinstance(teams_obj, dict):
            import logging
            logging.debug(f"Teams not found in standings. League object type: {type(league_obj)}")
            return []
        
        # Extract teams from teams_obj
        teams_list = []
        for key in teams_obj.keys():
            if key != 'count' and key.isdigit():
                team_data = teams_obj.get(key, {})
                if isinstance(team_data, dict) and 'team' in team_data:
                    team_raw = team_data.get('team', {})
                    
                    # Extract team info - team_raw is a list where:
                    # team[0] is a list of single-key dicts with team info
                    # team[1] is team_points
                    # team[2] is team_standings
                    team_info = {}
                    team_standings = None
                    
                    if isinstance(team_raw, list):
                        # First element is list of team info dicts
                        if len(team_raw) > 0 and isinstance(team_raw[0], list):
                            for item in team_raw[0]:
                                if isinstance(item, dict):
                                    # Extract key-value pairs from single-key dicts
                                    for k, v in item.items():
                                        # Handle managers structure
                                        if k == 'managers' and isinstance(v, list):
                                            # Extract manager nickname from managers list
                                            for manager_item in v:
                                                if isinstance(manager_item, dict) and 'manager' in manager_item:
                                                    manager = manager_item.get('manager', {})
                                                    if isinstance(manager, dict):
                                                        team_info['manager_nickname'] = manager.get('nickname', 'N/A')
                                                        # Apply nickname mapping if available (auto-adds to CSV if not found)
                                                        if self.nickname_mapper and team_info.get('manager_nickname') == "--hidden--":
                                                            team_name = team_info.get('name', 'N/A')
                                                            # Extract season from league_key (format: {game_id}.l.{league_id})
                                                            season = league_key.split('.')[0] if '.' in league_key and league_key else ''
                                                            if season and season.isdigit() and team_name != 'N/A':
                                                                # Apply mapping (will auto-add to CSV with "FIXME" if not found)
                                                                team_info['manager_nickname'] = self.nickname_mapper.apply_mapping(
                                                                    team_name, league_key, season, team_info['manager_nickname']
                                                                )
                                                        break  # Use first manager's nickname
                                        elif isinstance(v, dict):
                                            team_info.update(v)
                                        elif not isinstance(v, list):
                                            team_info[k] = v
                        
                        # Second element has team_points
                        # Third element has team_standings
                        if len(team_raw) > 2 and isinstance(team_raw[2], dict):
                            team_standings = team_raw[2].get('team_standings', {})
                    elif isinstance(team_raw, dict):
                        team_info = team_raw
                        team_standings = team_info.get('team_standings', {})
                    
                    # Extract basic team info
                    manager_nickname = team_info.get('manager_nickname', 'N/A')
                    team_name = team_info.get('name', 'N/A')
                    team_key = team_info.get('team_key', 'N/A')
                    
                    # Apply nickname mapping if available (auto-adds to CSV if not found)
                    if self.nickname_mapper and manager_nickname == "--hidden--":
                        # Extract season from league_key (format: {game_id}.l.{league_id})
                        # Season is typically the first part before the dot
                        season = league_key.split('.')[0] if '.' in league_key else ''
                        if season and season.isdigit():
                            # Apply mapping (will auto-add to CSV with "FIXME" if not found)
                            manager_nickname = self.nickname_mapper.apply_mapping(
                                team_name, league_key, season, manager_nickname
                            )
                    
                    parsed_team = {
                        'team_key': team_key,
                        'team_name': team_name,
                        'team_id': team_info.get('team_id', 'N/A'),
                        'number_of_moves': team_info.get('number_of_moves', 'N/A'),
                        'number_of_trades': team_info.get('number_of_trades', 'N/A'),
                        'faab_balance': team_info.get('faab_balance', 'N/A'),
                        'draft_grade': team_info.get('draft_grade', 'N/A'),
                        'manager_nickname': manager_nickname,
                    }
                    
                    # Add standings info if available
                    if isinstance(team_standings, dict):
                        parsed_team['rank'] = team_standings.get('rank', 'N/A')
                        outcome_totals = team_standings.get('outcome_totals', {})
                        if isinstance(outcome_totals, dict):
                            parsed_team['wins'] = outcome_totals.get('wins', 'N/A')
                            parsed_team['losses'] = outcome_totals.get('losses', 'N/A')
                            parsed_team['ties'] = outcome_totals.get('ties', 'N/A')
                        parsed_team['points_for'] = team_standings.get('points_for', 'N/A')
                        parsed_team['points_against'] = team_standings.get('points_against', 'N/A')
                        parsed_team['win_percentage'] = outcome_totals.get('percentage', 'N/A') if isinstance(outcome_totals, dict) else 'N/A'
                    else:
                        parsed_team['rank'] = 'N/A'
                        parsed_team['wins'] = 'N/A'
                        parsed_team['losses'] = 'N/A'
                        parsed_team['ties'] = 'N/A'
                        parsed_team['points_for'] = 'N/A'
                        parsed_team['points_against'] = 'N/A'
                        parsed_team['win_percentage'] = 'N/A'
                    
                    teams_list.append(parsed_team)
        
        return teams_list
    
    def get_weekly_dataframe(self, league_key: str, start_week: Optional[int] = None, end_week: Optional[int] = None, force_refresh: bool = False) -> Optional['pd.DataFrame']:
        """
        Get weekly matchup data for a league and return as a pandas DataFrame.
        Uses cache for prior seasons if available.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            start_week: Starting week number (default: 1)
            end_week: Ending week number (default: current week from league info)
            force_refresh: If True, bypass cache and fetch from API
            
        Returns:
            pd.DataFrame: DataFrame containing weekly matchup data, or None if pandas is not available
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for this functionality. Install it with: pip install pandas")
        
        # Get league info to determine available weeks
        league_info = self.get_league_info(league_key, force_refresh=force_refresh)
        if not league_info:
            return None
        
        is_prior_season = self.cache_manager.is_prior_season(league_info) if self.use_cache and league_info else False
        
        current_week = int(league_info.get('current_week', 1))
        start = int(league_info.get('start_week', 1))
        end = int(league_info.get('end_week', current_week))
        
        # Override with provided parameters if specified
        if start_week is not None:
            start = start_week
        if end_week is not None:
            end = end_week
        
        # Check cache first for prior seasons (unless forcing refresh)
        if self.use_cache and not force_refresh and is_prior_season:
            cached_data = self.cache_manager.get(league_key, 'weekly_data')
            if cached_data is not None:
                try:
                    df = pd.DataFrame(cached_data)
                    # Filter by week range if specified
                    if start_week is not None or end_week is not None:
                        df = df[(df['week'] >= start) & (df['week'] <= end)]
                    return df
                except Exception:
                    # If cached data is invalid, continue to fetch from API
                    pass
        
        # Collect weekly data
        weekly_data_list = []
        
        for week in range(start, min(end + 1, current_week + 1)):
            try:
                scoreboard_data = self.get_league_scoreboard(league_key, week)
                week_matchups = self._parse_scoreboard(scoreboard_data, week, league_key)
                if week_matchups:
                    weekly_data_list.extend(week_matchups)
            except Exception as e:
                import logging
                logging.debug(f"Failed to get scoreboard for week {week}: {e}")
        
        if not weekly_data_list:
            return None
        
        # Create DataFrame
        df = pd.DataFrame(weekly_data_list)
        
        # Add record_percentage_vs_all column (percentage of teams each team would beat that week)
        df = self._add_record_percentage_vs_all_to_weekly_df(df)
        
        # Cache the result if it's a prior season
        if self.use_cache and is_prior_season:
            # Convert DataFrame to dict for caching
            df_dict = df.to_dict(orient='records')
            self.cache_manager.set(league_key, 'weekly_data', df_dict)
        
        return df
    
    def _parse_scoreboard(self, scoreboard_data: Dict[str, Any], week: int, league_key: str = '') -> List[Dict[str, Any]]:
        """
        Parse scoreboard/matchup data from API response.
        
        Args:
            scoreboard_data: Raw scoreboard data from get_league_scoreboard()
            week: Week number
            league_key: League key for nickname mapping (optional)
            
        Returns:
            list: List of weekly matchup dictionaries
        """
        fantasy_content = scoreboard_data.get('fantasy_content', {})
        league_obj = fantasy_content.get('league', {})
        
        # Handle league as list or dict with numeric keys
        scoreboard = None
        
        if isinstance(league_obj, list):
            # League is a list - index 0 is league info, index 1 has scoreboard
            if len(league_obj) > 1:
                scoreboard_item = league_obj[1]
                if isinstance(scoreboard_item, dict) and 'scoreboard' in scoreboard_item:
                    scoreboard = scoreboard_item.get('scoreboard', {})
        elif isinstance(league_obj, dict):
            # Numeric key structure
            for key in league_obj.keys():
                if key != 'count' and key.isdigit():
                    league_data = league_obj.get(key, {})
                    if isinstance(league_data, dict):
                        if 'league' in league_data:
                            league_nested = league_data.get('league', {})
                            if isinstance(league_nested, list):
                                for item in league_nested:
                                    if isinstance(item, dict) and 'scoreboard' in item:
                                        scoreboard = item.get('scoreboard', {})
                                        break
                            elif isinstance(league_nested, dict) and 'scoreboard' in league_nested:
                                scoreboard = league_nested.get('scoreboard', {})
                        elif 'scoreboard' in league_data:
                            scoreboard = league_data.get('scoreboard', {})
                        break
        
        if not isinstance(scoreboard, dict):
            return []
        
        # Extract matchups - scoreboard may have numeric keys
        matchups_list = []
        
        # Check if scoreboard has numeric keys (like '0') containing matchups
        if '0' in scoreboard and isinstance(scoreboard.get('0'), dict):
            scoreboard_week = scoreboard.get('0', {})
            matchups = scoreboard_week.get('matchups', {})
        else:
            matchups = scoreboard.get('matchups', {})
        
        if isinstance(matchups, dict):
            for key in matchups.keys():
                if key != 'count' and key.isdigit():
                    matchup_data = matchups.get(key, {})
                    if isinstance(matchup_data, dict) and 'matchup' in matchup_data:
                        matchup_raw = matchup_data.get('matchup', {})
                        
                        # Parse matchup - can be list or dict with numeric keys
                        teams_in_matchup = []
                        
                        if isinstance(matchup_raw, dict):
                            # Check for numeric keys (like '0', '1') containing teams
                            for matchup_key in matchup_raw.keys():
                                if matchup_key.isdigit():
                                    teams_obj = matchup_raw.get(matchup_key, {})
                                    if isinstance(teams_obj, dict) and 'teams' in teams_obj:
                                        teams_dict = teams_obj.get('teams', {})
                                        if isinstance(teams_dict, dict):
                                            for team_key in teams_dict.keys():
                                                if team_key != 'count' and team_key.isdigit():
                                                    team_data = teams_dict.get(team_key, {})
                                                    if isinstance(team_data, dict) and 'team' in team_data:
                                                        team_info = self._extract_team_from_matchup(team_data.get('team', {}), league_key)
                                                        if team_info:
                                                            teams_in_matchup.append(team_info)
                                    elif isinstance(teams_obj, dict) and 'team' in teams_obj:
                                        team_info = self._extract_team_from_matchup(teams_obj.get('team', {}), league_key)
                                        if team_info:
                                            teams_in_matchup.append(team_info)
                        elif isinstance(matchup_raw, list):
                            # Extract teams from matchup list
                            for item in matchup_raw:
                                if isinstance(item, dict):
                                    if 'teams' in item:
                                        # Teams are nested
                                        teams_obj = item.get('teams', {})
                                        if isinstance(teams_obj, dict):
                                            for team_key in teams_obj.keys():
                                                if team_key != 'count' and team_key.isdigit():
                                                    team_data = teams_obj.get(team_key, {})
                                                    if isinstance(team_data, dict) and 'team' in team_data:
                                                        team_info = self._extract_team_from_matchup(team_data.get('team', {}), league_key)
                                                        if team_info:
                                                            teams_in_matchup.append(team_info)
                                    elif 'team' in item:
                                        team_info = self._extract_team_from_matchup(item.get('team', {}), league_key)
                                        if team_info:
                                            teams_in_matchup.append(team_info)
                            
                        # Create matchup entries
                        if len(teams_in_matchup) >= 2:
                            # Two teams matched up
                            team1 = teams_in_matchup[0]
                            team2 = teams_in_matchup[1]
                            
                            # Calculate winner
                            try:
                                team1_points = float(str(team1.get('points', 0)).replace('N/A', '0'))
                                team2_points = float(str(team2.get('points', 0)).replace('N/A', '0'))
                                winner = team1.get('name', 'N/A') if team1_points > team2_points else (team2.get('name', 'N/A') if team2_points > team1_points else 'Tie')
                            except (ValueError, TypeError):
                                winner = 'N/A'
                            
                            matchups_list.append({
                                'week': week,
                                'team1_name': team1.get('name', 'N/A'),
                                'team1_key': team1.get('team_key', 'N/A'),
                                'team1_points': team1.get('points', 'N/A'),
                                'team2_name': team2.get('name', 'N/A'),
                                'team2_key': team2.get('team_key', 'N/A'),
                                'team2_points': team2.get('points', 'N/A'),
                                'winner': winner,
                            })
        
        return matchups_list
    
    def _add_record_percentage_vs_all_to_weekly_df(self, df: 'pd.DataFrame') -> 'pd.DataFrame':
        """
        Add a record_percentage_vs_all column to weekly dataframe.
        This calculates what percentage of teams each team would beat in a given week.
        
        Args:
            df: Weekly matchup DataFrame with columns: week, team1_key, team1_points, team2_key, team2_points
            
        Returns:
            pd.DataFrame: DataFrame with record_percentage_vs_all column added
        """
        if df.empty:
            return df
        
        # Create a list to store team-week records with points
        team_week_points = []
        
        # Extract all team points by week from the matchup dataframe
        for _, row in df.iterrows():
            week = row.get('week')
            team1_key = row.get('team1_key')
            team1_points = row.get('team1_points')
            team2_key = row.get('team2_key')
            team2_points = row.get('team2_points')
            
            # Convert points to float, handling 'N/A' and other non-numeric values
            try:
                team1_pts = float(str(team1_points).replace('N/A', '0')) if team1_points != 'N/A' else 0.0
            except (ValueError, TypeError):
                team1_pts = 0.0
            
            try:
                team2_pts = float(str(team2_points).replace('N/A', '0')) if team2_points != 'N/A' else 0.0
            except (ValueError, TypeError):
                team2_pts = 0.0
            
            team_week_points.append({
                'week': week,
                'team_key': team1_key,
                'points': team1_pts
            })
            team_week_points.append({
                'week': week,
                'team_key': team2_key,
                'points': team2_pts
            })
        
        # Create a DataFrame from team-week points
        team_points_df = pd.DataFrame(team_week_points)
        
        # Calculate record percentage vs all for each team-week combination
        record_percentages = []
        
        for week in team_points_df['week'].unique():
            week_data = team_points_df[team_points_df['week'] == week]
            week_points = week_data['points'].values
            total_teams = len(week_points)
            
            if total_teams == 0:
                continue
            
            # For each team in this week, calculate percentage of teams they would beat
            for _, team_row in week_data.iterrows():
                team_key = team_row['team_key']
                team_points = team_row['points']
                
                # Count how many teams this team would beat (strictly greater)
                teams_beaten = sum(1 for pts in week_points if team_points > pts)
                
                # Calculate percentage (excluding the team itself from denominator)
                # If only one team, set to 0% or handle edge case
                if total_teams <= 1:
                    record_pct = 0.0
                else:
                    record_pct = (teams_beaten / (total_teams - 1)) * 100
                
                record_percentages.append({
                    'week': week,
                    'team_key': team_key,
                    'record_percentage_vs_all': record_pct
                })
        
        # Create DataFrame with record percentages
        record_pct_df = pd.DataFrame(record_percentages)
        
        # Add record_percentage_vs_all to original dataframe
        # We need to add it for both team1 and team2 in each matchup row
        df['team1_record_percentage_vs_all'] = 0.0
        df['team2_record_percentage_vs_all'] = 0.0
        
        for idx, row in df.iterrows():
            week = row.get('week')
            team1_key = row.get('team1_key')
            team2_key = row.get('team2_key')
            
            # Get record percentage vs all for team1
            team1_record = record_pct_df[
                (record_pct_df['week'] == week) & 
                (record_pct_df['team_key'] == team1_key)
            ]
            if not team1_record.empty:
                df.at[idx, 'team1_record_percentage_vs_all'] = team1_record.iloc[0]['record_percentage_vs_all']
            
            # Get record percentage vs all for team2
            team2_record = record_pct_df[
                (record_pct_df['week'] == week) & 
                (record_pct_df['team_key'] == team2_key)
            ]
            if not team2_record.empty:
                df.at[idx, 'team2_record_percentage_vs_all'] = team2_record.iloc[0]['record_percentage_vs_all']
        
        return df
    
    def get_weekly_team_performance_dataframe(self, league_key: str, start_week: Optional[int] = None, end_week: Optional[int] = None, force_refresh: bool = False) -> Optional['pd.DataFrame']:
        """
        Get weekly performance data for all teams with points and record percentage vs all.
        Returns one row per team per week.
        Uses cache for prior seasons if available.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            start_week: Starting week number (default: 1)
            end_week: Ending week number (default: current week from league info)
            force_refresh: If True, bypass cache and fetch from API
            
        Returns:
            pd.DataFrame: DataFrame with columns: week, team_key, team_name, points, record_percentage_vs_all
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for this functionality. Install it with: pip install pandas")
        
        # Get league info to check if prior season
        league_info = self.get_league_info(league_key, force_refresh=force_refresh)
        is_prior_season = self.cache_manager.is_prior_season(league_info) if self.use_cache and league_info else False
        
        # Check cache first for prior seasons (unless forcing refresh)
        if self.use_cache and not force_refresh and is_prior_season:
            cached_data = self.cache_manager.get(league_key, 'weekly_performance')
            if cached_data is not None:
                try:
                    df = pd.DataFrame(cached_data)
                    # Filter by week range if specified
                    if start_week is not None or end_week is not None:
                        league_info = self.get_league_info(league_key, force_refresh=force_refresh)
                        start = start_week if start_week is not None else int(league_info.get('start_week', 1))
                        end = end_week if end_week is not None else int(league_info.get('end_week', int(league_info.get('current_week', 1))))
                        df = df[(df['week'] >= start) & (df['week'] <= end)]
                    return df
                except Exception:
                    # If cached data is invalid, continue to fetch from API
                    pass
        
        # Get the weekly matchup dataframe
        weekly_df = self.get_weekly_dataframe(league_key, start_week, end_week, force_refresh=force_refresh)
        if weekly_df is None or weekly_df.empty:
            return None
        
        # Extract team-week records with points and record percentage vs all
        team_performance_list = []
        
        for _, row in weekly_df.iterrows():
            week = row.get('week')
            
            # Team 1
            team_performance_list.append({
                'week': week,
                'team_key': row.get('team1_key'),
                'team_name': row.get('team1_name'),
                'points': row.get('team1_points'),
                'record_percentage_vs_all': row.get('team1_record_percentage_vs_all', 0.0)
            })
            
            # Team 2
            team_performance_list.append({
                'week': week,
                'team_key': row.get('team2_key'),
                'team_name': row.get('team2_name'),
                'points': row.get('team2_points'),
                'record_percentage_vs_all': row.get('team2_record_percentage_vs_all', 0.0)
            })
        
        # Create DataFrame and remove duplicates (in case a team appears multiple times in a week)
        df = pd.DataFrame(team_performance_list)
        df = df.drop_duplicates(subset=['week', 'team_key'], keep='first')
        df = df.sort_values(['week', 'record_percentage_vs_all'], ascending=[True, False])
        
        return df
    
    def _extract_team_from_matchup(self, team_raw: Any, league_key: str = '') -> Optional[Dict[str, Any]]:
        """
        Extract team info from matchup team structure.
        
        Args:
            team_raw: Team data from matchup (can be list or dict)
            league_key: League key for nickname mapping (optional)
            
        Returns:
            dict: Team info dictionary, or None if not found
        """
        team_info = {}
        
        if isinstance(team_raw, list):
            # Team is a list where:
            # team[0] is a list of single-key dicts with team info (name, team_key, etc.)
            # team[1] is a dict with team_points
            if len(team_raw) > 0:
                # Extract team info from first element (list of single-key dicts)
                if isinstance(team_raw[0], list):
                    for item in team_raw[0]:
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if isinstance(v, dict):
                                    # Handle nested structures
                                    if k == 'managers' and isinstance(v, list):
                                        # Extract manager nickname if needed
                                        for manager_item in v:
                                            if isinstance(manager_item, dict) and 'manager' in manager_item:
                                                manager = manager_item.get('manager', {})
                                                if isinstance(manager, dict):
                                                    team_info['manager_nickname'] = manager.get('nickname', 'N/A')
                                                    # Apply nickname mapping if available (auto-adds to CSV if not found)
                                                    if self.nickname_mapper and team_info.get('manager_nickname') == "--hidden--":
                                                        team_name = team_info.get('name', 'N/A')
                                                        # Extract season from league_key (format: {game_id}.l.{league_id})
                                                        season = league_key.split('.')[0] if '.' in league_key and league_key else ''
                                                        if season and season.isdigit() and team_name != 'N/A':
                                                            # Apply mapping (will auto-add to CSV with "FIXME" if not found)
                                                            team_info['manager_nickname'] = self.nickname_mapper.apply_mapping(
                                                                team_name, league_key, season, team_info['manager_nickname']
                                                            )
                                    else:
                                        team_info.update(v)
                                elif not isinstance(v, list):
                                    team_info[k] = v
                elif isinstance(team_raw[0], dict):
                    # Direct dict structure
                    for k, v in team_raw[0].items():
                        if isinstance(v, dict):
                            team_info.update(v)
                        elif not isinstance(v, list):
                            team_info[k] = v
            
            # Extract points from second element
            if len(team_raw) > 1 and isinstance(team_raw[1], dict):
                if 'team_points' in team_raw[1]:
                    points_data = team_raw[1].get('team_points', {})
                    if isinstance(points_data, dict):
                        team_info['points'] = points_data.get('total', 'N/A')
        elif isinstance(team_raw, dict):
            team_info = team_raw
            # Extract points if available
            if 'team_points' in team_raw:
                points_data = team_raw.get('team_points', {})
                if isinstance(points_data, dict):
                    team_info['points'] = points_data.get('total', 'N/A')
        
        return team_info if team_info else None
    
    # ==================== Helper Methods ====================
    
    def _extract_user(self, fantasy_content: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract user object from fantasy_content.
        
        Args:
            fantasy_content: The fantasy_content dict from API response
            
        Returns:
            dict: User object, or None if not found
        """
        if 'user' in fantasy_content:
            user = fantasy_content.get('user', {})
            if isinstance(user, list):
                return user[0] if len(user) > 0 else {}
            return user if isinstance(user, dict) else {}
        
        if 'users' in fantasy_content:
            users = fantasy_content.get('users', {})
            if isinstance(users, dict):
                # Find first numeric key
                for key in users.keys():
                    if key != 'count' and key.isdigit():
                        user_data = users.get(key, {})
                        if isinstance(user_data, dict) and 'user' in user_data:
                            user_raw = user_data.get('user')
                            if isinstance(user_raw, list) and len(user_raw) > 0:
                                # The second element (index 1) usually has the games
                                return user_raw[1] if len(user_raw) > 1 else user_raw[0]
                            elif isinstance(user_raw, dict):
                                return user_raw
                        elif isinstance(user_data, dict):
                            return user_data
                        break
        
        return None
    
    def extract_leagues_from_dict(self, leagues_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract leagues from a leagues dictionary object.
        
        Args:
            leagues_obj: Leagues dictionary (may have numeric keys)
            
        Returns:
            list: List of league dictionaries
        """
        leagues = []
        if isinstance(leagues_obj, dict):
            for key in leagues_obj.keys():
                if key != 'count' and key.isdigit():
                    league_data = leagues_obj.get(key, {})
                    if isinstance(league_data, dict):
                        if 'league' in league_data:
                            league_raw = league_data.get('league')
                            if isinstance(league_raw, list):
                                for league_item in league_raw:
                                    if isinstance(league_item, dict):
                                        leagues.append(league_item)
                            elif isinstance(league_raw, dict):
                                leagues.append(league_raw)
                        else:
                            leagues.append(league_data)
        return leagues
    
    def is_football_game(self, game: Dict[str, Any]) -> bool:
        """
        Check if a game is a fantasy football game.
        
        Args:
            game: Game dictionary
            
        Returns:
            bool: True if the game is football-related
        """
        game_name = game.get('name', '').lower()
        game_code = game.get('code', '').lower()
        game_type = game.get('type', '').lower()
        
        return (
            'football' in game_name or
            'nfl' in game_name or
            game_code == 'nfl' or
            'football' in game_type
        )
    
    def format_league_info(self, league_info: Dict[str, Any]) -> str:
        """
        Format league information as a string for display.
        
        Args:
            league_info: Parsed league information dictionary
            
        Returns:
            str: Formatted league information string
        """
        if not league_info:
            return "No league information found"
        
        lines = [
            "=" * 50,
            "LEAGUE INFORMATION",
            "=" * 50,
            f"League Name: {league_info.get('name', 'N/A')}",
            f"League Key: {league_info.get('league_key', 'N/A')}",
            f"League ID: {league_info.get('league_id', 'N/A')}",
            f"Season: {league_info.get('season', 'N/A')}",
            f"Number of Teams: {league_info.get('num_teams', 'N/A')}",
            f"Scoring Type: {league_info.get('scoring_type', 'N/A')}",
            f"League Type: {league_info.get('league_type', 'N/A')}",
        ]
        
        # Add settings if available
        settings = league_info.get('settings', [None])[0]
        if settings:
            lines.extend([
                "",
                "Settings:",
                f"  Draft Status: {settings.get('draft_status', 'N/A')}",
                f"  Waiver Type: {settings.get('waiver_type', 'N/A')}",
                f"  Playoff Start Week: {settings.get('playoff_start_week', 'N/A')}",
            ])
        
        return "\n".join(lines)
    
    def format_teams_list(self, teams: List[Dict[str, Any]]) -> str:
        """
        Format teams list as a string for display.
        
        Args:
            teams: List of team dictionaries
            
        Returns:
            str: Formatted teams list string
        """
        if not teams:
            return "No teams found"
        
        lines = [f"\nTeams in League ({len(teams)}):"]
        for team in teams:
            if isinstance(team, dict):
                team_name = team.get('name', 'N/A')
                team_key = team.get('team_key', 'N/A')
                lines.append(f"  - {team_name} ({team_key})")
        
        return "\n".join(lines)
    
    # ==================== Playoff Stats Methods ====================
    
    def get_playoff_start_week(self, league_key: str) -> Optional[int]:
        """
        Get the playoff start week for a league.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            int: Playoff start week number, or None if not found
        """
        league_info = self.get_league_info(league_key)
        if not league_info:
            return None
        
        # Try to get from settings
        settings = league_info.get('settings')
        if settings:
            # Settings can be a list or dict
            if isinstance(settings, list) and len(settings) > 0:
                settings_dict = settings[0] if isinstance(settings[0], dict) else {}
            elif isinstance(settings, dict):
                settings_dict = settings
            else:
                settings_dict = {}
            
            playoff_start = settings_dict.get('playoff_start_week')
            if playoff_start:
                try:
                    return int(playoff_start)
                except (ValueError, TypeError):
                    pass
        
        # Try direct access from league_info
        playoff_start = league_info.get('playoff_start_week')
        if playoff_start:
            try:
                return int(playoff_start)
            except (ValueError, TypeError):
                pass
        
        return None
    
    def get_playoff_weekly_stats(self, league_key: str) -> List[Dict[str, Any]]:
        """
        Get weekly stats for all teams during playoff weeks only.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            list: List of weekly team stats dictionaries for playoff weeks
        """
        playoff_start_week = self.get_playoff_start_week(league_key)
        if not playoff_start_week:
            return []
        
        # Get league info to determine end week
        league_info = self.get_league_info(league_key)
        if not league_info:
            return []
        
        current_week = int(league_info.get('current_week', 1))
        end_week = int(league_info.get('end_week', current_week))
        
        # Get weekly stats starting from playoff week
        return self.get_all_teams_weekly_stats(league_key, start_week=playoff_start_week, end_week=end_week)
    
    def get_playoff_weekly_dataframe(self, league_key: str) -> Optional['pd.DataFrame']:
        """
        Get weekly matchup data for playoff weeks only and return as a pandas DataFrame.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            pd.DataFrame: DataFrame containing weekly matchup data for playoff weeks, or None if pandas is not available
        """
        playoff_start_week = self.get_playoff_start_week(league_key)
        if not playoff_start_week:
            return None
        
        # Get league info to determine end week
        league_info = self.get_league_info(league_key)
        if not league_info:
            return None
        
        current_week = int(league_info.get('current_week', 1))
        end_week = int(league_info.get('end_week', current_week))
        
        # Get weekly data starting from playoff week
        return self.get_weekly_dataframe(league_key, start_week=playoff_start_week, end_week=end_week)
    
    def get_playoff_scoreboard(self, league_key: str, week: Optional[int] = None) -> Dict[str, Any]:
        """
        Get league scoreboard/matchups for a specific playoff week.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            week: Playoff week number (default: current playoff week)
            
        Returns:
            dict: Scoreboard/matchup data from API for the playoff week
            
        Raises:
            Exception: If API call fails, response cannot be parsed, or week is not a playoff week
        """
        playoff_start_week = self.get_playoff_start_week(league_key)
        if not playoff_start_week:
            raise Exception("Could not determine playoff start week for this league")
        
        # If week is specified, verify it's a playoff week
        if week and week < playoff_start_week:
            raise Exception(f"Week {week} is not a playoff week. Playoffs start at week {playoff_start_week}")
        
        # Use the regular scoreboard method
        return self.get_league_scoreboard(league_key, week)
    
    def get_team_playoff_stats(self, team_key: str, league_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Get cumulative playoff stats for a specific team.
        Aggregates stats across all playoff weeks.
        
        Args:
            team_key: Team key (e.g., '461.l.621700.t.1')
            league_key: Optional league key to determine playoff weeks (if not provided, extracted from team_key)
            
        Returns:
            dict: Aggregated playoff stats for the team
        """
        # Extract league_key from team_key if not provided
        if not league_key:
            # Team key format: game_key.league_key.team_key
            parts = team_key.split('.')
            if len(parts) >= 2:
                league_key = '.'.join(parts[:2])
            else:
                raise Exception("Could not extract league_key from team_key")
        
        playoff_start_week = self.get_playoff_start_week(league_key)
        if not playoff_start_week:
            return {}
        
        # Get league info to determine end week
        league_info = self.get_league_info(league_key)
        if not league_info:
            return {}
        
        current_week = int(league_info.get('current_week', 1))
        end_week = int(league_info.get('end_week', current_week))
        
        # Aggregate stats across all playoff weeks
        playoff_stats = {
            'team_key': team_key,
            'playoff_start_week': playoff_start_week,
            'playoff_end_week': end_week,
            'number_of_moves': 0,
            'number_of_trades': 0,
            'faab_spent': 0,
            'faab_balance': 100,  # Starting balance
            'weeks': []
        }
        
        # Get stats for each playoff week
        for week in range(playoff_start_week, min(end_week + 1, current_week + 1)):
            week_stats = self.get_team_stats_by_week(team_key, week)
            if week_stats:
                playoff_stats['weeks'].append(week_stats)
                # Aggregate cumulative stats (use the last week's cumulative values)
                playoff_stats['number_of_moves'] = week_stats.get('number_of_moves', 0)
                playoff_stats['number_of_trades'] = week_stats.get('number_of_trades', 0)
                playoff_stats['faab_balance'] = week_stats.get('faab_balance', 100)
        
        # Calculate FAAB spent (starting - ending balance)
        playoff_stats['faab_spent'] = 100 - playoff_stats['faab_balance']
        
        return playoff_stats
    
    def get_all_teams_playoff_stats(self, league_key: str) -> List[Dict[str, Any]]:
        """
        Get cumulative playoff stats for all teams in a league.
        
        Args:
            league_key: League key (e.g., '461.l.621700')
            
        Returns:
            list: List of aggregated playoff stats dictionaries for all teams
        """
        teams = self.get_league_teams(league_key)
        if not teams:
            return []
        
        playoff_stats_list = []
        for team in teams:
            if not isinstance(team, dict):
                continue
            
            team_key = team.get('team_key')
            if not team_key:
                continue
            
            try:
                team_playoff_stats = self.get_team_playoff_stats(team_key, league_key)
                if team_playoff_stats:
                    # Add team name
                    team_playoff_stats['team_name'] = team.get('name', 'N/A')
                    playoff_stats_list.append(team_playoff_stats)
            except Exception as e:
                import logging
                logging.debug(f"Failed to get playoff stats for team {team_key}: {e}")
        
        return playoff_stats_list


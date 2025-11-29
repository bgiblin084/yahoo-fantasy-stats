"""
Manager Nickname Mapper

Handles mapping of team names to manager nicknames for teams with hidden managers.
Uses a CSV file to store and retrieve mappings.

Author: Braedon Giblin
"""

import csv
import os
from typing import Dict, Optional
import logging


class ManagerNicknameMapper:
    """Maps team names to manager nicknames using a CSV file."""
    
    CSV_FILE = "manager_nicknames.csv"
    CSV_COLUMNS = ['team_name', 'league_key', 'season', 'manager_nickname']
    
    def __init__(self, csv_file: str = None):
        """
        Initialize the manager nickname mapper.
        
        Args:
            csv_file: Optional custom CSV file path (default: manager_nicknames.csv)
        """
        self.csv_file = csv_file or self.CSV_FILE
        self.mappings: Dict[str, str] = {}  # Key: (team_name, league_key, season), Value: manager_nickname
        self._load_mappings()
    
    def _get_key(self, team_name: str, league_key: str, season: str) -> tuple:
        """
        Generate a key for the mappings dictionary.
        
        Args:
            team_name: Team name
            league_key: League key
            season: Season year
            
        Returns:
            tuple: (team_name, league_key, season)
        """
        return (str(team_name).strip(), str(league_key).strip(), str(season).strip())
    
    def _load_mappings(self):
        """Load mappings from CSV file. Creates file if it doesn't exist."""
        if not os.path.exists(self.csv_file):
            # Create empty CSV file with headers
            self._create_csv_file()
            return
        
        try:
            with open(self.csv_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                
                # Verify columns exist
                if not all(col in reader.fieldnames for col in self.CSV_COLUMNS):
                    logging.warning(f"CSV file {self.csv_file} has incorrect columns. Recreating...")
                    self._create_csv_file()
                    return
                
                for row in reader:
                    team_name = row.get('team_name', '').strip()
                    league_key = row.get('league_key', '').strip()
                    season = row.get('season', '').strip()
                    manager_nickname = row.get('manager_nickname', '').strip()
                    
                    if team_name and league_key and season and manager_nickname:
                        key = self._get_key(team_name, league_key, season)
                        self.mappings[key] = manager_nickname
        except Exception as e:
            logging.error(f"Error loading manager nickname mappings: {e}")
            # If file is corrupted, recreate it
            self._create_csv_file()
    
    def _create_csv_file(self):
        """Create a new CSV file with headers."""
        try:
            with open(self.csv_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_COLUMNS)
                writer.writeheader()
            logging.info(f"Created manager nickname CSV file: {self.csv_file}")
        except Exception as e:
            logging.error(f"Error creating manager nickname CSV file: {e}")
    
    def get_manager_nickname(self, team_name: str, league_key: str, season: str) -> Optional[str]:
        """
        Get manager nickname for a team.
        
        Args:
            team_name: Team name
            league_key: League key
            season: Season year
            
        Returns:
            str: Manager nickname if found, None otherwise
        """
        key = self._get_key(team_name, league_key, season)
        return self.mappings.get(key)
    
    def set_manager_nickname(self, team_name: str, league_key: str, season: str, manager_nickname: str) -> bool:
        """
        Set manager nickname for a team and save to CSV.
        
        Args:
            team_name: Team name
            league_key: League key
            season: Season year
            manager_nickname: Manager nickname to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not team_name or not league_key or not season or not manager_nickname:
            return False
        
        key = self._get_key(team_name, league_key, season)
        self.mappings[key] = manager_nickname
        
        # Save to CSV
        return self._save_mappings()
    
    def _save_mappings(self) -> bool:
        """Save all mappings to CSV file."""
        try:
            # Read existing file to preserve any manually added entries
            existing_rows = []
            if os.path.exists(self.csv_file):
                with open(self.csv_file, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames and all(col in reader.fieldnames for col in self.CSV_COLUMNS):
                        existing_rows = [row for row in reader]
            
            # Merge with current mappings
            merged_mappings = {}
            for row in existing_rows:
                key = self._get_key(
                    row.get('team_name', '').strip(),
                    row.get('league_key', '').strip(),
                    row.get('season', '').strip()
                )
                merged_mappings[key] = row.get('manager_nickname', '').strip()
            
            # Update with current mappings
            merged_mappings.update(self.mappings)
            
            # Write back to file
            with open(self.csv_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_COLUMNS)
                writer.writeheader()
                
                for key, nickname in sorted(merged_mappings.items()):
                    team_name, league_key, season = key
                    writer.writerow({
                        'team_name': team_name,
                        'league_key': league_key,
                        'season': season,
                        'manager_nickname': nickname
                    })
            
            return True
        except Exception as e:
            logging.error(f"Error saving manager nickname mappings: {e}")
            return False
    
    def apply_mapping(self, team_name: str, league_key: str, season: str, current_nickname: str) -> str:
        """
        Apply mapping if current nickname is "--hidden--" or empty.
        If not found in CSV, automatically adds entry with "FIXME" as default.
        
        Args:
            team_name: Team name
            league_key: League key
            season: Season year
            current_nickname: Current manager nickname from API
            
        Returns:
            str: Mapped nickname if available, "FIXME" if auto-added, otherwise current nickname
        """
        if current_nickname == "--hidden--" or not current_nickname or current_nickname == "N/A":
            mapped = self.get_manager_nickname(team_name, league_key, season)
            if mapped:
                return mapped
            
            # If not found and nickname is "--hidden--", add to CSV with "FIXME"
            if current_nickname == "--hidden--" and team_name and team_name != 'N/A' and league_key and season:
                # Add entry with FIXME as default
                self.set_manager_nickname(team_name, league_key, season, "FIXME")
                return "FIXME"
        
        return current_nickname
    
    def get_all_mappings(self) -> Dict[tuple, str]:
        """
        Get all current mappings.
        
        Returns:
            dict: Dictionary of (team_name, league_key, season) -> manager_nickname
        """
        return self.mappings.copy()


# yahoo_fantasy_stats.py

"""
Yahoo Fantasy Stats

Author: Braedon Giblin

This script interacts with the Yahoo API to pull historic stats and data for a fantasy football league.
"""

import argparse
import logging
import sys
from oauth import YahooOAuth
from credentials import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI
from yahoo_fantasy_api import YahooFantasyAPI


def authenticate():
    """
    Complete the OAuth authentication flow.
    
    Returns:
        YahooOAuth: Authenticated OAuth client
    """
    logging.info("Initializing Yahoo OAuth...")
    
    # Initialize OAuth 2.0 client with credentials
    oauth = YahooOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
    
    # Try to load existing tokens
    if oauth.load_tokens():
        logging.info("Found existing tokens, attempting to create authenticated session")
        try:
            oauth.create_authenticated_session()
            logging.info("Successfully authenticated with saved tokens")
            return oauth
        except Exception as e:
            logging.warning(f"Saved tokens are invalid or expired: {e}")
            # Try to refresh token if we have a refresh token
            if oauth.refresh_token:
                try:
                    logging.info("Attempting to refresh access token")
                    oauth.refresh_access_token()
                    oauth.save_tokens()
                    oauth.create_authenticated_session()
                    logging.info("Successfully refreshed and authenticated")
                    return oauth
                except Exception as refresh_error:
                    logging.error(f"Token refresh failed: {refresh_error}")
            logging.info("Starting new OAuth flow")
    
    # If no valid tokens, run full OAuth 2.0 flow
    logging.info("Starting OAuth 2.0 flow")
    print("\n=== Starting OAuth 2.0 Flow ===")
    
    try:
        # Step 1: Get authorization URL
        logging.debug("Step 1: Generating authorization URL")
        print("\nStep 1: Generating authorization URL...")
        auth_url = oauth.get_authorization_url()
        logging.debug(f"Authorization URL generated: {auth_url}")
        
        # Step 2: Get authorization from user
        logging.debug("Step 2: Opening browser for authorization")
        print("\nStep 2: Opening browser for authorization...")
        print(f"Authorization URL: {auth_url}")
        oauth.open_authorization_url()
        
        # Step 3: Get authorization code from user
        logging.debug("Step 3: Waiting for user authorization code")
        print("\nStep 3: After authorizing, you'll be redirected and see an authorization code.")
        print("Copy the authorization code from the URL and paste it below:")
        authorization_code = input("Enter authorization code: ").strip()
        
        if not authorization_code:
            raise Exception("Authorization code is required")
        
        # Step 4: Exchange authorization code for access token
        logging.debug("Step 4: Exchanging authorization code for access token")
        print("\nStep 4: Exchanging authorization code for access token...")
        token = oauth.get_access_token(authorization_code)
        logging.info("Access token obtained successfully")
        
        # Step 5: Save tokens for future use
        oauth.save_tokens()
        
        # Step 6: Create authenticated session
        logging.debug("Step 6: Creating authenticated session")
        oauth.create_authenticated_session()
        logging.info("OAuth 2.0 authentication complete")
        
        return oauth
        
    except Exception as e:
        logging.error(f"OAuth authentication failed: {e}", exc_info=True)
        raise




def setup_logging(verbosity=0):
    """
    Configure logging based on verbosity level.
    
    Args:
        verbosity: Verbosity level (0=none/WARNING, 1=INFO, 2=DEBUG)
    """
    if verbosity == 0:
        level = logging.WARNING
        format_str = '%(levelname)s - %(message)s'
    elif verbosity == 1:
        level = logging.INFO
        format_str = '%(levelname)s - %(message)s'
    else:  # verbosity >= 2
        level = logging.DEBUG
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Yahoo Fantasy Stats - Pull historic stats and data for fantasy football leagues',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python yahoo_fantasy_stats.py              # Run with no logging (default)
  python yahoo_fantasy_stats.py -v            # Run with INFO level logging
  python yahoo_fantasy_stats.py -vv           # Run with DEBUG level logging
  python yahoo_fantasy_stats.py --verbose     # Same as -v
  python yahoo_fantasy_stats.py --verbose --verbose  # Same as -vv
        """
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase logging verbosity. Use -v for INFO, -vv for DEBUG. Default: no logging output'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the Yahoo Fantasy Stats application."""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Setup logging based on verbosity level
    setup_logging(verbosity=args.verbose)
    
    logging.info("Starting Yahoo Fantasy Stats application")
    print("Welcome to Yahoo Fantasy Stats!")
    print("=" * 50)
    
    try:
        # Authenticate with Yahoo
        oauth = authenticate()
        
        logging.info("Authentication successful")
        
        # Initialize Yahoo Fantasy API client with OAuth client for automatic token refresh
        logging.debug("Initializing Yahoo Fantasy API client")
        api = YahooFantasyAPI(oauth.oauth_session, oauth_client=oauth)
        
        # Get user's games
        logging.info("Fetching user's games")
        games = api.get_user_games()
        
        if not games:
            logging.warning("No games found for user")
        else:
            logging.info(f"Found {len(games)} game(s)")
            
            # Filter for fantasy football games and extract leagues from games data
            football_games = []
            all_leagues = {}  # Store leagues by game_key
            
            for game in games:
                if isinstance(game, dict):
                    game_key = game.get('game_key', 'N/A')
                    game_name = game.get('name', 'N/A')
                    game_code = game.get('code', '').upper() if game.get('code') else 'N/A'
                    season = game.get('season', 'N/A')
                    
                    logging.debug(f"Processing game: {game_name} (key: {game_key}, season: {season})")
                    
                    # Check if it's a football game using API client method
                    if api.is_football_game(game):
                        football_games.append(game)
                        logging.debug(f"Football game detected: {game_name}")
                        logging.info(f"  [*] {game_name} ({season}) - Key: {game_key} [FOOTBALL - Code: {game_code}]")
                        
                        # Check if this game has leagues in the game data
                        leagues_in_game = api.parse_leagues_from_games([game], game_key)
                        if leagues_in_game:
                            logging.debug(f"Found {len(leagues_in_game)} league(s) in game data for {game_name}")
                            all_leagues[game_key] = leagues_in_game
                    else:
                        logging.info(f"  - {game_name} ({season}) - Key: {game_key}")
            
            # Check for fantasy football leagues
            if football_games:
                logging.info(f"Checking {len(football_games)} fantasy football game(s) for leagues")
                
                for football_game in football_games:
                    game_key = football_game.get('game_key')
                    game_name = football_game.get('name', 'N/A')
                    season = football_game.get('season', 'N/A')
                    
                    logging.debug(f"Processing football game: {game_name} (key: {game_key})")
                    print(f"\n--- {game_name} ({season}) ---")
                    
                    # Check if we already found leagues for this game
                    leagues = all_leagues.get(game_key, [])
                    
                    if not leagues:
                        # Try getting leagues via API endpoint
                        logging.debug(f"Fetching leagues via API for game key: {game_key}")
                        leagues = api.get_leagues(game_key)
                    
                    if not leagues:
                        logging.warning(f"No leagues found for {game_name} ({season})")
                        print(f"  No leagues found for {game_name} ({season})")
                        print(f"  (You may not have joined any leagues yet, or the season hasn't started)")
                    else:
                        logging.info(f"Found {len(leagues)} league(s) for {game_name}")
                        print(f"\nFound {len(leagues)} fantasy football league(s):")
                        for league in leagues:
                            # Handle both list and dict formats
                            if isinstance(league, dict):
                                league_key = league.get('league_key', 'N/A')
                                league_name = league.get('name', 'N/A')
                                logging.info(f"League: {league_name} (key: {league_key})")
                                print(f"  - {league_name}")
                            else:
                                raise ValueError(f"Invalid league format: expected dict, got {type(league)}")
                        
                        # Get detailed info for the first league
                        if leagues:
                            first_league = leagues[0] if isinstance(leagues[0], dict) else {}
                            league_key = first_league.get('league_key')
                            
                            if league_key:
                                logging.info(f"Fetching detailed info for league: {first_league.get('name', 'N/A')}")
                                league_info = api.get_league_info(league_key)
                                if league_info:
                                    print(api.format_league_info(league_info))
                                else:
                                    logging.warning("No league information found in response")
                                
                                # Get teams
                                logging.info(f"Fetching teams for league: {league_key}")
                                teams = api.get_league_teams(league_key)
                                if teams:
                                    logging.info(f"Found {len(teams)} team(s)")
                                    print(api.format_teams_list(teams))
                                    
                                    # Get team stats into pandas DataFrame
                                    try:
                                        logging.info("Fetching team stats...")
                                        team_stats_df = api.get_teams_stats_dataframe(league_key)
                                        if team_stats_df is not None and not team_stats_df.empty:
                                            logging.info(f"Successfully created DataFrame with {len(team_stats_df)} team(s)")
                                            print(f"\nTeam Stats DataFrame:")
                                            print(team_stats_df.to_string())
                                        else:
                                            logging.warning("No team stats data available")
                                    except ImportError as e:
                                        logging.warning(f"pandas not available: {e}")
                                    except Exception as e:
                                        logging.warning(f"Failed to fetch team stats: {e}")
                                    
                                    # Get weekly data into pandas DataFrame
                                    try:
                                        logging.info("Fetching weekly matchup data...")
                                        weekly_df = api.get_weekly_dataframe(league_key)
                                        if weekly_df is not None and not weekly_df.empty:
                                            logging.info(f"Successfully created weekly DataFrame with {len(weekly_df)} matchup(s)")
                                            print(f"\nWeekly Matchup DataFrame:")
                                            print(weekly_df.to_string())
                                        else:
                                            logging.warning("No weekly data available")
                                    except ImportError as e:
                                        logging.warning(f"pandas not available: {e}")
                                    except Exception as e:
                                        logging.warning(f"Failed to fetch weekly data: {e}")
                                else:
                                    logging.warning("No teams found or error extracting teams")
        
        logging.info("Application completed successfully")
        return 0
        
    except Exception as e:
        logging.error(f"Application error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

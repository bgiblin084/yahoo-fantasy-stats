"""
Flask web application for Yahoo Fantasy Stats visualization.

Author: Braedon Giblin
"""

from flask import Flask, render_template, jsonify
import logging
import json
from oauth import YahooOAuth
from credentials import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI
from yahoo_fantasy_api import YahooFantasyAPI

app = Flask(__name__)
app.config['SECRET_KEY'] = 'yahoo-fantasy-stats-secret-key'

# Global variables to store API client and data
api_client = None
team_stats_df = None
weekly_df = None
league_info = None
league_key = None


def initialize_api():
    """Initialize the Yahoo Fantasy API client."""
    global api_client
    
    if api_client is None:
        logging.info("Initializing Yahoo OAuth...")
        oauth = YahooOAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
        
        if oauth.load_tokens():
            try:
                oauth.create_authenticated_session()
                logging.info("Successfully authenticated with saved tokens")
            except Exception as e:
                logging.warning(f"Saved tokens are invalid or expired: {e}")
                if oauth.refresh_token:
                    try:
                        oauth.refresh_access_token()
                        oauth.save_tokens()
                        oauth.create_authenticated_session()
                        logging.info("Successfully refreshed and authenticated")
                    except Exception as refresh_error:
                        logging.error(f"Token refresh failed: {refresh_error}")
                        raise Exception("Authentication failed. Please re-authenticate.")
        
        api_client = YahooFantasyAPI(oauth.oauth_session, oauth_client=oauth)
    
    return api_client


def load_data():
    """Load fantasy league data."""
    global api_client, team_stats_df, weekly_df, league_info, league_key
    
    if api_client is None:
        api_client = initialize_api()
    
    # Get user's games and find first football league
    games = api_client.get_user_games()
    football_leagues = []
    
    for game in games:
        if api_client.is_football_game(game):
            game_key = game.get('game_key')
            leagues = api_client.get_leagues(game_key)
            if leagues:
                football_leagues.extend(leagues)
    
    if not football_leagues:
        raise Exception("No fantasy football leagues found")
    
    # Use first league
    league = football_leagues[0]
    league_key = league.get('league_key')
    
    if not league_key:
        raise Exception("No league key found")
    
    # Get league info
    league_info = api_client.get_league_info(league_key)
    
    # Get team stats DataFrame
    team_stats_df = api_client.get_teams_stats_dataframe(league_key)
    
    # Get weekly DataFrame
    weekly_df = api_client.get_weekly_dataframe(league_key)
    
    return league_info, team_stats_df, weekly_df, league_key


@app.route('/')
def index():
    """Main page displaying league dashboard."""
    try:
        league_info_data, teams_df, weekly_data, lk = load_data()
        
        # Get weekly team stats (moves, trades, FAAB by week)
        weekly_team_stats = api_client.get_all_teams_weekly_stats(lk)
        
        # Convert DataFrames to JSON for JavaScript
        teams_json = teams_df.to_json(orient='records') if teams_df is not None and not teams_df.empty else '[]'
        weekly_json = weekly_df.to_json(orient='records') if weekly_df is not None and not weekly_df.empty else '[]'
        weekly_stats_json = json.dumps(weekly_team_stats) if weekly_team_stats else '[]'
        
        # Ensure league_info is a dict
        if league_info_data is None:
            league_info_data = {}
        
        return render_template('index.html', 
                             league_info=league_info_data,
                             teams_data=teams_json,
                             weekly_data=weekly_json,
                             weekly_stats_data=weekly_stats_json)
    except Exception as e:
        logging.error(f"Error loading data: {e}", exc_info=True)
        return render_template('error.html', error_message=str(e))


@app.route('/api/teams')
def api_teams():
    """API endpoint for team stats data."""
    try:
        _, teams_df, _, _ = load_data()
        if teams_df is not None and not teams_df.empty:
            return jsonify(teams_df.to_dict(orient='records'))
        return jsonify([])
    except Exception as e:
        logging.error(f"Error fetching teams data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/weekly')
def api_weekly():
    """API endpoint for weekly matchup data."""
    try:
        _, _, weekly_data, _ = load_data()
        if weekly_data is not None and not weekly_data.empty:
            return jsonify(weekly_data.to_dict(orient='records'))
        return jsonify([])
    except Exception as e:
        logging.error(f"Error fetching weekly data: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='127.0.0.1', port=5000)


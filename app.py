"""
Flask web application for Yahoo Fantasy Stats visualization.

Author: Braedon Giblin
"""

from flask import Flask, render_template, jsonify, request
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


def get_all_games_and_leagues():
    """Get all football games and their leagues."""
    global api_client
    
    if api_client is None:
        api_client = initialize_api()
    
    games = api_client.get_user_games()
    games_with_leagues = []
    
    for game in games:
        if api_client.is_football_game(game):
            game_key = game.get('game_key')
            game_name = game.get('name', 'N/A')
            season = game.get('season', 'N/A')
            
            leagues = api_client.get_leagues(game_key)
            if leagues:
                games_with_leagues.append({
                    'game_key': game_key,
                    'game_name': game_name,
                    'season': season,
                    'leagues': leagues
                })
    
    return games_with_leagues


def load_data(league_key=None):
    """Load fantasy league data for a specific league."""
    global api_client, team_stats_df, weekly_df, league_info
    
    if api_client is None:
        api_client = initialize_api()
    
    # If no league_key provided, get the first available league
    if league_key is None:
        games_with_leagues = get_all_games_and_leagues()
        if not games_with_leagues:
            raise Exception("No fantasy football leagues found")
        
        # Use first league from first game
        league_key = games_with_leagues[0]['leagues'][0].get('league_key')
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
        # Get league_key from query parameter if provided
        league_key = request.args.get('league_key', None)
        
        # Get all games and leagues for dropdown
        games_with_leagues = get_all_games_and_leagues()
        
        # Load data for selected or default league
        league_info_data, teams_df, weekly_data, lk = load_data(league_key)
        
        # Get weekly team stats (moves, trades, FAAB by week)
        weekly_team_stats = api_client.get_all_teams_weekly_stats(lk)
        
        # Get weekly team performance (points and record percentage vs all)
        weekly_performance_df = api_client.get_weekly_team_performance_dataframe(lk)
        
        # Convert DataFrames to JSON for JavaScript
        teams_json = teams_df.to_json(orient='records') if teams_df is not None and not teams_df.empty else '[]'
        weekly_json = weekly_df.to_json(orient='records') if weekly_df is not None and not weekly_df.empty else '[]'
        weekly_stats_json = json.dumps(weekly_team_stats) if weekly_team_stats else '[]'
        weekly_performance_json = weekly_performance_df.to_json(orient='records') if weekly_performance_df is not None and not weekly_performance_df.empty else '[]'
        
        # Prepare games and leagues data for dropdowns
        games_leagues_json = json.dumps(games_with_leagues)
        
        # Ensure league_info is a dict
        if league_info_data is None:
            league_info_data = {}
        
        return render_template('index.html', 
                             league_info=league_info_data,
                             teams_data=teams_json,
                             weekly_data=weekly_json,
                             weekly_stats_data=weekly_stats_json,
                             weekly_performance_data=weekly_performance_json,
                             games_leagues_data=games_leagues_json,
                             selected_league_key=lk)
    except Exception as e:
        logging.error(f"Error loading data: {e}", exc_info=True)
        return render_template('error.html', error_message=str(e))


@app.route('/api/games-leagues')
def api_games_leagues():
    """API endpoint for games and leagues data."""
    try:
        games_with_leagues = get_all_games_and_leagues()
        return jsonify(games_with_leagues)
    except Exception as e:
        logging.error(f"Error fetching games and leagues: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/league-data')
def api_league_data():
    """API endpoint to get all data for a specific league."""
    try:
        league_key = request.args.get('league_key')
        if not league_key:
            return jsonify({'error': 'league_key parameter required'}), 400
        
        league_info_data, teams_df, weekly_data, lk = load_data(league_key)
        
        # Get weekly team stats (moves, trades, FAAB by week)
        weekly_team_stats = api_client.get_all_teams_weekly_stats(lk)
        
        # Get weekly team performance (points and record percentage vs all)
        weekly_performance_df = api_client.get_weekly_team_performance_dataframe(lk)
        
        return jsonify({
            'league_info': league_info_data if league_info_data else {},
            'teams': teams_df.to_dict(orient='records') if teams_df is not None and not teams_df.empty else [],
            'weekly': weekly_data.to_dict(orient='records') if weekly_data is not None and not weekly_data.empty else [],
            'weekly_stats': weekly_team_stats if weekly_team_stats else [],
            'weekly_performance': weekly_performance_df.to_dict(orient='records') if weekly_performance_df is not None and not weekly_performance_df.empty else []
        })
    except Exception as e:
        logging.error(f"Error fetching league data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/teams')
def api_teams():
    """API endpoint for team stats data."""
    try:
        league_key = request.args.get('league_key', None)
        _, teams_df, _, _ = load_data(league_key)
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
        league_key = request.args.get('league_key', None)
        _, _, weekly_data, _ = load_data(league_key)
        if weekly_data is not None and not weekly_data.empty:
            return jsonify(weekly_data.to_dict(orient='records'))
        return jsonify([])
    except Exception as e:
        logging.error(f"Error fetching weekly data: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='127.0.0.1', port=5000)


"""
Flask web application for Yahoo Fantasy Stats visualization.

Author: Braedon Giblin
"""

from flask import Flask, render_template, jsonify, request
import logging
import json
import re
from collections import defaultdict
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


def load_data(league_key=None, force_refresh=False):
    """Load fantasy league data for a specific league.
    Uses cache for prior seasons automatically.
    
    Args:
        league_key: Optional league key
        force_refresh: If True, bypass cache and fetch from API
    """
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
    
    # Get league info (uses cache for prior seasons)
    league_info = api_client.get_league_info(league_key, force_refresh=force_refresh)
    
    # Get team stats DataFrame (uses cache for prior seasons)
    team_stats_df = api_client.get_teams_stats_dataframe(league_key, force_refresh=force_refresh)
    
    # Get weekly DataFrame (uses cache for prior seasons)
    weekly_df = api_client.get_weekly_dataframe(league_key, force_refresh=force_refresh)
    
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


@app.route('/api/aggregate-standings')
def api_aggregate_standings():
    """API endpoint for aggregate standings across multiple leagues."""
    try:
        league_regex = request.args.get('league_regex', '.*')
        
        if not league_regex:
            return jsonify({'error': 'league_regex parameter required'}), 400
        
        # Initialize API if needed
        if api_client is None:
            initialize_api()
        
        # Get all games and leagues
        games_with_leagues = get_all_games_and_leagues()
        
        # Filter leagues by regex pattern
        matching_leagues = []
        try:
            pattern = re.compile(league_regex, re.IGNORECASE)
        except re.error as e:
            return jsonify({'error': f'Invalid regex pattern: {e}'}), 400
        
        for game in games_with_leagues:
            for league in game.get('leagues', []):
                league_name = league.get('name', '')
                if pattern.search(league_name):
                    matching_leagues.append({
                        'league_key': league.get('league_key'),
                        'league_name': league_name,
                        'season': game.get('season'),
                        'game_name': game.get('game_name')
                    })
        
        if not matching_leagues:
            return jsonify({
                'standings': [],
                'leagues_count': 0,
                'message': 'No leagues found matching the regex pattern'
            })
        
        # Aggregate stats by manager nickname (assuming same manager across leagues)
        aggregated_stats = defaultdict(lambda: {
            'manager_nickname': '',
            'team_names': [],  # Collect all team names for this manager
            'total_wins': 0,
            'total_losses': 0,
            'total_ties': 0,
            'total_points_for': 0.0,
            'total_points_against': 0.0,
            'leagues_played': 0,
            'total_expected_wins': 0.0,
            'total_expected_losses': 0.0,
            'total_number_of_moves': 0,
            'total_number_of_trades': 0
        })
        
        # Process each matching league
        for league_info in matching_leagues:
            league_key = league_info['league_key']
            try:
                # Get team stats for this league
                team_stats_df = api_client.get_teams_stats_dataframe(league_key)
                if team_stats_df is None or team_stats_df.empty:
                    continue
                
                # Aggregate by manager nickname
                for _, row in team_stats_df.iterrows():
                    manager = row.get('manager_nickname', 'N/A')
                    if manager == 'N/A' or not manager:
                        continue
                    
                    # Initialize if first time seeing this manager
                    if aggregated_stats[manager]['manager_nickname'] == '':
                        aggregated_stats[manager]['manager_nickname'] = manager
                    
                    # Collect team name
                    team_name = row.get('team_name', 'N/A')
                    if team_name != 'N/A' and team_name and team_name not in aggregated_stats[manager]['team_names']:
                        aggregated_stats[manager]['team_names'].append(team_name)
                    
                    # Aggregate stats
                    aggregated_stats[manager]['total_wins'] += int(row.get('wins', 0)) if str(row.get('wins', 0)) != 'N/A' else 0
                    aggregated_stats[manager]['total_losses'] += int(row.get('losses', 0)) if str(row.get('losses', 0)) != 'N/A' else 0
                    aggregated_stats[manager]['total_ties'] += int(row.get('ties', 0)) if str(row.get('ties', 0)) != 'N/A' else 0
                    aggregated_stats[manager]['total_points_for'] += float(row.get('points_for', 0)) if str(row.get('points_for', 0)) != 'N/A' else 0.0
                    aggregated_stats[manager]['total_points_against'] += float(row.get('points_against', 0)) if str(row.get('points_against', 0)) != 'N/A' else 0.0
                    aggregated_stats[manager]['total_expected_wins'] += float(row.get('expected_wins', 0)) if str(row.get('expected_wins', 0)) != 'N/A' else 0.0
                    aggregated_stats[manager]['total_expected_losses'] += float(row.get('expected_losses', 0)) if str(row.get('expected_losses', 0)) != 'N/A' else 0.0
                    aggregated_stats[manager]['total_number_of_moves'] += int(row.get('number_of_moves', 0)) if str(row.get('number_of_moves', 0)) != 'N/A' else 0
                    aggregated_stats[manager]['total_number_of_trades'] += int(row.get('number_of_trades', 0)) if str(row.get('number_of_trades', 0)) != 'N/A' else 0
                    aggregated_stats[manager]['leagues_played'] += 1
                    
            except Exception as e:
                logging.warning(f"Error processing league {league_key}: {e}")
                continue
        
        # Convert to list and calculate derived stats
        standings_list = []
        for manager, stats in aggregated_stats.items():
            total_games = stats['total_wins'] + stats['total_losses'] + stats['total_ties']
            avg_win_percentage = (stats['total_wins'] / total_games * 100) if total_games > 0 else 0.0
            
            total_expected_games = stats['total_expected_wins'] + stats['total_expected_losses']
            avg_expected_win_percentage = (stats['total_expected_wins'] / total_expected_games * 100) if total_expected_games > 0 else 0.0
            
            win_pct_diff = avg_win_percentage - avg_expected_win_percentage
            
            # Get team name(s) - use first one, or join if multiple
            team_names = stats.get('team_names', [])
            team_name_display = ', '.join(team_names) if team_names else 'N/A'
            
            standings_list.append({
                'manager_nickname': stats['manager_nickname'],
                'team_name': team_name_display,
                'leagues_played': stats['leagues_played'],
                'total_wins': stats['total_wins'],
                'total_losses': stats['total_losses'],
                'total_ties': stats['total_ties'],
                'total_games': total_games,
                'avg_win_percentage': round(avg_win_percentage, 3),
                'total_points_for': round(stats['total_points_for'], 2),
                'total_points_against': round(stats['total_points_against'], 2),
                'avg_points_for': round(stats['total_points_for'] / stats['leagues_played'], 2) if stats['leagues_played'] > 0 else 0.0,
                'avg_points_against': round(stats['total_points_against'] / stats['leagues_played'], 2) if stats['leagues_played'] > 0 else 0.0,
                'total_expected_wins': round(stats['total_expected_wins'], 1),
                'total_expected_losses': round(stats['total_expected_losses'], 1),
                'avg_expected_win_percentage': round(avg_expected_win_percentage, 3),
                'win_percentage_difference': round(win_pct_diff, 3),
                'total_number_of_moves': stats['total_number_of_moves'],
                'total_number_of_trades': stats['total_number_of_trades'],
                'avg_moves_per_league': round(stats['total_number_of_moves'] / stats['leagues_played'], 1) if stats['leagues_played'] > 0 else 0.0,
                'avg_trades_per_league': round(stats['total_number_of_trades'] / stats['leagues_played'], 1) if stats['leagues_played'] > 0 else 0.0
            })
        
        return jsonify({
            'standings': standings_list,
            'leagues_count': len(matching_leagues)
        })
        
    except Exception as e:
        logging.error(f"Error fetching aggregate standings: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='127.0.0.1', port=5000)


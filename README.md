# Yahoo Fantasy Stats

## Author
Braedon Giblin

---

This repo uses yahoo api to pull historic stats and data for fantasy football league.

## Features

- OAuth 2.0 authentication with Yahoo Fantasy Sports API
- Team statistics and standings
- Weekly matchup data
- Web dashboard with interactive charts and visualizations
- Command-line interface for data fetching

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Copy `credentials.example.py` to `credentials.py` and fill in your Yahoo OAuth credentials:
   ```bash
   cp credentials.example.py credentials.py
   ```
   Then edit `credentials.py` with your Client ID and Client Secret from Yahoo Developer Network.

3. Run the command-line script:
   ```bash
   python yahoo_fantasy_stats.py
   ```

4. Run the web interface:
   ```bash
   python app.py
   ```
   Then open your browser to `http://127.0.0.1:5000`

## Web Interface

The web interface provides:
- Interactive tables showing team standings and weekly matchups
- Visual charts:
  - Win percentage by team
  - Points for vs points against
  - Average weekly scores over time
- Responsive design that works on desktop and mobile devices
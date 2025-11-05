"""
OAuth 2.0 flow implementation for Yahoo Fantasy Sports API using requests-oauthlib.
"""
import os
import json
import webbrowser
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2.rfc6749.errors import OAuth2Error


class YahooOAuth:
    """Handles OAuth 2.0 authentication flow for Yahoo Fantasy Sports API."""
    
    # Yahoo OAuth 2.0 endpoints
    AUTHORIZATION_BASE_URL = "https://api.login.yahoo.com/oauth2/request_auth"
    TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
    
    def __init__(self, client_id, client_secret, redirect_uri="oob", scope=None, token_file="oauth_tokens.json"):
        """
        Initialize Yahoo OAuth 2.0 client.
        
        Args:
            client_id: OAuth 2.0 client ID from Yahoo Developer Network
            client_secret: OAuth 2.0 client secret from Yahoo Developer Network
            redirect_uri: Redirect URI (default "oob" for out-of-band/desktop apps)
            scope: Optional list of scopes to request
            token_file: Path to file for storing OAuth tokens (default "oauth_tokens.json")
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = scope or []
        self.token_file = token_file
        self.oauth_session = None
        self.token = None
        self.access_token = None
        self.refresh_token = None
    
    def get_authorization_url(self):
        """
        Get the authorization URL for the user to visit (OAuth 2.0 authorization code flow).
        
        Returns:
            str: Authorization URL
        """
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri,
            scope=self.scope
        )
        
        authorization_url, state = oauth.authorization_url(
            self.AUTHORIZATION_BASE_URL
        )
        
        # Store state for later verification (if needed)
        self._state = state
        
        return authorization_url
    
    def open_authorization_url(self):
        """
        Open the authorization URL in the default web browser.
        
        Returns:
            str: Authorization URL
        """
        url = self.get_authorization_url()
        print(f"Opening authorization URL in browser: {url}")
        webbrowser.open(url)
        return url
    
    def get_access_token(self, authorization_code):
        """
        Exchange the authorization code for an access token (OAuth 2.0).
        
        Args:
            authorization_code: OAuth 2.0 authorization code from the callback
            
        Returns:
            dict: Token dictionary containing access_token, refresh_token, etc.
        """
        oauth = OAuth2Session(
            self.client_id,
            redirect_uri=self.redirect_uri
        )
        
        try:
            self.token = oauth.fetch_token(
                self.TOKEN_URL,
                code=authorization_code,
                client_secret=self.client_secret
            )
            
            self.access_token = self.token.get('access_token')
            self.refresh_token = self.token.get('refresh_token')
            
            return self.token
        except OAuth2Error as e:
            raise Exception(f"Failed to get access token: {e}")
    
    def create_authenticated_session(self, token=None):
        """
        Create an authenticated OAuth 2.0 session for making API requests.
        
        Args:
            token: Optional token dictionary. If not provided, uses stored token.
        
        Returns:
            OAuth2Session: Authenticated session
        """
        if token:
            self.token = token
            self.access_token = token.get('access_token')
            self.refresh_token = token.get('refresh_token')
        
        if not self.token or not self.access_token:
            raise Exception("Access token not obtained. Complete OAuth flow first.")
        
        self.oauth_session = OAuth2Session(
            self.client_id,
            token=self.token
        )
        
        return self.oauth_session
    
    def refresh_access_token(self):
        """
        Refresh the access token using the refresh token.
        
        Returns:
            dict: New token dictionary
        """
        if not self.refresh_token:
            raise Exception("No refresh token available. Complete OAuth flow first.")
        
        oauth = OAuth2Session(self.client_id, token=self.token)
        
        try:
            self.token = oauth.refresh_token(
                self.TOKEN_URL,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token
            )
            
            self.access_token = self.token.get('access_token')
            self.refresh_token = self.token.get('refresh_token', self.refresh_token)
            
            return self.token
        except OAuth2Error as e:
            raise Exception(f"Failed to refresh access token: {e}")
    
    def load_tokens(self):
        """
        Load saved OAuth 2.0 tokens from file.
        
        Returns:
            bool: True if tokens loaded successfully, False otherwise
        """
        try:
            if not os.path.exists(self.token_file):
                return False
            
            with open(self.token_file, 'r') as f:
                token = json.load(f)
            
            if token and token.get('access_token'):
                self.token = token
                self.access_token = token.get('access_token')
                self.refresh_token = token.get('refresh_token')
                return True
            return False
        except Exception as e:
            print(f"Error loading tokens: {e}")
            return False
    
    def save_tokens(self):
        """
        Save OAuth 2.0 tokens to file.
        """
        try:
            if not self.token:
                # Create token dict if it doesn't exist
                self.token = {
                    'access_token': self.access_token,
                    'refresh_token': self.refresh_token,
                    'token_type': 'Bearer'
                }
            
            with open(self.token_file, 'w') as f:
                json.dump(self.token, f, indent=2)
            print(f"✓ Tokens saved to {self.token_file}")
        except Exception as e:
            print(f"Error saving tokens: {e}")


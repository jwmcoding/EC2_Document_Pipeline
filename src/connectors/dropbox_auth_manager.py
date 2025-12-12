"""
Enhanced Dropbox Authentication Manager with OAuth2 Refresh Token Support

Handles automatic token refresh and expiration detection for long-running processes.
Supports both simple access tokens and full OAuth2 flow with refresh tokens.
"""

import dropbox
import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any, List
from dataclasses import dataclass, asdict
import logging
from pathlib import Path


@dataclass
class TokenInfo:
    """Container for Dropbox OAuth2 token information"""
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[datetime] = None
    token_type: str = "bearer"
    app_key: Optional[str] = None
    app_secret: Optional[str] = None
    
    def is_expired(self, buffer_minutes: int = 5) -> bool:
        """Check if token is expired or will expire within buffer time"""
        if not self.expires_at:
            return False  # No expiration info, assume valid
        
        # Add buffer to avoid using tokens about to expire
        buffer_time = datetime.now() + timedelta(minutes=buffer_minutes)
        return self.expires_at <= buffer_time
    
    def time_until_expiry(self) -> Optional[timedelta]:
        """Get time remaining until token expires"""
        if not self.expires_at:
            return None
        return self.expires_at - datetime.now()


class DropboxAuthManager:
    """
    Enhanced Dropbox authentication manager with automatic token refresh.
    
    Supports both simple access tokens and full OAuth2 flow with refresh tokens.
    """
    
    def __init__(self, 
                 access_token: Optional[str] = None,
                 refresh_token: Optional[str] = None,
                 app_key: Optional[str] = None,
                 app_secret: Optional[str] = None,
                 token_storage_path: Optional[str] = None):
        """
        Initialize authentication manager.
        
        Args:
            access_token: Current access token (can be short-lived)
            refresh_token: OAuth2 refresh token for automatic renewal
            app_key: Dropbox app key for OAuth2 flow
            app_secret: Dropbox app secret for OAuth2 flow
            token_storage_path: Path to store/load token info
        """
        self.logger = logging.getLogger(__name__)
        self.token_storage_path = token_storage_path or ".dropbox_tokens.json"
        
        # Initialize token info
        self.token_info = TokenInfo(
            access_token=access_token or "",
            refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )
        
        # Try to load existing token info
        self._load_token_info()
        
        # Validate configuration
        self._validate_configuration()
        
        self.logger.info("🔐 Dropbox Authentication Manager initialized")
        if self.token_info.refresh_token:
            self.logger.info("✅ OAuth2 refresh token available for automatic renewal")
        else:
            self.logger.warning("⚠️ No refresh token - manual token updates required")
    
    def _validate_configuration(self) -> None:
        """Validate authentication configuration"""
        if not self.token_info.access_token:
            raise ValueError(
                "Access token is required. Set DROPBOX_ACCESS_TOKEN environment variable "
                "or provide access_token parameter."
            )
        
        # If refresh token provided, validate app credentials
        if self.token_info.refresh_token:
            if not self.token_info.app_key or not self.token_info.app_secret:
                self.logger.warning(
                    "⚠️ Refresh token provided but missing app credentials. "
                    "Set DROPBOX_APP_KEY and DROPBOX_APP_SECRET for automatic refresh."
                )
    
    def _load_token_info(self) -> None:
        """Load token information from storage file"""
        if not os.path.exists(self.token_storage_path):
            return
        
        try:
            with open(self.token_storage_path, 'r') as f:
                data = json.load(f)
            
            # Update token info with stored data
            if data.get('access_token'):
                self.token_info.access_token = data['access_token']
            if data.get('refresh_token'):
                self.token_info.refresh_token = data['refresh_token']
            if data.get('expires_at'):
                self.token_info.expires_at = datetime.fromisoformat(data['expires_at'])
            if data.get('app_key'):
                self.token_info.app_key = data['app_key']
            
            self.logger.info(f"📁 Loaded token info from {self.token_storage_path}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load token info: {e}")
    
    def _save_token_info(self) -> None:
        """Save token information to storage file"""
        try:
            data = {
                'access_token': self.token_info.access_token,
                'refresh_token': self.token_info.refresh_token,
                'expires_at': self.token_info.expires_at.isoformat() if self.token_info.expires_at else None,
                'token_type': self.token_info.token_type,
                'app_key': self.token_info.app_key,
                'updated_at': datetime.now().isoformat()
            }
            
            with open(self.token_storage_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.debug(f"💾 Saved token info to {self.token_storage_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save token info: {e}")
    
    def refresh_access_token(self) -> bool:
        """
        Refresh the access token using refresh token.
        
        Returns:
            True if refresh successful, False otherwise
        """
        if not self.token_info.refresh_token:
            self.logger.error("❌ No refresh token available for automatic renewal")
            return False
        
        if not self.token_info.app_key or not self.token_info.app_secret:
            self.logger.error("❌ App credentials required for token refresh")
            return False
        
        try:
            self.logger.info("🔄 Attempting to refresh Dropbox access token...")
            
            # Use Dropbox OAuth2 client for refresh
            from dropbox.oauth import DropboxOAuth2FlowNoRedirect
            
            auth_flow = DropboxOAuth2FlowNoRedirect(
                self.token_info.app_key,
                self.token_info.app_secret,
                token_access_type='offline'  # Required for refresh tokens
            )
            
            # Refresh the token
            oauth_result = auth_flow.refresh(self.token_info.refresh_token)
            
            # Update token info
            self.token_info.access_token = oauth_result.access_token
            if oauth_result.refresh_token:  # New refresh token may be provided
                self.token_info.refresh_token = oauth_result.refresh_token
            
            # Calculate expiration time (typically 4 hours for Dropbox)
            self.token_info.expires_at = datetime.now() + timedelta(seconds=oauth_result.expires_in)
            
            # Save updated token info
            self._save_token_info()
            
            self.logger.info(f"✅ Token refreshed successfully! Expires at {self.token_info.expires_at}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Token refresh failed: {e}")
            return False
    
    def get_valid_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            Valid access token
            
        Raises:
            ValueError: If unable to obtain valid token
        """
        # Check if current token is expired or will expire soon
        if self.token_info.is_expired():
            self.logger.warning("⚠️ Access token is expired or expiring soon")
            
            # Try to refresh if refresh token available
            if self.token_info.refresh_token:
                if self.refresh_access_token():
                    self.logger.info("✅ Successfully refreshed expired token")
                else:
                    raise ValueError(
                        "Access token expired and refresh failed. "
                        "Manual token update required."
                    )
            else:
                raise ValueError(
                    "Access token expired and no refresh token available. "
                    "Manual token update required."
                )
        
        return self.token_info.access_token
    
    def get_authenticated_client(self) -> dropbox.Dropbox:
        """
        Get an authenticated Dropbox client with valid token.
        
        Returns:
            Authenticated Dropbox client
        """
        valid_token = self.get_valid_access_token()
        return dropbox.Dropbox(valid_token)
    
    def check_token_health(self) -> Dict[str, Any]:
        """
        Check the health and status of current tokens.
        
        Returns:
            Dictionary with token health information
        """
        health_info = {
            'has_access_token': bool(self.token_info.access_token),
            'has_refresh_token': bool(self.token_info.refresh_token),
            'can_auto_refresh': bool(self.token_info.refresh_token and 
                                   self.token_info.app_key and 
                                   self.token_info.app_secret),
            'expires_at': self.token_info.expires_at.isoformat() if self.token_info.expires_at else None,
            'is_expired': self.token_info.is_expired(),
            'time_until_expiry': None,
            'requires_manual_update': False
        }
        
        # Calculate time until expiry
        time_remaining = self.token_info.time_until_expiry()
        if time_remaining:
            health_info['time_until_expiry'] = str(time_remaining)
            health_info['requires_manual_update'] = (
                time_remaining.total_seconds() < 300 and  # Less than 5 minutes
                not health_info['can_auto_refresh']
            )
        
        return health_info
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test Dropbox API connection with current token.
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            client = self.get_authenticated_client()
            account = client.users_get_current_account()
            
            self.logger.info(f"✅ Connection test successful for {account.email}")
            return True, None
            
        except dropbox.exceptions.AuthError as e:
            error_msg = f"Authentication failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
            
        except Exception as e:
            error_msg = f"Connection test failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
    
    def setup_oauth_flow(self) -> str:
        """
        Set up OAuth2 flow for obtaining refresh token.
        
        Returns:
            Authorization URL for user to visit
        """
        if not self.token_info.app_key or not self.token_info.app_secret:
            raise ValueError(
                "App key and secret required for OAuth flow. "
                "Set DROPBOX_APP_KEY and DROPBOX_APP_SECRET environment variables."
            )
        
        from dropbox.oauth import DropboxOAuth2FlowNoRedirect
        
        auth_flow = DropboxOAuth2FlowNoRedirect(
            self.token_info.app_key,
            self.token_info.app_secret,
            token_access_type='offline'  # Required for refresh tokens
        )
        
        authorize_url = auth_flow.start()
        
        self.logger.info("🔗 OAuth2 flow started")
        self.logger.info(f"Please visit: {authorize_url}")
        self.logger.info("After authorization, call complete_oauth_flow() with the authorization code")
        
        return authorize_url
    
    def complete_oauth_flow(self, authorization_code: str) -> bool:
        """
        Complete OAuth2 flow with authorization code.
        
        Args:
            authorization_code: Code received after user authorization
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from dropbox.oauth import DropboxOAuth2FlowNoRedirect
            
            auth_flow = DropboxOAuth2FlowNoRedirect(
                self.token_info.app_key,
                self.token_info.app_secret,
                token_access_type='offline'
            )
            
            oauth_result = auth_flow.finish(authorization_code)
            
            # Update token info
            self.token_info.access_token = oauth_result.access_token
            self.token_info.refresh_token = oauth_result.refresh_token
            self.token_info.expires_at = datetime.now() + timedelta(seconds=oauth_result.expires_in)
            
            # Save tokens
            self._save_token_info()
            
            self.logger.info("✅ OAuth2 flow completed successfully!")
            self.logger.info(f"🔑 Refresh token obtained - automatic renewal enabled")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ OAuth2 flow completion failed: {e}")
            return False
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics and token information"""
        health = self.check_token_health()
        
        return {
            'authentication_method': 'OAuth2' if self.token_info.refresh_token else 'Simple Token',
            'auto_refresh_enabled': health['can_auto_refresh'],
            'token_health': health,
            'recommendations': self._get_recommendations(health)
        }
    
    def _get_recommendations(self, health: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on token health"""
        recommendations = []
        
        if not health['has_refresh_token']:
            recommendations.append(
                "Set up OAuth2 flow with refresh token for automatic token renewal"
            )
        
        if health['requires_manual_update']:
            recommendations.append(
                "Token expires soon and cannot auto-refresh - manual update required"
            )
        
        if not health['can_auto_refresh'] and health['has_refresh_token']:
            recommendations.append(
                "Refresh token available but missing app credentials for auto-refresh"
            )
        
        if health['is_expired']:
            recommendations.append(
                "Access token is expired - immediate refresh or manual update required"
            )
        
        return recommendations


# Utility function for backwards compatibility
def create_dropbox_client_with_auth(access_token: str = None, 
                                   refresh_token: str = None,
                                   app_key: str = None,
                                   app_secret: str = None) -> dropbox.Dropbox:
    """
    Create authenticated Dropbox client with automatic token refresh capability.
    
    Args:
        access_token: Current access token
        refresh_token: OAuth2 refresh token for automatic renewal
        app_key: Dropbox app key
        app_secret: Dropbox app secret
        
    Returns:
        Authenticated Dropbox client
    """
    auth_manager = DropboxAuthManager(
        access_token=access_token,
        refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret
    )
    
    return auth_manager.get_authenticated_client() 
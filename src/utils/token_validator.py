"""
Token Validation Utility for Dropbox API

Provides token validation checks and user interaction for token refresh during long-running processes.
"""

import os
import time
import dropbox
from typing import Optional, Tuple, Callable
from dotenv import load_dotenv
import logging


class TokenValidator:
    """
    Handles token validation and user prompts for token refresh.
    
    Designed to pause processing when tokens expire and allow users to
    manually refresh tokens without losing progress.
    """
    
    def __init__(self, dropbox_client: dropbox.Dropbox, logger: Optional[logging.Logger] = None):
        """
        Initialize token validator.
        
        Args:
            dropbox_client: Dropbox client instance to validate
            logger: Logger for output (optional)
        """
        self.client = dropbox_client
        self.logger = logger or logging.getLogger(__name__)
        self.last_validation_time = 0
        self.validation_interval = 300  # Check every 5 minutes by default
        
    def is_token_valid(self) -> Tuple[bool, Optional[str]]:
        """
        Test if the current token is valid by making a simple API call.
        
        Returns:
            Tuple of (is_valid: bool, error_message: Optional[str])
        """
        try:
            # Use a lightweight API call to test token validity
            account = self.client.users_get_current_account()
            return True, None
        except dropbox.exceptions.AuthError as e:
            return False, f"Authentication failed: {str(e)}"
        except Exception as e:
            # Other errors might be network issues, not token issues
            return True, f"Non-auth error (token likely valid): {str(e)}"
    
    def should_validate_now(self, force: bool = False) -> bool:
        """
        Check if we should validate the token now based on timing.
        
        Args:
            force: Force validation regardless of timing
            
        Returns:
            True if validation should be performed
        """
        if force:
            return True
        
        current_time = time.time()
        return (current_time - self.last_validation_time) >= self.validation_interval
    
    def validate_with_retry(self, max_retries: int = 3, context: str = "processing") -> bool:
        """
        Validate token with user interaction for refresh if needed.
        
        Args:
            max_retries: Maximum number of retry attempts
            context: Description of what we're doing (for user messages)
            
        Returns:
            True if token is valid or successfully refreshed, False to abort
        """
        self.last_validation_time = time.time()
        
        for attempt in range(max_retries):
            is_valid, error_msg = self.is_token_valid()
            
            if is_valid:
                if attempt > 0:
                    self.logger.info("✅ Token validation successful after refresh!")
                return True
            
            # Token is invalid - prompt user for action
            self.logger.error(f"❌ Token validation failed: {error_msg}")
            
            if attempt < max_retries - 1:  # Don't prompt on last attempt
                action = self._prompt_user_for_token_refresh(context, attempt + 1, max_retries)
                
                if action == "refresh":
                    self._wait_for_token_refresh()
                elif action == "abort":
                    self.logger.warning("🛑 User chose to abort processing")
                    return False
                # If action == "retry", we'll loop again
            else:
                self.logger.error(f"❌ Token validation failed after {max_retries} attempts")
                return False
        
        return False
    
    def _prompt_user_for_token_refresh(self, context: str, attempt: int, max_attempts: int) -> str:
        """
        Prompt user for action when token is invalid.
        
        Returns:
            "refresh", "retry", or "abort"
        """
        print("\n" + "="*60)
        print("🚨 DROPBOX TOKEN EXPIRED")
        print("="*60)
        print(f"📍 Context: {context}")
        print(f"🔄 Attempt: {attempt}/{max_attempts}")
        print(f"⏰ Time: {time.strftime('%H:%M:%S')}")
        
        print("\n📋 Your Options:")
        print("1. Refresh token manually and continue")
        print("2. Retry validation (if you just refreshed)")
        print("3. Abort processing")
        
        print("\n🔧 To refresh your token:")
        print("1. Go to your Dropbox app settings")
        print("2. Generate a new access token")
        print("3. Update your .env file: DROPBOX_ACCESS_TOKEN=new_token")
        print("4. Choose option 1 below to continue")
        
        while True:
            try:
                choice = input("\nSelect option (1-3): ").strip()
                
                if choice == "1":
                    return "refresh"
                elif choice == "2":
                    return "retry"
                elif choice == "3":
                    return "abort"
                else:
                    print("❌ Invalid choice. Please enter 1, 2, or 3.")
            except KeyboardInterrupt:
                print("\n🛑 Interrupted. Aborting...")
                return "abort"
    
    def _wait_for_token_refresh(self):
        """Wait for user to refresh token with progress indication."""
        print("\n⏳ Waiting for token refresh...")
        print("Press Enter when you've updated the token in your .env file")
        print("(This will reload the environment variables)")
        
        try:
            input()
            
            # Reload environment variables
            load_dotenv(override=True)
            new_token = os.getenv('DROPBOX_ACCESS_TOKEN')
            
            if new_token:
                # Update the client with new token
                self.client._oauth2_access_token = new_token
                print("✅ Environment reloaded. Testing new token...")
            else:
                print("⚠️ No token found in environment. Make sure .env file is updated.")
                
        except KeyboardInterrupt:
            print("\n🛑 Token refresh interrupted")
    
    def create_validation_checkpoint(self, checkpoint_name: str) -> Callable[[], bool]:
        """
        Create a validation checkpoint function for use in processing loops.
        
        Args:
            checkpoint_name: Descriptive name for this validation point
            
        Returns:
            Function that returns True to continue, False to abort
        """
        def checkpoint() -> bool:
            if self.should_validate_now():
                self.logger.info(f"🔍 Token validation checkpoint: {checkpoint_name}")
                return self.validate_with_retry(context=checkpoint_name)
            return True
        
        return checkpoint


class TokenValidatedDropboxClient:
    """
    Wrapper around DropboxClient that includes automatic token validation.
    
    This provides a drop-in replacement for existing DropboxClient usage
    with built-in token validation at key operations.
    """
    
    def __init__(self, dropbox_client, validation_interval: int = 300):
        """
        Initialize validated client wrapper.
        
        Args:
            dropbox_client: The DropboxClient instance to wrap
            validation_interval: Seconds between automatic validations
        """
        self.client = dropbox_client
        self.validator = TokenValidator(dropbox_client.client, dropbox_client.logger)
        self.validator.validation_interval = validation_interval
        self.logger = dropbox_client.logger
        
    def list_documents_with_validation(self, folder_path: str, validation_interval_docs: int = 50):
        """
        List documents with periodic token validation.
        
        Args:
            folder_path: Dropbox folder path
            validation_interval_docs: Validate token every N documents
            
        Yields:
            DocumentMetadata objects
        """
        document_count = 0
        validation_checkpoint = self.validator.create_validation_checkpoint(
            f"document discovery in {folder_path}"
        )
        
        # Initial validation
        if not validation_checkpoint():
            self.logger.error("❌ Initial token validation failed. Aborting discovery.")
            return
        
        try:
            for document in self.client.list_documents(folder_path):
                yield document
                document_count += 1
                
                # Periodic validation during discovery
                if document_count % validation_interval_docs == 0:
                    self.logger.info(f"📊 Discovered {document_count} documents, validating token...")
                    if not validation_checkpoint():
                        self.logger.error("❌ Token validation failed during discovery. Stopping.")
                        break
                        
        except Exception as e:
            # Check if this might be a token issue
            if "auth" in str(e).lower() or "unauthorized" in str(e).lower():
                self.logger.error(f"❌ Authentication error during discovery: {e}")
                if not validation_checkpoint():
                    return
            else:
                raise
    
    def validate_before_batch(self, batch_id: int, batch_size: int) -> bool:
        """
        Validate token before processing a batch.
        
        Args:
            batch_id: Batch identifier
            batch_size: Number of documents in batch
            
        Returns:
            True to continue, False to abort
        """
        validation_checkpoint = self.validator.create_validation_checkpoint(
            f"batch {batch_id} processing ({batch_size} documents)"
        )
        
        return validation_checkpoint()
    
    def __getattr__(self, name):
        """Delegate all other attributes to the wrapped client."""
        return getattr(self.client, name) 
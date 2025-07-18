"""Configuration module for the Delta Scoreboard application."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Configuration class for the application."""
    
    # Databricks Configuration (optional - can be set at runtime)
    DATABRICKS_SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME', '')
    DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH', '')
    DATABRICKS_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN', '')
    
    # Application Configuration
    APP_SECRET_KEY = os.getenv('APP_SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    PORT = int(os.getenv('PORT', '8050'))
    HOST = os.getenv('HOST', '127.0.0.1')
    
    # Database Configuration
    DATABASE_NAME = os.getenv('DATABASE_NAME', 'workshop_leaderboard')
    USERS_TABLE = os.getenv('USERS_TABLE', 'eligible_users')
    RESPONSES_TABLE = os.getenv('RESPONSES_TABLE', 'user_responses')
    LEADERBOARD_TABLE = os.getenv('LEADERBOARD_TABLE', 'leaderboard')
    
    # Workshop Configuration
    MAX_QUESTIONS = int(os.getenv('MAX_QUESTIONS', '10'))
    POINTS_PER_QUESTION = int(os.getenv('POINTS_PER_QUESTION', '10'))
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate that required configuration values are set."""
        # For runtime credentials, this is no longer required
        return True
    
    @classmethod
    def validate_runtime_credentials(cls, hostname: str, http_path: str, access_token: str, workspace_id: str = None) -> bool:
        """Validate runtime credentials."""
        if not hostname or not http_path or not access_token:
            return False
        if hostname.startswith('your-') or http_path.startswith('your-') or access_token.startswith('your-'):
            return False
        # Workspace ID is optional but if provided, should be numeric
        if workspace_id and not workspace_id.isdigit():
            return False
        return True 
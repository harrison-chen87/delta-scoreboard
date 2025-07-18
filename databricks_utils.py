"""Databricks utilities for connecting to SQL warehouse and managing data."""

import pandas as pd
import requests
from databricks import sql
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Optional, Any
import logging
from config import Config

logger = logging.getLogger(__name__)

class DatabricksConnection:
    """Handles connection to Databricks SQL warehouse."""
    
    def __init__(self, hostname: str = None, workspace_id: str = None, http_path: str = None, access_token: str = None):
        self.connection = None
        self.workspace_client = None
        
        # Use provided credentials or fall back to config
        self.hostname = hostname or Config.DATABRICKS_SERVER_HOSTNAME
        self.workspace_id = workspace_id
        self.http_path = http_path or Config.DATABRICKS_HTTP_PATH
        self.access_token = access_token or Config.DATABRICKS_ACCESS_TOKEN
        
    def connect(self) -> bool:
        """Establish connection to Databricks SQL warehouse."""
        try:
            if not self.hostname or not self.http_path or not self.access_token:
                logger.error("Missing required Databricks credentials")
                return False
                
            self.connection = sql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            # Also create workspace client for user management
            self.workspace_client = WorkspaceClient(
                host=f"https://{self.hostname}",
                token=self.access_token
            )
            
            logger.info("Successfully connected to Databricks")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        if not self.connection:
            raise Exception("Not connected to Databricks")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or {})
            
            # Fetch results and column names
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            return pd.DataFrame(results, columns=columns)
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
    
    def get_scim_headers(self) -> Dict[str, str]:
        """Get headers for SCIM API requests."""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/scim+json'
        }
    
    def get_scim_base_url(self) -> str:
        """Get base URL for SCIM API requests."""
        return f"https://{self.hostname}/api/2.0/preview/scim/v2"
    
    def create_sql_warehouse(self, name: str, cluster_size: str, auto_stop_mins: int = 45, 
                           max_num_clusters: int = 1, warehouse_type: str = "PRO") -> Dict[str, Any]:
        """Create a new SQL warehouse."""
        try:
            url = f"https://{self.hostname}/api/2.0/sql/warehouses"
            
            payload = {
                "name": name,
                "cluster_size": cluster_size,
                "auto_stop_mins": auto_stop_mins,
                "max_num_clusters": max_num_clusters,
                "warehouse_type": warehouse_type,
                "enable_photon": True,
                "channel": {
                    "name": "CHANNEL_NAME_CURRENT"
                }
            }
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            warehouse_data = response.json()
            logger.info(f"Successfully created SQL warehouse: {name}")
            
            return {
                "id": warehouse_data.get("id"),
                "name": name,
                "cluster_size": cluster_size,
                "http_path": f"/sql/1.0/warehouses/{warehouse_data.get('id')}",
                "state": warehouse_data.get("state"),
                "auto_stop_mins": auto_stop_mins,
                "max_num_clusters": max_num_clusters,
                "warehouse_type": warehouse_type
            }
            
        except Exception as e:
            logger.error(f"Error creating SQL warehouse: {str(e)}")
            raise

class UserManager:
    """Manages user data and authentication."""
    
    def __init__(self, db_connection: DatabricksConnection):
        self.db = db_connection
    
    def get_workspace_users(self) -> List[Dict[str, str]]:
        """Get all users eligible to work in the workspace using SCIM API."""
        try:
            if not self.db.access_token or not self.db.hostname:
                raise Exception("Missing credentials for SCIM API")
            
            # Use SCIM API to get users
            scim_url = f"{self.db.get_scim_base_url()}/Users"
            headers = self.db.get_scim_headers()
            
            response = requests.get(scim_url, headers=headers, params={'count': 100})
            response.raise_for_status()
            
            data = response.json()
            users = []
            
            for user in data.get('Resources', []):
                if user.get('active', False):
                    # Extract email from emails array
                    email = None
                    emails = user.get('emails', [])
                    if emails:
                        email = emails[0].get('value')
                    
                    users.append({
                        'id': user.get('id'),
                        'email': email,
                        'display_name': user.get('displayName'),
                        'user_name': user.get('userName')
                    })
            
            logger.info(f"Retrieved {len(users)} users from SCIM API")
            return users
            
        except Exception as e:
            logger.error(f"Error fetching workspace users via SCIM API: {str(e)}")
            # Return mock data for testing
            return [
                {'id': '1', 'email': 'user1@example.com', 'display_name': 'User One', 'user_name': 'user1'},
                {'id': '2', 'email': 'user2@example.com', 'display_name': 'User Two', 'user_name': 'user2'},
                {'id': '3', 'email': 'user3@example.com', 'display_name': 'User Three', 'user_name': 'user3'}
            ]
    
    def create_users_table(self) -> bool:
        """Create the eligible users table if it doesn't exist."""
        try:
            query = f"""
            CREATE TABLE IF NOT EXISTS {Config.USERS_TABLE} (
                user_id STRING,
                email STRING,
                display_name STRING,
                user_name STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (user_id)
            )
            """
            
            self.db.execute_query(query)
            logger.info("Users table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating users table: {str(e)}")
            return False
    
    def sync_users_to_table(self) -> bool:
        """Sync workspace users to the database table."""
        try:
            users = self.get_workspace_users()
            
            for user in users:
                query = f"""
                MERGE INTO {Config.USERS_TABLE} AS target
                USING (SELECT '{user['id']}' as user_id, '{user['email']}' as email, 
                       '{user['display_name']}' as display_name, '{user['user_name']}' as user_name) AS source
                ON target.user_id = source.user_id
                WHEN NOT MATCHED THEN
                    INSERT (user_id, email, display_name, user_name)
                    VALUES (source.user_id, source.email, source.display_name, source.user_name)
                WHEN MATCHED THEN
                    UPDATE SET email = source.email, display_name = source.display_name, user_name = source.user_name
                """
                
                self.db.execute_query(query)
            
            logger.info(f"Synced {len(users)} users to database")
            return True
            
        except Exception as e:
            logger.error(f"Error syncing users to table: {str(e)}")
            return False
    
    def is_user_eligible(self, email: str) -> bool:
        """Check if a user is eligible to participate."""
        try:
            query = f"SELECT COUNT(*) as count FROM {Config.USERS_TABLE} WHERE email = '{email}'"
            result = self.db.execute_query(query)
            return result.iloc[0]['count'] > 0
            
        except Exception as e:
            logger.error(f"Error checking user eligibility: {str(e)}")
            return False

class ResponseManager:
    """Manages user responses and scoring."""
    
    def __init__(self, db_connection: DatabricksConnection):
        self.db = db_connection
    
    def create_responses_table(self) -> bool:
        """Create the user responses table if it doesn't exist."""
        try:
            query = f"""
            CREATE TABLE IF NOT EXISTS {Config.RESPONSES_TABLE} (
                response_id STRING,
                user_id STRING,
                question_id INT,
                user_answer STRING,
                correct_answer STRING,
                points_earned INT,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (response_id)
            )
            """
            
            self.db.execute_query(query)
            logger.info("Responses table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating responses table: {str(e)}")
            return False
    
    def submit_response(self, user_id: str, question_id: int, user_answer: str, correct_answer: str) -> bool:
        """Submit a user's response to a question."""
        try:
            import uuid
            response_id = str(uuid.uuid4())
            points = Config.POINTS_PER_QUESTION if user_answer.strip().lower() == correct_answer.strip().lower() else 0
            
            query = f"""
            INSERT INTO {Config.RESPONSES_TABLE} 
            (response_id, user_id, question_id, user_answer, correct_answer, points_earned)
            VALUES ('{response_id}', '{user_id}', {question_id}, '{user_answer}', '{correct_answer}', {points})
            """
            
            self.db.execute_query(query)
            logger.info(f"Response submitted for user {user_id}, question {question_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error submitting response: {str(e)}")
            return False
    
    def get_user_score(self, user_id: str) -> int:
        """Get the total score for a user."""
        try:
            query = f"SELECT SUM(points_earned) as total_points FROM {Config.RESPONSES_TABLE} WHERE user_id = '{user_id}'"
            result = self.db.execute_query(query)
            return result.iloc[0]['total_points'] or 0
            
        except Exception as e:
            logger.error(f"Error getting user score: {str(e)}")
            return 0
    
    def get_leaderboard(self) -> pd.DataFrame:
        """Get the current leaderboard."""
        try:
            query = f"""
            SELECT 
                u.display_name,
                u.email,
                COALESCE(SUM(r.points_earned), 0) as total_points,
                COUNT(r.response_id) as questions_answered
            FROM {Config.USERS_TABLE} u
            LEFT JOIN {Config.RESPONSES_TABLE} r ON u.user_id = r.user_id
            GROUP BY u.user_id, u.display_name, u.email
            ORDER BY total_points DESC, questions_answered DESC
            """
            
            return self.db.execute_query(query)
            
        except Exception as e:
            logger.error(f"Error getting leaderboard: {str(e)}")
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=['display_name', 'email', 'total_points', 'questions_answered']) 
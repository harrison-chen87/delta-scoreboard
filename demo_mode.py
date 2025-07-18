"""Demo mode for testing the app without Databricks connection."""

import pandas as pd
import uuid
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class MockDatabricksConnection:
    """Mock Databricks connection for demo purposes."""
    
    def __init__(self):
        self.connected = False
        self.mock_data = {
            'users': [
                {'user_id': '1', 'email': 'demo@example.com', 'display_name': 'Demo User', 'user_name': 'demo'},
                {'user_id': '2', 'email': 'test@example.com', 'display_name': 'Test User', 'user_name': 'test'},
                {'user_id': '3', 'email': 'admin@example.com', 'display_name': 'Admin User', 'user_name': 'admin'}
            ],
            'responses': []
        }
    
    def connect(self) -> bool:
        """Mock connection."""
        self.connected = True
        logger.info("Mock Databricks connection established")
        return True
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Mock query execution."""
        if not self.connected:
            raise Exception("Not connected to mock database")
        
        # Simple query parsing for demo
        if "eligible_users" in query.lower():
            return pd.DataFrame(self.mock_data['users'])
        elif "user_responses" in query.lower():
            return pd.DataFrame(self.mock_data['responses'])
        else:
            return pd.DataFrame()
    
    def close(self):
        """Mock close connection."""
        self.connected = False

class MockUserManager:
    """Mock user manager for demo purposes."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.users = db_connection.mock_data['users']
    
    def get_workspace_users(self) -> List[Dict[str, str]]:
        """Return mock users."""
        return self.users
    
    def create_users_table(self) -> bool:
        """Mock table creation."""
        logger.info("Mock users table created")
        return True
    
    def sync_users_to_table(self) -> bool:
        """Mock user sync."""
        logger.info("Mock user sync completed")
        return True
    
    def is_user_eligible(self, email: str) -> bool:
        """Check if user is eligible (mock)."""
        return any(user['email'] == email for user in self.users)

class MockResponseManager:
    """Mock response manager for demo purposes."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.responses = db_connection.mock_data['responses']
    
    def create_responses_table(self) -> bool:
        """Mock table creation."""
        logger.info("Mock responses table created")
        return True
    
    def submit_response(self, user_id: str, question_id: int, user_answer: str, correct_answer: str) -> bool:
        """Submit a mock response."""
        try:
            response = {
                'response_id': str(uuid.uuid4()),
                'user_id': user_id,
                'question_id': question_id,
                'user_answer': user_answer,
                'correct_answer': correct_answer,
                'points_earned': 10 if user_answer.strip().lower() == correct_answer.strip().lower() else 0,
                'submitted_at': pd.Timestamp.now()
            }
            self.responses.append(response)
            logger.info(f"Mock response submitted for user {user_id}, question {question_id}")
            return True
        except Exception as e:
            logger.error(f"Error submitting mock response: {str(e)}")
            return False
    
    def get_user_score(self, user_id: str) -> int:
        """Get user's total score."""
        return sum(r['points_earned'] for r in self.responses if r['user_id'] == user_id)
    
    def get_leaderboard(self) -> pd.DataFrame:
        """Get mock leaderboard."""
        try:
            # Calculate scores by user
            user_scores = {}
            for response in self.responses:
                user_id = response['user_id']
                if user_id not in user_scores:
                    user_scores[user_id] = {'total_points': 0, 'questions_answered': 0}
                user_scores[user_id]['total_points'] += response['points_earned']
                user_scores[user_id]['questions_answered'] += 1
            
            # Build leaderboard
            leaderboard_data = []
            for user in self.db.mock_data['users']:
                user_id = user['user_id']
                if user_id in user_scores:
                    leaderboard_data.append({
                        'display_name': user['display_name'],
                        'email': user['email'],
                        'total_points': user_scores[user_id]['total_points'],
                        'questions_answered': user_scores[user_id]['questions_answered']
                    })
                else:
                    leaderboard_data.append({
                        'display_name': user['display_name'],
                        'email': user['email'],
                        'total_points': 0,
                        'questions_answered': 0
                    })
            
            # Sort by points descending
            leaderboard_data.sort(key=lambda x: (x['total_points'], x['questions_answered']), reverse=True)
            
            return pd.DataFrame(leaderboard_data)
            
        except Exception as e:
            logger.error(f"Error generating mock leaderboard: {str(e)}")
            return pd.DataFrame(columns=['display_name', 'email', 'total_points', 'questions_answered'])

def create_demo_app():
    """Create app instances for demo mode."""
    connection = MockDatabricksConnection()
    connection.connect()
    
    user_manager = MockUserManager(connection)
    response_manager = MockResponseManager(connection)
    
    return connection, user_manager, response_manager 
"""Demo version of Delta Scoreboard Dash application for testing without Databricks."""

import dash
from dash import dcc, html, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import logging
from typing import Dict, List, Optional
import os

# Import demo mode components
from demo_mode import create_demo_app

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Delta Scoreboard - Workshop Leaderboard (Demo)"

# Global variables
db_connection = None
user_manager = None
response_manager = None

# Sample workshop questions (same as main app)
WORKSHOP_QUESTIONS = [
    {
        "id": 1,
        "question": "What is the primary benefit of Delta Lake?",
        "options": ["ACID transactions", "Schema evolution", "Time travel", "All of the above"],
        "correct_answer": "All of the above"
    },
    {
        "id": 2,
        "question": "Which format does Delta Lake use for storage?",
        "options": ["JSON", "Parquet", "CSV", "Avro"],
        "correct_answer": "Parquet"
    },
    {
        "id": 3,
        "question": "What command is used to optimize Delta tables?",
        "options": ["VACUUM", "OPTIMIZE", "COMPACT", "MERGE"],
        "correct_answer": "OPTIMIZE"
    },
    {
        "id": 4,
        "question": "Which SQL command allows you to see table history in Delta Lake?",
        "options": ["SHOW HISTORY", "DESCRIBE HISTORY", "SELECT HISTORY", "TABLE HISTORY"],
        "correct_answer": "DESCRIBE HISTORY"
    },
    {
        "id": 5,
        "question": "What is the default retention period for Delta Lake time travel?",
        "options": ["7 days", "30 days", "90 days", "365 days"],
        "correct_answer": "30 days"
    }
]

def initialize_demo_app():
    """Initialize the demo app with mock data."""
    global db_connection, user_manager, response_manager
    
    try:
        db_connection, user_manager, response_manager = create_demo_app()
        logger.info("Demo app initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing demo app: {str(e)}")
        return False

def create_header():
    """Create the application header."""
    return dbc.NavbarSimple(
        brand="🏆 Delta Scoreboard (Demo Mode)",
        brand_href="#",
        color="warning",
        dark=True,
        className="mb-4"
    )

def create_demo_info():
    """Create demo mode information banner."""
    return dbc.Alert([
        html.H5("Demo Mode", className="alert-heading"),
        html.P("This is a demo version running without Databricks connection."),
        html.P("Try logging in with: demo@example.com, test@example.com, or admin@example.com")
    ], color="info", className="mb-4")

def create_login_form():
    """Create the login form for user authentication."""
    return dbc.Container([
        create_demo_info(),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Welcome to Delta Scoreboard", className="card-title text-center mb-4"),
                        html.P("Enter your email to join the workshop leaderboard:", className="text-center mb-4"),
                        dbc.InputGroup([
                            dbc.Input(
                                id="email-input",
                                type="email",
                                placeholder="Enter your email address",
                                className="form-control"
                            ),
                            dbc.Button("Login", id="login-btn", color="primary", className="btn")
                        ], className="mb-3"),
                        html.Div(id="login-message", className="text-center")
                    ])
                ], className="shadow")
            ], width=6)
        ], justify="center")
    ])

def create_question_form(question_num: int):
    """Create a form for a specific question."""
    if question_num > len(WORKSHOP_QUESTIONS):
        return html.Div("No more questions available!")
    
    question = WORKSHOP_QUESTIONS[question_num - 1]
    
    return dbc.Card([
        dbc.CardBody([
            html.H5(f"Question {question_num}", className="card-title"),
            html.P(question["question"], className="card-text mb-3"),
            dbc.RadioItems(
                id=f"question-{question_num}-options",
                options=[{"label": option, "value": option} for option in question["options"]],
                className="mb-3"
            ),
            dbc.Button(
                "Submit Answer",
                id=f"submit-question-{question_num}",
                color="success",
                className="me-2"
            ),
            html.Div(id=f"question-{question_num}-feedback", className="mt-3")
        ])
    ], className="mb-3")

def create_questions_page():
    """Create the questions page."""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H3("Workshop Questions", className="mb-4"),
                html.Div(id="questions-container"),
                html.Hr(),
                dbc.Button("View Leaderboard", id="view-leaderboard-btn", color="primary", className="mb-3"),
                html.Div(id="user-score-display", className="mb-3")
            ])
        ])
    ])

def create_leaderboard():
    """Create the leaderboard display."""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H3("🏆 Workshop Leaderboard", className="mb-4"),
                html.Div(id="leaderboard-container"),
                html.Hr(),
                dbc.Button("Back to Questions", id="back-to-questions-btn", color="primary", className="mb-3"),
                dbc.Button("Refresh Leaderboard", id="refresh-leaderboard-btn", color="secondary", className="mb-3 ms-2")
            ])
        ])
    ])

# App layout
app.layout = dbc.Container([
    dcc.Store(id="user-data", data=None),
    dcc.Store(id="app-state", data={"page": "login", "question_num": 1}),
    dcc.Interval(id="auto-refresh", interval=10000, n_intervals=0),  # Auto-refresh every 10 seconds for demo
    
    create_header(),
    
    html.Div(id="page-content")
], fluid=True)

# Callbacks (same as main app)
@app.callback(
    [Output("page-content", "children"),
     Output("user-data", "data"),
     Output("app-state", "data")],
    [Input("login-btn", "n_clicks"),
     Input("view-leaderboard-btn", "n_clicks"),
     Input("back-to-questions-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")],
    [State("email-input", "value"),
     State("user-data", "data"),
     State("app-state", "data")]
)
def handle_navigation(login_clicks, leaderboard_clicks, back_clicks, auto_refresh, email, user_data, app_state):
    """Handle navigation between pages."""
    ctx = dash.callback_context
    
    if not ctx.triggered:
        return create_login_form(), None, {"page": "login", "question_num": 1}
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Handle login
    if trigger_id == "login-btn" and login_clicks and email:
        if user_manager and user_manager.is_user_eligible(email):
            user_data = {"email": email, "user_id": email}
            app_state = {"page": "questions", "question_num": 1}
            return create_questions_page(), user_data, app_state
        else:
            return create_login_form(), None, {"page": "login", "question_num": 1}
    
    # Handle leaderboard view
    elif trigger_id == "view-leaderboard-btn" and leaderboard_clicks:
        app_state["page"] = "leaderboard"
        return create_leaderboard(), user_data, app_state
    
    # Handle back to questions
    elif trigger_id == "back-to-questions-btn" and back_clicks:
        app_state["page"] = "questions"
        return create_questions_page(), user_data, app_state
    
    # Handle auto-refresh
    elif trigger_id == "auto-refresh" and app_state and app_state.get("page") == "leaderboard":
        return create_leaderboard(), user_data, app_state
    
    # Default state
    if not user_data:
        return create_login_form(), None, {"page": "login", "question_num": 1}
    elif app_state.get("page") == "questions":
        return create_questions_page(), user_data, app_state
    elif app_state.get("page") == "leaderboard":
        return create_leaderboard(), user_data, app_state
    else:
        return create_login_form(), None, {"page": "login", "question_num": 1}

@app.callback(
    Output("questions-container", "children"),
    Input("page-content", "children"),
    State("app-state", "data")
)
def load_questions(page_content, app_state):
    """Load questions when on the questions page."""
    if app_state and app_state.get("page") == "questions":
        return [create_question_form(i) for i in range(1, len(WORKSHOP_QUESTIONS) + 1)]
    return []

@app.callback(
    Output("leaderboard-container", "children"),
    [Input("page-content", "children"),
     Input("refresh-leaderboard-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")],
    State("app-state", "data")
)
def load_leaderboard(page_content, refresh_clicks, auto_refresh, app_state):
    """Load and display the leaderboard."""
    if app_state and app_state.get("page") == "leaderboard":
        try:
            if response_manager:
                leaderboard_df = response_manager.get_leaderboard()
                
                if not leaderboard_df.empty:
                    # Create a more visual leaderboard
                    leaderboard_components = []
                    
                    for i, row in leaderboard_df.iterrows():
                        rank = i + 1
                        medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                        
                        card = dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        html.H5(f"{medal} {row['display_name']}", className="mb-1"),
                                        html.P(row['email'], className="text-muted mb-0")
                                    ], width=8),
                                    dbc.Col([
                                        html.H4(f"{row['total_points']}", className="text-primary mb-0"),
                                        html.P(f"{row['questions_answered']} questions", className="text-muted mb-0")
                                    ], width=4, className="text-end")
                                ])
                            ])
                        ], className="mb-2")
                        
                        leaderboard_components.append(card)
                    
                    return leaderboard_components
                else:
                    return [html.P("No scores yet. Be the first to answer questions!", className="text-center")]
            else:
                return [html.P("Unable to load leaderboard. Please check your connection.", className="text-center")]
        except Exception as e:
            logger.error(f"Error loading leaderboard: {str(e)}")
            return [html.P("Error loading leaderboard.", className="text-center")]
    
    return []

# Dynamic callbacks for question submissions
for i in range(1, len(WORKSHOP_QUESTIONS) + 1):
    @app.callback(
        Output(f"question-{i}-feedback", "children"),
        Input(f"submit-question-{i}", "n_clicks"),
        [State(f"question-{i}-options", "value"),
         State("user-data", "data")]
    )
    def submit_answer(n_clicks, selected_answer, user_data, question_num=i):
        """Submit an answer for a question."""
        if n_clicks and selected_answer and user_data:
            try:
                question = WORKSHOP_QUESTIONS[question_num - 1]
                correct_answer = question["correct_answer"]
                
                if response_manager:
                    success = response_manager.submit_response(
                        user_data["user_id"], 
                        question_num, 
                        selected_answer, 
                        correct_answer
                    )
                    
                    if success:
                        if selected_answer == correct_answer:
                            return dbc.Alert("Correct! ✅", color="success")
                        else:
                            return dbc.Alert(f"Incorrect. The correct answer is: {correct_answer}", color="warning")
                    else:
                        return dbc.Alert("Error submitting answer. Please try again.", color="danger")
                else:
                    return dbc.Alert("Database connection error.", color="danger")
            except Exception as e:
                logger.error(f"Error submitting answer: {str(e)}")
                return dbc.Alert("Error submitting answer.", color="danger")
        
        return ""

if __name__ == "__main__":
    # Initialize the demo app
    if initialize_demo_app():
        print("Starting Delta Scoreboard in Demo Mode...")
        print("Open your browser to: http://127.0.0.1:8050")
        print("Demo emails: demo@example.com, test@example.com, admin@example.com")
        app.run(debug=True, host="127.0.0.1", port=8050)
    else:
        logger.error("Failed to initialize demo app.")
        print("Failed to initialize demo app.") 
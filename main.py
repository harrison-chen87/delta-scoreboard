"""Main Delta Scoreboard Dash application."""

import dash
from dash import dcc, html, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import logging
from typing import Dict, List, Optional

from config import Config
from databricks_utils import DatabricksConnection, UserManager, ResponseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Delta Scoreboard - Workshop Leaderboard"

# Global variables
db_connection = None
user_manager = None
response_manager = None
current_user = None

# Sample workshop questions (replace with your actual questions)
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

def initialize_app_with_credentials(hostname: str, workspace_id: str, http_path: str, token: str):
    """Initialize the database connection and managers with runtime credentials."""
    global db_connection, user_manager, response_manager
    
    try:
        db_connection = DatabricksConnection(hostname=hostname, workspace_id=workspace_id, http_path=http_path, access_token=token)
        if db_connection.connect():
            user_manager = UserManager(db_connection)
            response_manager = ResponseManager(db_connection)
            
            # Create tables if they don't exist
            user_manager.create_users_table()
            response_manager.create_responses_table()
            
            # Sync users from workspace using SCIM API
            user_manager.sync_users_to_table()
            
            logger.info("App initialized successfully with runtime credentials")
            return True
        else:
            logger.error("Failed to connect to database")
            return False
            
    except Exception as e:
        logger.error(f"Error initializing app: {str(e)}")
        return False

def create_header():
    """Create the application header."""
    return dbc.NavbarSimple(
        brand="🏆 Delta Scoreboard",
        brand_href="#",
        color="primary",
        dark=True,
        className="mb-4"
    )

def create_credentials_form():
    """Create the credentials setup form."""
    logger.info("Creating credentials form with Workspace ID field")
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Databricks Configuration", className="card-title text-center mb-4"),
                        html.P("Please enter your Databricks workspace credentials:", className="text-center mb-4"),
                        dbc.Form([
                            dbc.Row([
                                dbc.Label("Workspace Hostname", html_for="hostname-input", width=12),
                                dbc.Col([
                                    dbc.Input(
                                        id="hostname-input",
                                        type="text",
                                        placeholder="your-workspace.cloud.databricks.com",
                                        className="mb-3"
                                    ),
                                ], width=12),
                            ]),
                            dbc.Row([
                                dbc.Label("Workspace ID", html_for="workspace-id-input", width=12),
                                dbc.Col([
                                    dbc.Input(
                                        id="workspace-id-input",
                                        type="text",
                                        placeholder="1234567890123456",
                                        className="mb-3"
                                    ),
                                    dbc.FormText("Found in your workspace URL or admin console", className="mb-3")
                                ], width=12),
                            ]),
                            dbc.Row([
                                dbc.Label("HTTP Path", html_for="http-path-input", width=12),
                                dbc.Col([
                                    dbc.Input(
                                        id="http-path-input",
                                        type="text",
                                        placeholder="/sql/1.0/warehouses/your-warehouse-id",
                                        className="mb-3"
                                    ),
                                ], width=12),
                            ]),
                            dbc.Row([
                                dbc.Label("Access Token", html_for="token-input", width=12),
                                dbc.Col([
                                    dbc.Input(
                                        id="token-input",
                                        type="password",
                                        placeholder="Enter your personal access token",
                                        className="mb-3"
                                    ),
                                ], width=12),
                            ]),
                            dbc.Button("Connect to Databricks", id="connect-btn", color="primary", className="w-100 mb-3"),
                            html.Div(id="connection-message", className="text-center")
                        ])
                    ])
                ], className="shadow")
            ], width=8)
        ], justify="center")
    ])

def create_login_form():
    """Create the login form for user authentication."""
    return dbc.Container([
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
    dcc.Store(id="credentials-data", data=None),
    dcc.Store(id="app-state", data={"page": "credentials", "question_num": 1}),
    dcc.Interval(id="auto-refresh", interval=30000, n_intervals=0),  # Auto-refresh every 30 seconds
    
    create_header(),
    
    html.Div(id="page-content", children=create_credentials_form())
], fluid=True)

# Callbacks
@app.callback(
    [Output("page-content", "children"),
     Output("user-data", "data"),
     Output("credentials-data", "data"),
     Output("app-state", "data")],
    [Input("connect-btn", "n_clicks"),
     Input("login-btn", "n_clicks"),
     Input("view-leaderboard-btn", "n_clicks"),
     Input("back-to-questions-btn", "n_clicks"),
     Input("auto-refresh", "n_intervals")],
    [State("hostname-input", "value"),
     State("workspace-id-input", "value"),
     State("http-path-input", "value"),
     State("token-input", "value"),
     State("email-input", "value"),
     State("user-data", "data"),
     State("credentials-data", "data"),
     State("app-state", "data")]
)
def handle_navigation(connect_clicks, login_clicks, leaderboard_clicks, back_clicks, auto_refresh,
                     hostname, workspace_id, http_path, token, email, user_data, credentials_data, app_state):
    """Handle navigation between pages."""
    ctx = dash.callback_context
    
    # Debug logging
    logger.info(f"Navigation callback triggered: {ctx.triggered}")
    
    if not ctx.triggered:
        logger.info("No trigger found, returning credentials form")
        return create_credentials_form(), None, None, {"page": "credentials", "question_num": 1}
    
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Handle credentials setup
    if trigger_id == "connect-btn" and connect_clicks:
        if Config.validate_runtime_credentials(hostname, http_path, token, workspace_id):
            credentials_data = {
                "hostname": hostname,
                "workspace_id": workspace_id,
                "http_path": http_path,
                "token": token
            }
            app_state = {"page": "login", "question_num": 1}
            return create_login_form(), user_data, credentials_data, app_state
        else:
            return create_credentials_form(), user_data, credentials_data, app_state
    
    # Handle login
    elif trigger_id == "login-btn" and login_clicks and email and credentials_data:
        if user_manager and user_manager.is_user_eligible(email):
            user_data = {"email": email, "user_id": email}
            app_state = {"page": "questions", "question_num": 1}
            return create_questions_page(), user_data, credentials_data, app_state
        else:
            return create_login_form(), None, credentials_data, {"page": "login", "question_num": 1}
    
    # Handle leaderboard view
    elif trigger_id == "view-leaderboard-btn" and leaderboard_clicks:
        app_state["page"] = "leaderboard"
        return create_leaderboard(), user_data, credentials_data, app_state
    
    # Handle back to questions
    elif trigger_id == "back-to-questions-btn" and back_clicks:
        app_state["page"] = "questions"
        return create_questions_page(), user_data, credentials_data, app_state
    
    # Handle auto-refresh
    elif trigger_id == "auto-refresh" and app_state and app_state.get("page") == "leaderboard":
        return create_leaderboard(), user_data, credentials_data, app_state
    
    # Default state based on current page
    if not credentials_data:
        return create_credentials_form(), None, None, {"page": "credentials", "question_num": 1}
    elif not user_data:
        return create_login_form(), None, credentials_data, {"page": "login", "question_num": 1}
    elif app_state.get("page") == "questions":
        return create_questions_page(), user_data, credentials_data, app_state
    elif app_state.get("page") == "leaderboard":
        return create_leaderboard(), user_data, credentials_data, app_state
    else:
        return create_credentials_form(), None, None, {"page": "credentials", "question_num": 1}

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
                    # Create a professional leaderboard table
                    table_header = [
                        html.Thead([
                            html.Tr([
                                html.Th("Rank", className="text-center"),
                                html.Th("Participant", className="text-start"),
                                html.Th("Email", className="text-start"),
                                html.Th("Score", className="text-center"),
                                html.Th("Questions", className="text-center"),
                                html.Th("Progress", className="text-center")
                            ])
                        ])
                    ]
                    
                    table_rows = []
                    for i, row in leaderboard_df.iterrows():
                        rank = i + 1
                        medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                        
                        # Calculate progress percentage
                        max_possible_score = len(WORKSHOP_QUESTIONS) * Config.POINTS_PER_QUESTION
                        progress_percentage = (row['total_points'] / max_possible_score * 100) if max_possible_score > 0 else 0
                        
                        # Style the row based on rank
                        row_class = ""
                        if rank == 1:
                            row_class = "table-warning"
                        elif rank == 2:
                            row_class = "table-light"
                        elif rank == 3:
                            row_class = "table-info"
                        
                        table_rows.append(
                            html.Tr([
                                html.Td(
                                    html.Span(medal, className="h4 mb-0"),
                                    className="text-center align-middle"
                                ),
                                html.Td(
                                    html.Div([
                                        html.Strong(row['display_name'], className="d-block"),
                                        html.Small(f"User #{i+1}", className="text-muted")
                                    ]),
                                    className="align-middle"
                                ),
                                html.Td(row['email'], className="align-middle"),
                                html.Td(
                                    html.Div([
                                        html.Span(f"{row['total_points']}", className="h5 text-primary mb-0"),
                                        html.Small(" pts", className="text-muted")
                                    ]),
                                    className="text-center align-middle"
                                ),
                                html.Td(
                                    html.Div([
                                        html.Span(f"{row['questions_answered']}", className="h6 mb-0"),
                                        html.Small(f"/{len(WORKSHOP_QUESTIONS)}", className="text-muted")
                                    ]),
                                    className="text-center align-middle"
                                ),
                                html.Td(
                                    html.Div([
                                        dbc.Progress(
                                            value=progress_percentage,
                                            color="success" if progress_percentage == 100 else "primary",
                                            className="mb-1"
                                        ),
                                        html.Small(f"{progress_percentage:.0f}%", className="text-muted")
                                    ]),
                                    className="text-center align-middle"
                                )
                            ], className=row_class)
                        )
                    
                    table_body = [html.Tbody(table_rows)]
                    
                    return [
                        html.Div([
                            html.H5(f"🏆 Workshop Leaderboard - {len(leaderboard_df)} Participants", className="mb-3"),
                            dbc.Table(
                                table_header + table_body,
                                bordered=True,
                                hover=True,
                                striped=True,
                                responsive=True,
                                className="shadow-sm"
                            )
                        ])
                    ]
                else:
                    return [
                        html.Div([
                            html.H5("🏆 Workshop Leaderboard", className="mb-3"),
                            dbc.Alert([
                                html.I(className="fas fa-info-circle me-2"),
                                "No scores yet. Be the first to answer questions!"
                            ], color="info", className="text-center")
                        ])
                    ]
            else:
                return [
                    dbc.Alert([
                        html.I(className="fas fa-exclamation-triangle me-2"),
                        "Unable to load leaderboard. Please check your connection."
                    ], color="warning", className="text-center")
                ]
        except Exception as e:
            logger.error(f"Error loading leaderboard: {str(e)}")
            return [
                dbc.Alert([
                    html.I(className="fas fa-times-circle me-2"),
                    "Error loading leaderboard."
                ], color="danger", className="text-center")
            ]
    
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

# Add callback for login feedback
@app.callback(
    Output("login-message", "children"),
    Input("login-btn", "n_clicks"),
    [State("email-input", "value"),
     State("credentials-data", "data")]
)
def handle_login_feedback(n_clicks, email, credentials_data):
    """Handle login feedback."""
    if n_clicks and email and credentials_data:
        if user_manager and user_manager.is_user_eligible(email):
            return dbc.Alert("✅ Login successful!", color="success")
        else:
            return dbc.Alert("❌ Email not found in workspace users.", color="danger")
    elif n_clicks and email and not credentials_data:
        return dbc.Alert("❌ Please set up Databricks connection first.", color="warning")
    return ""

# Add callback for connection feedback
@app.callback(
    Output("connection-message", "children"),
    Input("connect-btn", "n_clicks"),
    [State("hostname-input", "value"),
     State("workspace-id-input", "value"),
     State("http-path-input", "value"),
     State("token-input", "value")]
)
def handle_connection(n_clicks, hostname, workspace_id, http_path, token):
    """Handle connection attempt and provide feedback."""
    if n_clicks and hostname and workspace_id and http_path and token:
        if Config.validate_runtime_credentials(hostname, http_path, token, workspace_id):
            try:
                # Try to initialize with the provided credentials
                if initialize_app_with_credentials(hostname, workspace_id, http_path, token):
                    return dbc.Alert("✅ Successfully connected to Databricks!", color="success")
                else:
                    return dbc.Alert("❌ Failed to connect. Please check your credentials.", color="danger")
            except Exception as e:
                return dbc.Alert(f"❌ Connection error: {str(e)}", color="danger")
        else:
            return dbc.Alert("❌ Please fill in all required fields.", color="warning")
    return ""

if __name__ == "__main__":
    # No need to initialize here - will be done when credentials are provided
    app.run(
        debug=Config.DEBUG,
        host=Config.HOST,
        port=8051  # Use different port to avoid caching issues
    ) 
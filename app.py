"""Delta Drive Workshop Setup - Databricks App Entry Point."""

import dash
from dash import html, dcc, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import json
import os
import logging
import pandas as pd

from infrastructure.resource_manager import SQLWarehouseManager
from databricks.sdk import WorkspaceClient

# Global variable to track created warehouses during session
created_warehouses = []

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Dash app for Databricks deployment
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
server = app.server  # This is needed for Databricks Apps

# T-shirt size options based on Databricks documentation
WAREHOUSE_SIZES = [
    {"label": "2X-Small (1 worker)", "value": "2X-Small"},
    {"label": "X-Small (2 workers)", "value": "X-Small"}, 
    {"label": "Small (4 workers)", "value": "Small"},
    {"label": "Medium (8 workers)", "value": "Medium"},
    {"label": "Large (16 workers)", "value": "Large"},
    {"label": "X-Large (32 workers)", "value": "X-Large"},
    {"label": "2X-Large (64 workers)", "value": "2X-Large"},
    {"label": "3X-Large (128 workers)", "value": "3X-Large"},
    {"label": "4X-Large (256 workers)", "value": "4X-Large"}
]

def create_warehouse_creation_form():
    """Create the SQL warehouse creation form (serverless only)."""
    return dbc.Card([
        dbc.CardBody([
            html.H5("🏗️ Create New Serverless SQL Warehouse", className="card-title mb-4"),
            
            dbc.Label("Warehouse Name", html_for="warehouse-name"),
            dbc.Input(
                id="warehouse-name",
                type="text",
                placeholder="workshop-leaderboard-warehouse",
                className="mb-3"
            ),
            
            dbc.Label("Cluster Size (T-shirt sizing)", html_for="cluster-size"),
            dbc.Select(
                id="cluster-size",
                options=WAREHOUSE_SIZES,
                value="Medium",
                className="mb-3"
            ),
            dbc.FormText("Medium is recommended for workshops (8 workers)", className="mb-3"),
            
            dbc.Alert([
                html.I(className="bi bi-info-circle me-2"),
                "Serverless warehouses provide automatic scaling and cost optimization"
            ], color="info", className="mb-3"),
            
            dbc.Label("Auto Stop", html_for="auto-stop"),
            dbc.Select(
                id="auto-stop",
                options=[
                    {"label": "2 Hours", "value": 120},
                    {"label": "4 Hours", "value": 240},
                    {"label": "8 Hours", "value": 480}
                ],
                value=240,  # Default to 4 hours
                className="mb-3"
            ),
            dbc.FormText("Warehouse will stop automatically after this period of inactivity", className="mb-3"),
            
            dbc.Label("Number of Warehouses", html_for="warehouse-count"),
            dbc.Input(
                id="warehouse-count",
                type="number",
                value=1,
                min=1,
                max=5,
                step=1,
                className="mb-3"
            ),
            dbc.FormText("Create 1-5 warehouses of the same size for load distribution", className="mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Button("Create Serverless Warehouse(s)", id="create-warehouse-btn", color="success", className="w-100")
                ], width=8),
                dbc.Col([
                    dbc.Button("🗑️ Stop & Delete All", id="delete-all-warehouses-btn", color="danger", className="w-100")
                ], width=4)
            ], className="mb-3"),
            html.Div(id="warehouse-creation-message", className="text-center"),
            html.Div(id="warehouse-management-status", className="text-center mt-3")
        ])
    ])

def create_user_management_section():
    """Create the user management section with SCIM API integration."""
    return dbc.Card([
        dbc.CardBody([
            html.H5("🏆 Workshop Leaderboard", className="card-title mb-4"),
            
            dbc.Button(
                [html.I(className="bi bi-database-fill me-2"), "Initialize Leaderboard Database"],
                id="fetch-users-btn",
                color="success",
                className="mb-3"
            ),
            
            # Default leaderboard structure - will be replaced by callback
            html.Div([
                html.Div([
                    html.H6([
                        html.I(className="bi bi-trophy-fill me-2", style={"color": "#ffc107"}),
                        "Workshop Leaderboard - Database Mode"
                    ], className="mb-3 text-center"),
                    
                    dbc.Alert([
                        html.H6("🚀 New Database-Driven Leaderboard!", className="alert-heading"),
                        html.P("This leaderboard now uses your SQL warehouse for storage and real-time updates."),
                        html.P("📋 Steps to initialize:"),
                        html.Ol([
                            html.Li("Create a SQL warehouse in the Infrastructure section above"),
                            html.Li("Click 'Initialize Leaderboard Database' to store all participants"),
                            html.Li("View live leaderboard powered by your SQL warehouse")
                        ]),
                        html.P("🎯 Scores can be updated directly in the database during workshop activities!")
                    ], color="info", className="mb-3"),
                    
                    html.Hr(),
                    html.P("📊 The leaderboard will show participant rankings, names, emails, active status, and scores from the SQL warehouse.", 
                           className="text-muted text-center small")
                ])
            ], id="users-table-container", className="mt-3"),
            
            html.Div(id="fetch-users-message", className="text-center mt-3")
        ])
    ])



app.layout = html.Div([
    html.H1("🏗️ Delta Drive Workshop Setup", className="text-center text-success mb-4"),
    html.Hr(),
    
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H2("🔧 Connection Setup", className="mb-3"),
                dbc.Card([
                    dbc.CardBody([
                        dbc.Label("Workspace Hostname", html_for="hostname"),
                        dbc.Input(
                            id="hostname",
                            type="text",
                            placeholder="your-workspace.cloud.databricks.com",
                            className="mb-3"
                        ),
                        dbc.Label("Workspace ID", html_for="workspace-id"),
                        dbc.Input(
                            id="workspace-id",
                            type="text",
                            placeholder="1234567890123456",
                            className="mb-3"
                        ),
                        dbc.Label("Access Token", html_for="access-token"),
                        dbc.Input(
                            id="access-token",
                            type="password",
                            placeholder="dapi1234567890abcdef",
                            className="mb-3"
                        ),
                        dbc.FormText("Need Admin permissions to create warehouses and fetch users", className="text-warning")
                    ])
                ])
            ], width=6),
            
            dbc.Col([
                html.H2("🏭 Create SQL Warehouse", className="mb-3"),
                create_warehouse_creation_form()
            ], width=6)
        ], className="mb-4"),
        
        dbc.Row([
                            dbc.Col([
                    html.H2("👥 User Management", className="mb-3"),
                    create_user_management_section()
                ], width=12)
            ], className="mb-4"),
            

    ], fluid=True)
])

# Callback for SQL warehouse creation (serverless only)
@app.callback(
    Output("warehouse-creation-message", "children"),
    Input("create-warehouse-btn", "n_clicks"),
    [State("hostname", "value"),
     State("access-token", "value"),
     State("warehouse-name", "value"),
     State("cluster-size", "value"),
     State("auto-stop", "value"),
     State("warehouse-count", "value")]
)
def create_sql_warehouse(n_clicks, hostname, access_token, warehouse_name, cluster_size, auto_stop, warehouse_count):
    """Handle serverless SQL warehouse creation using the resource manager."""
    if not n_clicks:
        return ""
    
    # Validate inputs
    if not all([hostname, access_token, warehouse_name, cluster_size]):
        return dbc.Alert("❌ Please fill in all required fields", color="danger")
    
    try:
        # Convert warehouse_count to integer and validate
        try:
            warehouse_count = int(warehouse_count) if warehouse_count else 1
            if warehouse_count < 1 or warehouse_count > 5:
                return dbc.Alert("❌ Number of warehouses must be between 1 and 5", color="danger")
        except (ValueError, TypeError):
            return dbc.Alert("❌ Please enter a valid number for warehouse count", color="danger")
        
        # Convert auto_stop to integer
        try:
            auto_stop = int(auto_stop) if auto_stop else 240
        except (ValueError, TypeError):
            return dbc.Alert("❌ Invalid auto-stop value", color="danger")
        
        # Initialize the SQL warehouse manager
        warehouse_manager = SQLWarehouseManager(hostname, access_token)
        
        # Create warehouses using the resource manager
        logger.info(f"Creating {warehouse_count} warehouse(s) with name '{warehouse_name}' and size '{cluster_size}'")
        
        # Show progress message
        progress_message = dbc.Alert([
            dbc.Spinner(size="sm"),
            f" Creating {warehouse_count} serverless warehouse(s)..."
        ], color="info")
        
        results = warehouse_manager.create_multiple_warehouses(
            base_name=warehouse_name,
            cluster_size=cluster_size,
            auto_stop_mins=auto_stop,
            count=warehouse_count
        )
        
        # Process results
        successful_warehouses = [r for r in results if r.success]
        failed_warehouses = [r for r in results if not r.success]
        
        # Store created warehouse IDs in global list for tracking
        global created_warehouses
        for warehouse in successful_warehouses:
            created_warehouses.append({
                'id': warehouse.id,
                'name': warehouse.name,
                'http_path': warehouse.http_path
            })
        
        # Convert minutes to hours for display
        auto_stop_hours = auto_stop // 60
        
        if successful_warehouses:
            # Create success message
            success_count = len(successful_warehouses)
            success_title = f"✅ {success_count} Serverless SQL Warehouse{'s' if success_count > 1 else ''} Created Successfully!"
            
            warehouse_details = []
            for i, warehouse in enumerate(successful_warehouses):
                warehouse_details.extend([
                    html.H6(f"Warehouse {i+1}:", className="text-success mt-2"),
                    html.P(f"Name: {warehouse.name}"),
                    html.P(f"ID: {warehouse.id}"),
                    html.P(f"HTTP Path: {warehouse.http_path}")
                ])
            
            # Add error details if some failed
            if failed_warehouses:
                warehouse_details.append(html.Hr())
                warehouse_details.append(html.H6("⚠️ Some warehouses failed to create:", className="text-warning"))
                for failed in failed_warehouses:
                    warehouse_details.extend([
                        html.P(f"Failed: {failed.name} - {failed.error}", className="text-danger small")
                    ])
            
            return dbc.Alert([
                html.H6(success_title, className="alert-heading"),
                html.P(f"Size: {cluster_size}"),
                html.P(f"Type: Serverless (PRO)"),
                html.P(f"Auto-stop: {auto_stop_hours} hours"),
                html.Hr(),
                html.H6("📋 Connection Details:", className="text-success"),
                *warehouse_details,
                html.Hr(),
                html.P(f"📊 Total warehouses being tracked: {len(created_warehouses)}", className="mb-1"),
                html.P("Use the red '🗑️ Stop & Delete All' button to clean up all resources when done", className="mb-0 text-muted")
            ], color="success" if not failed_warehouses else "warning")
        
        else:
            # All warehouses failed
            error_details = []
            for failed in failed_warehouses:
                error_details.append(html.P(f"• {failed.name}: {failed.error}"))
            
            return dbc.Alert([
                html.H6("❌ All Warehouse Creation Failed", className="alert-heading"),
                html.P("The following errors occurred:"),
                *error_details,
                html.Hr(),
                html.P("Please check your credentials and try again.", className="small text-muted")
            ], color="danger")
        
    except Exception as e:
        logger.error(f"Unexpected error in warehouse creation: {str(e)}")
        return dbc.Alert(f"❌ Unexpected error: {str(e)}", color="danger")

# Callback for fetching users from SCIM API
@app.callback(
    [Output("users-table-container", "children"),
     Output("fetch-users-message", "children")],
    Input("fetch-users-btn", "n_clicks"),
    [State("hostname", "value"),
     State("access-token", "value")]
)
def fetch_users_from_scim(n_clicks, hostname, access_token):
    """Fetch users from Databricks workspace using the official SDK users.list() method."""
    logger.info(f"=== FETCH USERS CALLBACK START ===")
    logger.info(f"n_clicks={n_clicks}, hostname_provided={hostname is not None}, token_provided={access_token is not None}")
    
    if not n_clicks:
        logger.info("No clicks detected, returning empty values")
        return "", ""
    
    # Show loading state immediately
    loading_message = dbc.Alert([
        dbc.Spinner(size="sm"),
        " 🔄 Fetching users from workspace... This may take a moment for large workspaces."
    ], color="info")
    logger.info("Showing loading state to user")
    
    # Validate inputs
    if not all([hostname, access_token]):
        error_msg = "❌ Please fill in hostname and access token"
        logger.warning(error_msg)
        logger.info(f"Returning validation error: {error_msg}")
        return "", dbc.Alert(error_msg, color="danger")
    
    # Test return to verify callback is working
    logger.info("✅ Validation passed, proceeding with user fetch")
    
    logger.info(f"Fetching users from Databricks workspace: {hostname}")
    
    try:
        # Clean hostname to avoid double https:// prefix
        clean_hostname = hostname.replace("https://", "").replace("http://", "")
        logger.info(f"Cleaned hostname: {clean_hostname}")
        
        # Initialize WorkspaceClient as per official SDK documentation
        workspace_client = WorkspaceClient(
            host=f"https://{clean_hostname}",
            token=access_token,
            auth_type="pat"
        )
        logger.info("WorkspaceClient initialized successfully")
        
        # Use the official SDK users.list() method
        logger.info("Calling workspace_client.users.list() as per official SDK documentation")
        users_iterator = workspace_client.users.list(
            attributes="id,userName,displayName,emails,active",
            sort_by="userName"
        )
        
        # Convert iterator to list and process users
        users_data = []
        user_count = 0
        
        for user in users_iterator:
            user_count += 1
            # Only log progress for first 5 users and every 1000th user
            if user_count <= 5 or user_count % 1000 == 0:
                logger.info(f"Processing user {user_count}: {getattr(user, 'user_name', 'Unknown')}")
            
            # Check if user is active (default to True if not specified)
            is_active = getattr(user, 'active', True)
            
            if is_active:
                # Extract user information safely
                user_id = getattr(user, 'id', '')
                display_name = getattr(user, 'display_name', '')
                user_name = getattr(user, 'user_name', '')
                
                # Extract email from emails array
                email = ""
                emails = getattr(user, 'emails', [])
                if emails and len(emails) > 0:
                    # Handle both list and ComplexValue formats
                    first_email = emails[0]
                    if hasattr(first_email, 'value'):
                        email = first_email.value
                    elif isinstance(first_email, dict):
                        email = first_email.get('value', '')
                    else:
                        email = str(first_email)
                
                users_data.append({
                    'ID': user_id,
                    'Display Name': display_name,
                    'Email': email,
                    'Username': user_name,
                    'Status': '✅ Active'
                })
                
                logger.info(f"Added active user: {user_name} ({email})")
        
        logger.info(f"Successfully processed {len(users_data)} active users from {user_count} total users")
        
        # If no users found, fall back to demo data
        if not users_data:
            logger.warning("No active users found in workspace, falling back to demo data")
            users_data = [
                {
                    'ID': 'demo-user-1',
                    'Display Name': 'John Doe',
                    'Email': 'john.doe@company.com',
                    'Username': 'john.doe@company.com',
                    'Status': '⚠️ Demo Data'
                },
                {
                    'ID': 'demo-user-2',
                    'Display Name': 'Jane Smith',
                    'Email': 'jane.smith@company.com',
                    'Username': 'jane.smith@company.com',
                    'Status': '⚠️ Demo Data'
                },
                {
                    'ID': 'demo-user-3',
                    'Display Name': 'Bob Johnson',
                    'Email': 'bob.johnson@company.com',
                    'Username': 'bob.johnson@company.com',
                    'Status': '⚠️ Demo Data'
                },
                {
                    'ID': 'demo-user-4',
                    'Display Name': 'Digital Workshop Admin',
                    'Email': 'admin@company.com',
                    'Username': 'admin@company.com',
                    'Status': '⚠️ Demo Data'
                },
                {
                    'ID': 'demo-user-5',
                    'Display Name': 'Charlie Davis',
                    'Email': 'charlie.davis@company.com',
                    'Username': 'charlie.davis@company.com',
                    'Status': '⚠️ Demo Data'
                }
            ]

        # Create the leaderboard-themed users table with scroll functionality
        display_limit = 200  # Increased limit for better leaderboard view
        display_users = users_data[:display_limit]
        logger.info(f"Creating leaderboard table with {len(display_users)} displayed users (out of {len(users_data)} total)")
        
        table_rows = []
        for i, user in enumerate(display_users):
            if i < 5:  # Only log first 5 users to avoid spam
                logger.info(f"Creating leaderboard row {i+1}: {user.get('Display Name', 'No name')} ({user.get('Email', 'No email')})")
            
            # Initialize score as 0 for new participants
            initial_score = 0
            
            table_rows.append(
                html.Tr([
                    html.Td(f"#{i+1}", style={"font-weight": "bold", "color": "#0d6efd"}),  # Rank
                    html.Td(user['Display Name'], style={"font-weight": "500"}),
                    html.Td(user['Email'], style={"color": "#6c757d"}),
                    html.Td("✅ Yes" if user['Status'] == '✅ Active' else "❌ No", 
                           style={"color": "#198754" if user['Status'] == '✅ Active' else "#dc3545"}),
                    html.Td(f"{initial_score} pts", style={"font-weight": "bold", "color": "#198754"})
                ])
            )
        
        # Create scrollable table container with leaderboard styling
        users_table = html.Div([
            html.H5([
                html.I(className="bi bi-trophy-fill me-2", style={"color": "#ffc107"}),
                f"Workshop Leaderboard - {len(display_users)} Participants"
            ], className="mb-3"),
            
            html.Div([
                dbc.Table([
                    html.Thead([
                        html.Tr([
                            html.Th("🏆 Rank", style={"background-color": "#f8f9fa", "border-bottom": "2px solid #dee2e6"}),
                            html.Th("👤 Participant Name", style={"background-color": "#f8f9fa", "border-bottom": "2px solid #dee2e6"}),
                            html.Th("📧 Email Address", style={"background-color": "#f8f9fa", "border-bottom": "2px solid #dee2e6"}),
                            html.Th("📊 Active on Workspace", style={"background-color": "#f8f9fa", "border-bottom": "2px solid #dee2e6"}),
                            html.Th("🎯 Current Score", style={"background-color": "#f8f9fa", "border-bottom": "2px solid #dee2e6"})
                        ])
                    ]),
                    html.Tbody(table_rows)
                ], bordered=True, hover=True, striped=True, responsive=True)
            ], style={
                "max-height": "500px", 
                "overflow-y": "auto", 
                "border": "1px solid #dee2e6",
                "border-radius": "8px"
            })
        ], className="mt-3")
        
        logger.info(f"Leaderboard table created successfully with {len(table_rows)} participant rows")

        # Check if we're showing demo data
        is_demo_data = any(user.get('Status', '').startswith('⚠️') for user in users_data)
        
        if is_demo_data:
            success_message = dbc.Alert([
                html.H6("⚠️ Demo Data Displayed", className="alert-heading"),
                html.P(f"Unable to fetch real users from workspace. Showing {len(users_data)} demo users instead."),
                html.P("This may be due to API permissions or network restrictions in the deployed environment."),
                html.P("In a real workshop setup, this would show actual workspace users with their email addresses.")
            ], color="warning")
        else:
            if len(users_data) > display_limit:
                success_message = dbc.Alert([
                    html.H6("🎉 Leaderboard Initialized Successfully!", className="alert-heading"),
                    html.P(f"🏆 Found {len(users_data)} eligible participants in the workspace"),
                    html.P(f"📊 Displaying top {display_limit} participants in the leaderboard below"),
                    html.P("🎯 All participants start with 0 points. Scores will update as workshop activities are completed!")
                ], color="success")
            else:
                success_message = dbc.Alert([
                    html.H6("🎉 Leaderboard Initialized Successfully!", className="alert-heading"),
                    html.P(f"🏆 Found {len(users_data)} eligible participants in the workspace"),
                    html.P("🎯 All participants start with 0 points. Scores will update as workshop activities are completed!")
                ], color="success")
        
        logger.info(f"=== CALLBACK RETURN PREPARATION ===")
        logger.info(f"📊 Returning leaderboard with {len(display_users)} displayed participants (out of {len(users_data)} total)")
        logger.info(f"🏆 Users table type: {type(users_table)}")
        logger.info(f"✅ Success message type: {type(success_message)}")
        logger.info(f"👥 First few participants: {users_data[:2] if users_data else 'No users'}")
        
        # Verify the table structure
        if hasattr(users_table, 'children') and users_table.children:
            logger.info(f"📋 Table structure verified: {len(users_table.children)} components")
        else:
            logger.warning("⚠️  Table structure may be incorrect")
        
        logger.info("🔄 Executing callback return...")
        
        # Convert users to DataFrame for database storage
        logger.info(f"Converting {len(users_data)} users to DataFrame")
        
        # Create DataFrame with leaderboard structure
        df_data = []
        for i, user in enumerate(users_data):
            df_data.append({
                'participant_id': user['ID'],
                'rank': i + 1,
                'display_name': user['Display Name'],
                'email': user['Email'],
                'username': user['Username'],
                'is_active': user['Status'] == '✅ Active',
                'status': 'Active' if user['Status'] == '✅ Active' else 'Inactive',
                'score': 0,  # Initialize all scores to 0
                'last_updated': pd.Timestamp.now()
            })
        
        participants_df = pd.DataFrame(df_data)
        logger.info(f"Created DataFrame with {len(participants_df)} participants")
        
        # Store DataFrame in SQL warehouse
        warehouse_result = store_leaderboard_in_warehouse(participants_df, hostname, access_token)
        
        if warehouse_result['success']:
            # Query the warehouse to populate the UI
            leaderboard_ui = query_leaderboard_from_warehouse(hostname, access_token)
            success_message = dbc.Alert([
                html.H6("🏆 Leaderboard Initialized Successfully!", className="alert-heading"),
                html.P(f"✅ Stored {len(users_data)} participants in SQL warehouse"),
                html.P(f"📊 Database table created: {warehouse_result['table_name']}"),
                html.P("🎯 Leaderboard is now ready for workshop activities!")
            ], color="success")
            
            logger.info(f"Successfully stored leaderboard in warehouse and created UI")
            return leaderboard_ui, success_message
        else:
            # Fallback to simple UI display if warehouse storage fails
            error_message = dbc.Alert([
                html.H6("⚠️ Warehouse Storage Failed", className="alert-heading"), 
                html.P(f"❌ {warehouse_result['error']}"),
                html.P("Showing participants in memory instead...")
            ], color="warning")
            
            # Create simple fallback table
            fallback_table = create_simple_leaderboard_table(users_data[:20])
            return fallback_table, error_message

    except Exception as e:
        error_msg = f"❌ Error fetching users with Databricks SDK: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Fall back to demo data on any error
        logger.info("Falling back to demo data due to SDK error")
        users_data = [
            {
                'ID': 'demo-user-1',
                'Display Name': 'John Doe',
                'Email': 'john.doe@company.com',
                'Username': 'john.doe@company.com',
                'Status': '⚠️ Demo Data'
            },
            {
                'ID': 'demo-user-2',
                'Display Name': 'Jane Smith',
                'Email': 'jane.smith@company.com',
                'Username': 'jane.smith@company.com',
                'Status': '⚠️ Demo Data'
            },
            {
                'ID': 'demo-user-3',
                'Display Name': 'Bob Johnson',
                'Email': 'bob.johnson@company.com',
                'Username': 'bob.johnson@company.com',
                'Status': '⚠️ Demo Data'
            },
            {
                'ID': 'demo-user-4',
                'Display Name': 'Digital Workshop Admin',
                'Email': 'admin@company.com',
                'Username': 'admin@company.com',
                'Status': '⚠️ Demo Data'
            },
            {
                'ID': 'demo-user-5',
                'Display Name': 'Charlie Davis',
                'Email': 'charlie.davis@company.com',
                'Username': 'charlie.davis@company.com',
                'Status': '⚠️ Demo Data'
            }
        ]
        
        # Create the users table with demo data
        users_table = dbc.Table([
            html.Thead([
                html.Tr([
                    html.Th("👤 User ID"),
                    html.Th("📝 Display Name"),
                    html.Th("📧 Email Address"),
                    html.Th("🔑 Username"),
                    html.Th("🔄 Status")
                ])
            ]),
            html.Tbody([
                html.Tr([
                    html.Td(user['ID'][:12] + "..." if len(user['ID']) > 15 else user['ID']),
                    html.Td(user['Display Name']),
                    html.Td(user['Email']),
                    html.Td(user['Username']),
                    html.Td(user['Status'])
                ]) for user in users_data
            ])
        ], bordered=True, hover=True, striped=True, className="mt-3")

        error_alert = dbc.Alert([
            html.H6("⚠️ SDK Error - Demo Data Displayed", className="alert-heading"),
            html.P(f"SDK Error: {str(e)}"),
            html.P("Showing demo data instead. In a real workshop, this would show actual workspace users."),
            html.P("Check logs for detailed error information.")
        ], color="warning")
        
        return users_table, error_alert

# Callback for stopping and deleting all created warehouses
@app.callback(
    Output("warehouse-management-status", "children"),
    Input("delete-all-warehouses-btn", "n_clicks"),
    [State("hostname", "value"),
     State("access-token", "value")]
)
def stop_and_delete_all_warehouses(n_clicks, hostname, access_token):
    """Stop and delete all warehouses created via the UI."""
    if not n_clicks:
        return ""
    
    # Validate inputs
    if not all([hostname, access_token]):
        return dbc.Alert("❌ Please fill in hostname and access token", color="danger")
    
    global created_warehouses
    
    if not created_warehouses:
        return dbc.Alert("📭 No warehouses to delete. Create some warehouses first!", color="info")
    
    try:
        # Initialize the SQL warehouse manager
        warehouse_manager = SQLWarehouseManager(hostname, access_token)
        
        # Show progress message
        warehouse_count = len(created_warehouses)
        progress_message = dbc.Alert([
            dbc.Spinner(size="sm"),
            f" Stopping and deleting {warehouse_count} warehouse(s)..."
        ], color="warning")
        
        # Stop and delete each warehouse
        deleted_count = 0
        failed_deletions = []
        
        for warehouse in created_warehouses:
            try:
                warehouse_id = warehouse['id']
                warehouse_name = warehouse['name']
                
                # First try to stop the warehouse
                logger.info(f"Stopping warehouse: {warehouse_name} ({warehouse_id})")
                warehouse_manager.stop_warehouse(warehouse_id)
                
                # Then delete the warehouse
                logger.info(f"Deleting warehouse: {warehouse_name} ({warehouse_id})")
                success = warehouse_manager.delete_warehouse(warehouse_id)
                
                if success:
                    deleted_count += 1
                    logger.info(f"Successfully deleted warehouse: {warehouse_name}")
                else:
                    failed_deletions.append(warehouse_name)
                    logger.error(f"Failed to delete warehouse: {warehouse_name}")
                    
            except Exception as e:
                failed_deletions.append(warehouse['name'])
                logger.error(f"Error deleting warehouse {warehouse['name']}: {str(e)}")
        
        # Clear the created warehouses list
        created_warehouses.clear()
        
        # Return success/error message
        if deleted_count == warehouse_count:
            return dbc.Alert([
                html.H6("✅ All Warehouses Deleted Successfully!", className="alert-heading"),
                html.P(f"Successfully stopped and deleted {deleted_count} warehouses"),
                html.P("All resources have been cleaned up")
            ], color="success")
        elif deleted_count > 0:
            return dbc.Alert([
                html.H6("⚠️ Partial Success", className="alert-heading"),
                html.P(f"Successfully deleted {deleted_count} out of {warehouse_count} warehouses"),
                html.P(f"Failed to delete: {', '.join(failed_deletions)}")
            ], color="warning")
        else:
            return dbc.Alert([
                html.H6("❌ Deletion Failed", className="alert-heading"),
                html.P(f"Failed to delete any warehouses"),
                html.P(f"Failed warehouses: {', '.join(failed_deletions)}")
            ], color="danger")
            
    except Exception as e:
        logger.error(f"Error in stop_and_delete_all_warehouses: {str(e)}")
        return dbc.Alert(f"❌ Error managing warehouses: {str(e)}", color="danger")


def store_leaderboard_in_warehouse(df, hostname, access_token):
    """Store the leaderboard DataFrame in the SQL warehouse."""
    try:
        logger.info(f"Storing leaderboard DataFrame with {len(df)} participants in warehouse")
        
        # Create Databricks WorkspaceClient
        workspace_client = WorkspaceClient(host=hostname, token=access_token)
        
        # Check if we have any running warehouses
        warehouses = workspace_client.warehouses.list()
        running_warehouse = None
        
        for warehouse in warehouses:
            if warehouse.state and warehouse.state.name == "RUNNING":
                running_warehouse = warehouse
                break
        
        if not running_warehouse:
            logger.warning("No running warehouse found, attempting to start one")
            # Try to start the first available warehouse
            for warehouse in warehouses:
                try:
                    workspace_client.warehouses.start(warehouse.id)
                    running_warehouse = warehouse
                    logger.info(f"Started warehouse: {warehouse.name}")
                    break
                except Exception as e:
                    logger.warning(f"Failed to start warehouse {warehouse.name}: {e}")
                    continue
        
        if not running_warehouse:
            return {
                'success': False,
                'error': 'No SQL warehouse available. Please create a warehouse first.'
            }
        
        # Create the database table name
        table_name = "workshop_leaderboard"
        database_name = "default"
        full_table_name = f"{database_name}.{table_name}"
        
        # Create the SQL table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            participant_id STRING,
            rank INT,
            display_name STRING,
            email STRING,
            username STRING,
            is_active BOOLEAN,
            status STRING,
            score INT,
            last_updated TIMESTAMP
        ) USING DELTA
        """
        
        # Execute the create table SQL
        workspace_client.statement_execution.execute_statement(
            warehouse_id=running_warehouse.id,
            statement=create_table_sql
        )
        
        logger.info(f"Created table: {full_table_name}")
        
        # Clear existing data
        clear_sql = f"DELETE FROM {full_table_name}"
        workspace_client.statement_execution.execute_statement(
            warehouse_id=running_warehouse.id,
            statement=clear_sql
        )
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            # Create INSERT statements with proper escaping
            values_list = []
            for _, row in batch_df.iterrows():
                # Escape single quotes in string values
                participant_id = str(row['participant_id']).replace("'", "''")
                display_name = str(row['display_name']).replace("'", "''")
                email = str(row['email']).replace("'", "''")
                username = str(row['username']).replace("'", "''")
                status = str(row['status']).replace("'", "''")
                
                values_list.append(f"""
                    ('{participant_id}', {row['rank']}, '{display_name}', 
                     '{email}', '{username}', {str(row['is_active']).lower()}, 
                     '{status}', {row['score']}, '{row['last_updated']}')
                """)
            
            insert_sql = f"""
            INSERT INTO {full_table_name} 
            (participant_id, rank, display_name, email, username, is_active, status, score, last_updated)
            VALUES {', '.join(values_list)}
            """
            
            workspace_client.statement_execution.execute_statement(
                warehouse_id=running_warehouse.id,
                statement=insert_sql
            )
            
            logger.info(f"Inserted batch {i//batch_size + 1} with {len(batch_df)} records")
        
        logger.info(f"Successfully stored {len(df)} participants in {full_table_name}")
        
        return {
            'success': True,
            'table_name': full_table_name,
            'warehouse_id': running_warehouse.id,
            'record_count': len(df)
        }
        
    except Exception as e:
        logger.error(f"Error storing leaderboard in warehouse: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def query_leaderboard_from_warehouse(hostname, access_token):
    """Query the leaderboard from the SQL warehouse and return UI components."""
    try:
        logger.info("Querying leaderboard from warehouse")
        
        # Create Databricks WorkspaceClient
        workspace_client = WorkspaceClient(host=hostname, token=access_token)
        
        # Find running warehouse
        warehouses = workspace_client.warehouses.list()
        running_warehouse = None
        
        for warehouse in warehouses:
            if warehouse.state and warehouse.state.name == "RUNNING":
                running_warehouse = warehouse
                break
        
        if not running_warehouse:
            raise Exception("No running warehouse found")
        
        # Query the leaderboard (top 50 for UI performance)
        query_sql = """
        SELECT rank, display_name, email, status, score, last_updated
        FROM default.workshop_leaderboard
        ORDER BY rank
        LIMIT 50
        """
        
        result = workspace_client.statement_execution.execute_statement(
            warehouse_id=running_warehouse.id,
            statement=query_sql
        )
        
        # Parse the results
        leaderboard_data = []
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                leaderboard_data.append({
                    'rank': row[0],
                    'display_name': row[1],
                    'email': row[2],
                    'status': row[3],
                    'score': row[4],
                    'last_updated': row[5]
                })
        
        # Create the leaderboard UI
        table_rows = []
        for participant in leaderboard_data:
            table_rows.append(html.Tr([
                html.Td(f"#{participant['rank']}", style={"font-weight": "bold"}),
                html.Td(participant['display_name']),
                html.Td(participant['email'], style={"color": "#6c757d"}),
                html.Td("✅ Active" if participant['status'] == 'Active' else "❌ Inactive"),
                html.Td(f"{participant['score']} pts", style={"font-weight": "bold", "color": "#198754"})
            ]))
        
        leaderboard_ui = html.Div([
            html.H6([
                html.I(className="bi bi-trophy-fill me-2", style={"color": "#ffc107"}),
                f"Workshop Leaderboard - Live from SQL Warehouse"
            ], className="mb-3"),
            
            dbc.Table([
                html.Thead([
                    html.Tr([
                        html.Th("🏆 Rank"),
                        html.Th("👤 Name"),
                        html.Th("📧 Email"),
                        html.Th("📊 Status"),
                        html.Th("🎯 Score")
                    ])
                ]),
                html.Tbody(table_rows)
            ], bordered=True, striped=True, hover=True),
            
            html.P(f"Showing top {len(leaderboard_data)} participants from SQL warehouse", 
                   className="text-muted small mt-2"),
            
            dbc.Button("Refresh Leaderboard", color="outline-primary", size="sm", className="mt-2")
        ])
        
        logger.info(f"Created leaderboard UI with {len(leaderboard_data)} participants")
        return leaderboard_ui
        
    except Exception as e:
        logger.error(f"Error querying leaderboard from warehouse: {str(e)}")
        return html.Div([
            html.H6("❌ Failed to Load Leaderboard", className="text-danger"),
            html.P(f"Error: {str(e)}")
        ])


def create_simple_leaderboard_table(users_data):
    """Create a simple fallback leaderboard table for in-memory display."""
    table_rows = []
    for i, user in enumerate(users_data):
        table_rows.append(html.Tr([
            html.Td(f"#{i+1}"),
            html.Td(user['Display Name']),
            html.Td(user['Email']),
            html.Td("✅ Active" if user['Status'] == '✅ Active' else "❌ Inactive"),
            html.Td("0 pts")
        ]))
    
    return html.Div([
        html.H6([
            html.I(className="bi bi-trophy-fill me-2", style={"color": "#ffc107"}),
            f"Workshop Leaderboard - Memory Mode"
        ], className="mb-3"),
        
        dbc.Table([
            html.Thead([
                html.Tr([
                    html.Th("Rank"),
                    html.Th("Name"),
                    html.Th("Email"),
                    html.Th("Status"),
                    html.Th("Score")
                ])
            ]),
            html.Tbody(table_rows)
        ], bordered=True, striped=True, hover=True),
        
        html.P(f"Showing {len(users_data)} participants (fallback mode)", 
               className="text-muted small mt-2")
    ])


# For Databricks Apps deployment
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port) 
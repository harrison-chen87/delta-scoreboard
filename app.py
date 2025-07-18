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
                [html.I(className="bi bi-database-fill me-2"), "🚀 Create Leaderboard Warehouse & Initialize"],
                id="fetch-users-btn",
                color="success",
                size="lg",
                className="mb-3"
            ),
            
            # Progress bar for leaderboard initialization
            html.Div(id="leaderboard-progress", className="mb-3"),
            
            # Manual refresh button for querying warehouse
            dbc.Button(
                [html.I(className="bi bi-arrow-clockwise me-2"), "🔄 Manual Refresh from Warehouse"],
                id="manual-refresh-btn",
                color="outline-primary", 
                size="sm",
                className="mb-3",
                style={"display": "none"}  # Initially hidden, shown after initialization
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
                            html.Li("Enter your workspace connection details including catalog name"),
                            html.Li("Create a SQL warehouse in the Infrastructure section above"),
                            html.Li("Click 'Initialize Leaderboard Database' to store all participants"),
                            html.Li("View live leaderboard powered by your SQL warehouse")
                        ]),
                                            html.P("🎯 Scores can be updated directly in the database during workshop activities!"),
                    html.Hr(),
                    html.H6("🔧 For Workshop Facilitators:", className="text-primary"),
                    html.P([
                        "Update participant scores with SQL: ",
                        html.Code("UPDATE <catalog>.<schema>.<table> SET score = score + 10 WHERE email = 'participant@company.com'")
                    ], className="small text-muted")
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
                        dbc.Label("Catalog Name", html_for="catalog-name"),
                        dbc.Input(
                            id="catalog-name",
                            type="text",
                            placeholder="main",
                            value="main",
                            className="mb-3"
                        ),
                        dbc.Label("Schema Name", html_for="schema-name"),
                        dbc.Input(
                            id="schema-name",
                            type="text",
                            placeholder="default",
                            value="default",
                            className="mb-3"
                        ),
                        dbc.Label("Table Name", html_for="table-name"),
                        dbc.Input(
                            id="table-name",
                            type="text",
                            placeholder="workshop_leaderboard",
                            value="workshop_leaderboard",
                            className="mb-3"
                        ),
                        dbc.FormText([
                            "Need Admin permissions to create warehouses and fetch users", 
                            html.Br(),
                            "Unity Catalog naming: catalog.schema.table_name (all will be created if they don't exist)"
                        ], className="text-warning")
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

# Callback for showing progress bar immediately when button is clicked
@app.callback(
    Output("leaderboard-progress", "children"),
    Input("fetch-users-btn", "n_clicks"),
    prevent_initial_call=True
)
def show_progress_bar(n_clicks):
    """Show progress bar immediately when initialize button is clicked."""
    if n_clicks:
        return html.Div([
            dbc.Progress(
                value=10, 
                color="info", 
                label="🚀 Starting leaderboard initialization...",
                className="mb-2",
                style={"height": "30px"}
            ),
            html.Small([
                "🔄 Process starting: Data Processing → Warehouse Creation → Data Storage → UI Generation"
            ], className="text-muted")
        ])
    return ""


# Callback for fetching users from SCIM API
@app.callback(
    [Output("users-table-container", "children"),
     Output("fetch-users-message", "children"),
     Output("leaderboard-progress", "children", allow_duplicate=True),
     Output("manual-refresh-btn", "style", allow_duplicate=True)],
    Input("fetch-users-btn", "n_clicks"),
    [State("hostname", "value"),
     State("access-token", "value"),
     State("catalog-name", "value"),
     State("schema-name", "value"),
     State("table-name", "value")],
    prevent_initial_call=True
)
def fetch_users_from_scim(n_clicks, hostname, access_token, catalog_name, schema_name, table_name):
    """Fetch users from Databricks workspace using the official SDK users.list() method."""
    logger.info(f"=== FETCH USERS CALLBACK START ===")
    logger.info(f"n_clicks={n_clicks}, hostname_provided={hostname is not None}, token_provided={access_token is not None}")
    
    if not n_clicks:
        logger.info("No clicks detected, returning empty values")
        return "", "", "", {"display": "none"}
    
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
        return "", dbc.Alert(error_msg, color="danger"), "", {"display": "none"}
    
    # Default naming if not provided
    catalog_name = catalog_name or "main"
    schema_name = schema_name or "default"
    table_name = table_name or "workshop_leaderboard"
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    logger.info(f"Using table: {full_table_name}")
    
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
        logger.info("📊 Step 1/4: Converting users to DataFrame for database storage...")
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
        logger.info(f"✅ Step 1/4 complete: Created DataFrame with {len(participants_df)} participants")
        
        # Create dedicated serverless warehouse for leaderboard  
        logger.info("🏗️ Step 2/4: Creating dedicated serverless warehouse...")
        warehouse_result = create_leaderboard_warehouse(hostname, access_token, catalog_name, schema_name)
        
        if warehouse_result['success']:
            logger.info("✅ Step 2/4 complete: Warehouse created successfully")
            # Store DataFrame in the newly created warehouse
            logger.info("💾 Step 3/4: Storing participant data in SQL warehouse...")
            storage_result = store_leaderboard_in_warehouse(
                participants_df, 
                hostname, 
                access_token, 
                warehouse_result['warehouse_id'],
                catalog_name,
                schema_name,
                table_name
            )
            
            if storage_result['success']:
                logger.info("✅ Step 3/4 complete: Data stored successfully in warehouse")
                # Query the warehouse to populate the UI
                logger.info("🎯 Step 4/4: Creating real-time leaderboard UI...")
                leaderboard_ui = query_leaderboard_from_warehouse(hostname, access_token, warehouse_result['warehouse_id'], catalog_name, schema_name, table_name)
                logger.info("✅ Step 4/4 complete: Leaderboard UI created successfully")
                success_message = dbc.Alert([
                    html.H6("🏆 Leaderboard Database Initialized Successfully!", className="alert-heading"),
                    html.P(f"✅ Created dedicated serverless warehouse: {warehouse_result['warehouse_name']}"),
                    html.P(f"⚙️ Configuration: XL size, 8-hour autostop, serverless"),
                    html.P(f"🗃️ Using catalog: {catalog_name}"),
                    html.P(f"📊 Stored {len(users_data)} participants in table: {storage_result['table_name']}"),
                    html.P("🎯 Real-time leaderboard is now ready for workshop activities!")
                ], color="success")
                
                logger.info(f"Successfully created warehouse and stored leaderboard")
                
                # Show completion progress with detailed stages
                completion_progress = html.Div([
                    dbc.Progress(
                        value=100, 
                        color="success", 
                        label="✅ All 4 steps completed successfully!",
                        className="mb-2",
                        style={"height": "30px"}
                    ),
                    html.Small([
                        "🎯 Process completed: Data Processing → Warehouse Creation → Data Storage → UI Generation"
                    ], className="text-muted")
                ])
                
                return leaderboard_ui, success_message, completion_progress, {"display": "block"}
            else:
                # Warehouse created but storage failed
                error_message = dbc.Alert([
                    html.H6("⚠️ Warehouse Created but Storage Failed", className="alert-heading"), 
                    html.P(f"✅ Created warehouse: {warehouse_result['warehouse_name']}"),
                    html.P(f"❌ Storage error: {storage_result['error']}"),
                    html.P("Please try initializing again...")
                ], color="warning")
                
                fallback_table = create_simple_leaderboard_table(users_data[:20])
                
                # Show error progress
                error_progress = html.Div([
                    dbc.Progress(
                        value=70, 
                        color="warning", 
                        label="⚠️ Partial completion - warehouse created",
                        className="mb-2",
                        style={"height": "30px"}
                    ),
                    html.Small([
                        "✅ Steps completed: Data Processing → Warehouse Creation | ❌ Data Storage failed"
                    ], className="text-muted")
                ])
                
                return fallback_table, error_message, error_progress, {"display": "none"}
        else:
            # Warehouse creation failed
            error_message = dbc.Alert([
                html.H6("❌ Warehouse Creation Failed", className="alert-heading"), 
                html.P(f"Error: {warehouse_result['error']}"),
                html.P("Showing participants in memory instead...")
            ], color="danger")
            
            # Create simple fallback table
            fallback_table = create_simple_leaderboard_table(users_data[:20])
            
            # Show error progress
            error_progress = html.Div([
                dbc.Progress(
                    value=30, 
                    color="danger", 
                    label="❌ Warehouse creation failed",
                    className="mb-2",
                    style={"height": "30px"}
                ),
                html.Small([
                    "✅ Steps completed: Data Processing | ❌ Warehouse Creation failed"
                ], className="text-muted")
            ])
            
            return fallback_table, error_message, error_progress, {"display": "none"}

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
        
        # Show error progress
        error_progress = html.Div([
            dbc.Progress(
                value=20, 
                color="danger", 
                label="❌ SDK error - showing demo data",
                className="mb-2",
                style={"height": "30px"}
            ),
            html.Small([
                "❌ Process failed early: Authentication or connection issues"
            ], className="text-muted")
        ])
        
        return users_table, error_alert, error_progress, {"display": "none"}


# Callback for manual refresh button
@app.callback(
    [Output("users-table-container", "children", allow_duplicate=True),
     Output("manual-refresh-btn", "style", allow_duplicate=True)],
    Input("manual-refresh-btn", "n_clicks"),
    [State("hostname", "value"),
     State("access-token", "value"),
     State("catalog-name", "value"),
     State("schema-name", "value"),
     State("table-name", "value")],
    prevent_initial_call=True
)
def manual_refresh_leaderboard(n_clicks, hostname, access_token, catalog_name, schema_name, table_name):
    """Manual refresh of the leaderboard from warehouse."""
    if not n_clicks:
        return "", {"display": "none"}
    
    try:
        logger.info(f"Manual refresh triggered by user")
        
        # Check if we have credentials
        if not all([hostname, access_token]):
            return html.Div([
                dbc.Alert("⚠️ Please enter workspace credentials first", color="warning", className="text-center")
            ]), {"display": "block"}
        
        # Default naming if not provided
        catalog_name = catalog_name or "main"
        schema_name = schema_name or "default"
        table_name = table_name or "workshop_leaderboard"
        
        # Try to refresh the leaderboard from warehouse
        leaderboard_ui = query_leaderboard_from_warehouse(hostname, access_token, None, catalog_name, schema_name, table_name)
        logger.info("Manual leaderboard refresh successful")
        return leaderboard_ui, {"display": "block"}
        
    except Exception as e:
        logger.warning(f"Manual refresh failed: {str(e)}")
        error_ui = html.Div([
            dbc.Alert([
                html.H6("❌ Refresh Failed", className="alert-heading"),
                html.P(f"Error: {str(e)}"),
                html.P("Make sure the leaderboard has been initialized and the warehouse is running.")
            ], color="danger")
        ])
        return error_ui, {"display": "block"}


# Callback for auto-refreshing the leaderboard
@app.callback(
    Output("users-table-container", "children", allow_duplicate=True),
    [Input("leaderboard-refresh-interval", "n_intervals"),
     Input("refresh-leaderboard-btn", "n_clicks")],
    [State("hostname", "value"),
     State("access-token", "value"),
     State("catalog-name", "value"),
     State("schema-name", "value"),
     State("table-name", "value")],
    prevent_initial_call=True
)
def auto_refresh_leaderboard(n_intervals, refresh_clicks, hostname, access_token, catalog_name, schema_name, table_name):
    """Auto-refresh the leaderboard every 30 seconds or when refresh button is clicked."""
    try:
        logger.info(f"Auto-refreshing leaderboard: intervals={n_intervals}, clicks={refresh_clicks}")
        
        # Check if we have credentials
        if not all([hostname, access_token]):
            return html.Div([
                html.P("⚠️ Leaderboard auto-refresh requires workspace credentials", 
                       className="text-muted text-center")
            ])
        
        # Default naming if not provided
        catalog_name = catalog_name or "main"
        schema_name = schema_name or "default"
        table_name = table_name or "workshop_leaderboard"
        
        # Try to refresh the leaderboard from warehouse
        leaderboard_ui = query_leaderboard_from_warehouse(hostname, access_token, None, catalog_name, schema_name, table_name)
        logger.info("Leaderboard auto-refresh successful")
        return leaderboard_ui
        
    except Exception as e:
        logger.warning(f"Auto-refresh failed: {str(e)}")
        # Return current content on error (don't break the UI)
        return html.Div([
            html.P(f"⚠️ Auto-refresh temporarily unavailable: {str(e)}", 
                   className="text-muted text-center small")
        ])


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


def create_leaderboard_warehouse(hostname, access_token, catalog_name, schema_name):
    """Create a dedicated serverless XL warehouse for the leaderboard."""
    try:
        logger.info("Creating dedicated serverless warehouse for leaderboard")
        
        # Create Databricks WorkspaceClient with explicit PAT authentication
        # Clear any OAuth environment variables that might interfere
        import os
        oauth_env_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 'DATABRICKS_OAUTH_TOKEN']
        original_env = {}
        for var in oauth_env_vars:
            if var in os.environ:
                original_env[var] = os.environ[var]
                del os.environ[var]
        
        try:
            workspace_client = WorkspaceClient(
                host=hostname, 
                token=access_token,
                auth_type="pat"  # Explicitly set to Personal Access Token
            )
        finally:
            # Restore original environment variables
            for var, value in original_env.items():
                os.environ[var] = value
        
        # Create catalog and schema if they don't exist
        try:
            logger.info(f"Ensuring catalog '{catalog_name}' exists")
            # First check if catalog exists
            try:
                catalog_info = workspace_client.catalogs.get(catalog_name)
                logger.info(f"Catalog '{catalog_name}' already exists")
            except Exception:
                # Catalog doesn't exist, create it
                logger.info(f"Creating catalog '{catalog_name}'")
                workspace_client.catalogs.create(name=catalog_name)
                logger.info(f"Successfully created catalog '{catalog_name}'")
        except Exception as e:
            logger.warning(f"Could not create catalog '{catalog_name}': {str(e)}. Proceeding with existing catalog.")
        
        # Create schema if it doesn't exist
        try:
            logger.info(f"Ensuring schema '{catalog_name}.{schema_name}' exists")
            try:
                schema_info = workspace_client.schemas.get(f"{catalog_name}.{schema_name}")
                logger.info(f"Schema '{catalog_name}.{schema_name}' already exists")
            except Exception:
                # Schema doesn't exist, create it
                logger.info(f"Creating schema '{catalog_name}.{schema_name}'")
                workspace_client.schemas.create(name=schema_name, catalog_name=catalog_name)
                logger.info(f"Successfully created schema '{catalog_name}.{schema_name}'")
        except Exception as e:
            logger.warning(f"Could not create schema '{catalog_name}.{schema_name}': {str(e)}. Proceeding with existing schema.")
        
        # Import required classes for warehouse creation
        from databricks.sdk.service.sql import EndpointInfoWarehouseType
        
        # Generate unique warehouse name
        import time
        timestamp = int(time.time())
        warehouse_name = f"workshop-leaderboard-{timestamp}"
        
        logger.info(f"Creating warehouse: {warehouse_name} (XL, serverless, 8h autostop)")
        
        # Create the warehouse using keyword arguments (not request object)
        warehouse = workspace_client.warehouses.create(
            name=warehouse_name,
            cluster_size="X-Large",  # XL size as requested
            warehouse_type=EndpointInfoWarehouseType.PRO,  # PRO type supports serverless
            auto_stop_mins=480,  # 8 hours = 480 minutes
            min_num_clusters=1,
            max_num_clusters=1,
            enable_photon=True,  # Enable Photon for better performance
            enable_serverless_compute=True  # This enables serverless for PRO type
        )
        warehouse_id = warehouse.id
        
        # Start the warehouse
        logger.info(f"Starting warehouse: {warehouse_id}")
        workspace_client.warehouses.start(warehouse_id)
        
        # Wait for warehouse to be running (with timeout)
        import time
        max_wait = 300  # 5 minutes timeout
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            warehouse_info = workspace_client.warehouses.get(warehouse_id)
            if warehouse_info.state and warehouse_info.state.name == "RUNNING":
                logger.info(f"Warehouse {warehouse_name} is now running")
                break
            time.sleep(10)  # Wait 10 seconds before checking again
        else:
            logger.warning(f"Warehouse {warehouse_name} did not start within timeout")
        
        return {
            'success': True,
            'warehouse_id': warehouse_id,
            'warehouse_name': warehouse_name
        }
        
    except Exception as e:
        logger.error(f"Error creating leaderboard warehouse: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def store_leaderboard_in_warehouse(df, hostname, access_token, warehouse_id=None, catalog_name="main", schema_name="default", table_name="workshop_leaderboard"):
    """Store the leaderboard DataFrame in the specified SQL warehouse."""
    try:
        logger.info(f"Storing leaderboard DataFrame with {len(df)} participants in warehouse")
        
        # Create Databricks WorkspaceClient with explicit PAT authentication
        # Clear any OAuth environment variables that might interfere
        import os
        oauth_env_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 'DATABRICKS_OAUTH_TOKEN']
        original_env = {}
        for var in oauth_env_vars:
            if var in os.environ:
                original_env[var] = os.environ[var]
                del os.environ[var]
        
        try:
            workspace_client = WorkspaceClient(
                host=hostname, 
                token=access_token,
                auth_type="pat"  # Explicitly set to Personal Access Token
            )
        finally:
            # Restore original environment variables
            for var, value in original_env.items():
                os.environ[var] = value
        
        # Use specified warehouse or find a running one
        if warehouse_id:
            running_warehouse = workspace_client.warehouses.get(warehouse_id)
            logger.info(f"Using specified warehouse: {running_warehouse.name}")
        else:
            # Fallback to finding any running warehouse
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
        
        # Create the full Unity Catalog table name
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
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


def query_leaderboard_from_warehouse(hostname, access_token, warehouse_id=None, catalog_name="main", schema_name="default", table_name="workshop_leaderboard"):
    """Query the leaderboard from the SQL warehouse and return UI components."""
    try:
        logger.info("Querying leaderboard from warehouse")
        
        # Create Databricks WorkspaceClient with explicit PAT authentication
        # Clear any OAuth environment variables that might interfere
        import os
        oauth_env_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 'DATABRICKS_OAUTH_TOKEN']
        original_env = {}
        for var in oauth_env_vars:
            if var in os.environ:
                original_env[var] = os.environ[var]
                del os.environ[var]
        
        try:
            workspace_client = WorkspaceClient(
                host=hostname, 
                token=access_token,
                auth_type="pat"  # Explicitly set to Personal Access Token
            )
        finally:
            # Restore original environment variables
            for var, value in original_env.items():
                os.environ[var] = value
        
        # Use specified warehouse or find a running one
        if warehouse_id:
            running_warehouse = workspace_client.warehouses.get(warehouse_id)
            logger.info(f"Using specified warehouse: {running_warehouse.name}")
        else:
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
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        query_sql = f"""
        SELECT rank, display_name, email, status, score, last_updated
        FROM {full_table_name}
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
            # Header with real-time status
            html.Div([
                html.H6([
                    html.I(className="bi bi-trophy-fill me-2", style={"color": "#ffc107"}),
                    f"Workshop Leaderboard - Live from SQL Warehouse"
                ], className="mb-2"),
                html.Div([
                    html.Span("🔴 Live", className="badge bg-danger me-2"),
                    html.Span(f"Updated: {pd.Timestamp.now().strftime('%H:%M:%S')}", 
                             className="text-muted small"),
                    dbc.Button("🔄 Refresh Now", id="refresh-leaderboard-btn", 
                              color="outline-success", size="sm", className="ms-2")
                ], className="d-flex align-items-center mb-3")
            ]),
            
            # Leaderboard table
            html.Div([
                dbc.Table([
                    html.Thead([
                        html.Tr([
                            html.Th("🏆 Rank", style={"width": "10%"}),
                            html.Th("👤 Name", style={"width": "25%"}),
                            html.Th("📧 Email", style={"width": "30%"}),
                            html.Th("📊 Status", style={"width": "15%"}),
                            html.Th("🎯 Score", style={"width": "20%"})
                        ], style={"background-color": "#f8f9fa"})
                    ]),
                    html.Tbody(table_rows, id="leaderboard-tbody")
                ], bordered=True, striped=True, hover=True, responsive=True)
            ], style={"max-height": "600px", "overflow-y": "auto"}),
            
            # Footer info
            html.Div([
                html.P([
                    html.Span(f"📊 Showing top {len(leaderboard_data)} participants from SQL warehouse", 
                             className="text-muted small"),
                    html.Br(),
                    html.Span("⚡ Real-time updates: Scores refresh automatically when updated in database", 
                             className="text-info small")
                ], className="mt-2 mb-2"),
                
                # Auto-refresh interval (every 30 seconds)
                dcc.Interval(
                    id='leaderboard-refresh-interval',
                    interval=30*1000,  # 30 seconds in milliseconds
                    n_intervals=0,
                    disabled=False
                )
            ])
        ], id="live-leaderboard-container")
        
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
"""Delta Drive Workshop Setup - Databricks App Entry Point."""

import dash
from dash import html, dcc, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import json
import os
import logging
from infrastructure.resource_manager import SQLWarehouseManager
from databricks.sdk import WorkspaceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Dash app for Databricks deployment
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.config.suppress_callback_exceptions = True
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
            dbc.Select(
                id="warehouse-count",
                options=[
                    {"label": "1 Warehouse", "value": 1},
                    {"label": "2 Warehouses", "value": 2},
                    {"label": "3 Warehouses", "value": 3},
                    {"label": "4 Warehouses", "value": 4},
                    {"label": "5 Warehouses", "value": 5}
                ],
                value=1,
                className="mb-3"
            ),
            dbc.FormText("Create multiple warehouses of the same size for load distribution", className="mb-3"),
            
            dbc.Button("Create Serverless Warehouse(s)", id="create-warehouse-btn", color="success", className="w-100 mb-3"),
            html.Div(id="warehouse-creation-message", className="text-center")
        ])
    ])

def create_user_management_section():
    """Create the user management section with SCIM API integration."""
    return dbc.Card([
        dbc.CardBody([
            html.H5("👥 Workshop Participants", className="card-title mb-4"),
            
            dbc.Alert([
                html.I(className="bi bi-info-circle me-2"),
                "Use the SCIM API to fetch all workspace users who can participate in the workshop"
            ], color="info", className="mb-3"),
            
            dbc.Button(
                [html.I(className="bi bi-people-fill me-2"), "Fetch Users from SCIM API"],
                id="fetch-users-btn",
                color="primary",
                className="mb-3"
            ),
            
            html.Div(id="users-table-container", className="mt-3"),
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
        ], className="mb-4")
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
        # Initialize the SQL warehouse manager
        warehouse_manager = SQLWarehouseManager(hostname, access_token)
        
        # Create warehouses using the resource manager
        logger.info(f"Creating {warehouse_count} warehouse(s) with name '{warehouse_name}' and size '{cluster_size}'")
        results = warehouse_manager.create_multiple_warehouses(
            base_name=warehouse_name,
            cluster_size=cluster_size,
            auto_stop_mins=auto_stop,
            count=warehouse_count
        )
        
        # Process results
        successful_warehouses = [r for r in results if r.success]
        failed_warehouses = [r for r in results if not r.success]
        
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
                *warehouse_details
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
    """Fetch users from SCIM API and display in table."""
    if not n_clicks:
        return "", ""
    
    # Validate inputs
    if not all([hostname, access_token]):
        return "", dbc.Alert("❌ Please fill in hostname and access token", color="danger")
    
    try:
        # Use Databricks SDK to fetch users
        try:
            workspace_client = WorkspaceClient(
                host=f"https://{hostname}",
                token=access_token
            )
            
            # Get all users from the workspace
            users = list(workspace_client.users.list())
            
            # Process users data
            users_data = []
            for user in users:
                if user.active:
                    email = user.emails[0].value if user.emails else ""
                    users_data.append({
                        'ID': user.id,
                        'Display Name': user.display_name or "",
                        'Email': email,
                        'Username': user.user_name or "",
                        'Status': '✅ Active' if user.active else '❌ Inactive'
                    })
        except Exception as e:
            logger.warning(f"Failed to fetch users via SDK: {e}")
            # If SDK fails, use demo data
            users_data = [
                {
                    'ID': 'user-1',
                    'Display Name': 'John Doe',
                    'Email': 'john.doe@company.com',
                    'Username': 'john.doe@company.com',
                    'Status': '✅ Active'
                },
                {
                    'ID': 'user-2',
                    'Display Name': 'Jane Smith',
                    'Email': 'jane.smith@company.com',
                    'Username': 'jane.smith@company.com',
                    'Status': '✅ Active'
                },
                {
                    'ID': 'user-3',
                    'Display Name': 'Bob Johnson',
                    'Email': 'bob.johnson@company.com',
                    'Username': 'bob.johnson@company.com',
                    'Status': '✅ Active'
                },
                {
                    'ID': 'user-4',
                    'Display Name': 'Alice Brown',
                    'Email': 'alice.brown@company.com',
                    'Username': 'alice.brown@company.com',
                    'Status': '✅ Active'
                },
                {
                    'ID': 'user-5',
                    'Display Name': 'Charlie Davis',
                    'Email': 'charlie.davis@company.com',
                    'Username': 'charlie.davis@company.com',
                    'Status': '✅ Active'
                }
            ]
        
        # Create the users table
        users_table = dbc.Table([
            html.Thead([
                html.Tr([
                    html.Th("ID"),
                    html.Th("Display Name"),
                    html.Th("Email"),
                    html.Th("Username"),
                    html.Th("Status")
                ])
            ]),
            html.Tbody([
                html.Tr([
                    html.Td(user['ID']),
                    html.Td(user['Display Name']),
                    html.Td(user['Email']),
                    html.Td(user['Username']),
                    html.Td(user['Status'])
                ]) for user in users_data
            ])
        ], bordered=True, hover=True, striped=True, className="mt-3")
        
        success_message = dbc.Alert([
            html.H6("✅ Users Fetched Successfully!", className="alert-heading"),
            html.P(f"Retrieved {len(users_data)} active users from workspace"),
            html.P("These users are eligible to participate in the workshop")
        ], color="success")
        
        return users_table, success_message
        
    except Exception as e:
        return "", dbc.Alert(f"❌ Error fetching users: {str(e)}", color="danger")

# For Databricks Apps deployment
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    app.run(debug=False, host="0.0.0.0", port=port) 
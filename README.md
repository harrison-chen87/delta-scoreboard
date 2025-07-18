# Delta Scoreboard - Workshop Leaderboard App

A Dash-based web application for creating interactive workshop leaderboards with Databricks integration. This app allows participants to answer questions, track their scores, and view real-time leaderboards during workshop sessions.

## Features

- 🏆 **Real-time Leaderboard**: Professional grid layout with progress bars and rankings
- 🔐 **Auto-Authentication**: Automatic user authentication based on Databricks workspace eligibility
- 📊 **Interactive Questions**: Multiple-choice questions with immediate feedback
- 📱 **Responsive Design**: Bootstrap-based UI that works on all devices
- 🔄 **Auto-Refresh**: Automatic leaderboard updates every 30 seconds
- 📈 **Analytics**: Track question completion and scoring statistics with progress tracking
- 🎯 **Demo Mode**: Test the application without Databricks connection
- 🔌 **SCIM API Integration**: Uses Databricks SCIM API for robust user management
- 🌐 **Runtime Credentials**: Enter credentials through web interface (no config files needed)

## Project Structure

```
delta-scoreboard/
├── main.py              # Main application (production with Databricks)
├── main_demo.py         # Demo version (no Databricks required)
├── config.py            # Configuration settings
├── databricks_utils.py  # Databricks connection utilities
├── demo_mode.py         # Mock classes for demo mode
├── pyproject.toml       # uv package configuration
└── README.md           # This file
```

## Quick Start (Demo Mode)

For immediate testing without Databricks:

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and setup**:
   ```bash
   git clone <your-repo-url>
   cd delta-scoreboard
   uv sync
   ```

3. **Run demo**:
   ```bash
   uv run python main_demo.py
   ```

4. **Access the app**: Open http://127.0.0.1:8050 in your browser

5. **Test with demo emails**:
   - `demo@example.com`
   - `test@example.com`
   - `admin@example.com`

## Quick Start (Production Mode)

For production use with Databricks:

1. **Follow steps 1-2 above** to install and setup

2. **Run production app**:
   ```bash
   uv run python main.py
   ```

3. **Access the app**: Open http://127.0.0.1:8050 in your browser

4. **Enter your Databricks credentials** in the web interface

5. **Login with your workspace email** after successful connection

## Production Setup (Databricks)

### Prerequisites

- Databricks workspace with SQL warehouse
- Databricks personal access token
- Python 3.9+
- uv package manager

### Installation

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd delta-scoreboard
   ```

3. **Install dependencies**:
   ```bash
   uv sync
   ```

### Configuration

**No `.env` file needed!** The app now accepts credentials through the web interface.

**Get your Databricks credentials**:
- **Workspace URL**: Found in your Databricks workspace URL (e.g., `company-name.cloud.databricks.com`)
- **HTTP Path**: Go to SQL Warehouses → Select your warehouse → Connection Details → Copy the HTTP Path
- **Access Token**: User Settings → Access Tokens → Generate New Token

### Running the Application

1. **Start the application**:
   ```bash
   uv run python main.py
   ```

2. **Open your browser** to http://127.0.0.1:8050

3. **Enter your credentials** in the web interface:
   - Workspace Hostname: `your-workspace.cloud.databricks.com`
   - Workspace ID: `1234567890123456` (found in your workspace URL)
   - HTTP Path: `/sql/1.0/warehouses/your-warehouse-id`
   - Access Token: Your personal access token

4. **Click "Connect to Databricks"** to establish the connection

5. **Login with your email** after successful connection

### Database Setup

The app will automatically create the required tables:

- `eligible_users`: Stores workspace user information
- `user_responses`: Stores user answers and scores
- `leaderboard`: Calculated leaderboard data

### Production Mode

For production deployment, use gunicorn:
```bash
uv run gunicorn --bind 0.0.0.0:8050 main:app.server
```

## Deployment on Databricks

### Method 1: Databricks Apps (Recommended)

1. **Upload files** to your Databricks workspace
2. **Create a new app** in the Apps section
3. **Set environment variables** in the app configuration
4. **Deploy** the application

### Method 2: Databricks Jobs

1. **Create a new job** with the following configuration:
   ```python
   # Job configuration
   {
       "name": "delta-scoreboard-app",
       "tasks": [
           {
               "task_key": "run-app",
               "python_wheel_task": {
                   "entry_point": "main",
                   "parameters": []
               },
               "libraries": [
                   {"pypi": {"package": "dash>=2.17.0"}},
                   {"pypi": {"package": "dash-bootstrap-components>=1.5.0"}},
                   {"pypi": {"package": "databricks-sql-connector>=3.0.0"}}
               ]
           }
       ]
   }
   ```

### Method 3: Docker Deployment

1. **Create Dockerfile**:
   ```dockerfile
   FROM python:3.9-slim
   
   WORKDIR /app
   COPY . .
   
   RUN pip install uv
   RUN uv sync
   
   EXPOSE 8050
   CMD ["uv", "run", "gunicorn", "--bind", "0.0.0.0:8050", "main:app.server"]
   ```

2. **Build and run**:
   ```bash
   docker build -t delta-scoreboard .
   docker run -p 8050:8050 --env-file .env delta-scoreboard
   ```

## Customization

### Adding Questions

Edit the `WORKSHOP_QUESTIONS` list in `main.py` or `main_demo.py`:

```python
WORKSHOP_QUESTIONS = [
    {
        "id": 1,
        "question": "Your question here?",
        "options": ["Option A", "Option B", "Option C", "Option D"],
        "correct_answer": "Option A"
    },
    # Add more questions...
]
```

### Styling

The app uses Bootstrap components. Customize the theme by changing the Bootstrap theme in the app initialization:

```python
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
```

Available themes: `BOOTSTRAP`, `CERULEAN`, `COSMO`, `CYBORG`, `DARKLY`, `FLATLY`, `JOURNAL`, `LITERA`, `LUMEN`, `LUX`, `MATERIA`, `MINTY`, `MORPH`, `PULSE`, `QUARTZ`, `SANDSTONE`, `SIMPLEX`, `SKETCHY`, `SLATE`, `SOLAR`, `SPACELAB`, `SUPERHERO`, `UNITED`, `VAPOR`, `YETI`, `ZEPHYR`

### Configuration Options

Edit `config.py` to modify:

- **Points per question**: `POINTS_PER_QUESTION`
- **Maximum questions**: `MAX_QUESTIONS`
- **Auto-refresh interval**: Modify `dcc.Interval` in the layout
- **Database table names**: Update table name constants

## API Reference

### Core Classes

#### `DatabricksConnection`
- `connect()`: Establish connection to Databricks
- `execute_query(query, params)`: Execute SQL queries
- `close()`: Close connection

#### `UserManager`
- `get_workspace_users()`: Fetch workspace users
- `is_user_eligible(email)`: Check user eligibility
- `sync_users_to_table()`: Sync users to database

#### `ResponseManager`
- `submit_response(user_id, question_id, answer, correct)`: Submit answer
- `get_user_score(user_id)`: Get user's total score
- `get_leaderboard()`: Get current leaderboard

## Troubleshooting

### Common Issues

1. **Connection Error**: Check Databricks credentials and network access
2. **Permission Denied**: Verify access token has necessary permissions
3. **Table Not Found**: Ensure warehouse has create table permissions
4. **Import Errors**: Run `uv sync` to install dependencies

### Debug Mode

Enable debug mode by setting `DEBUG=True` in your `.env` file or:

```python
app.run_server(debug=True)
```

### Logging

Logs are written to console. For file logging, add:

```python
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
```

## Security Considerations

- Store sensitive credentials in environment variables
- Use HTTPS in production
- Implement rate limiting for API endpoints
- Regular security updates for dependencies
- Validate all user inputs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Support

For issues and questions:
- Check the troubleshooting section
- Review Databricks documentation
- Create an issue in the repository

## Version History

- **v0.2.0**: Major updates with enhanced features
- ✨ Added Workspace ID input field for better connection management
- 🔌 Integrated Databricks SCIM API for robust user management
- 📊 Professional leaderboard grid with progress bars and rankings
- 🌐 Runtime credential input through web interface
- 📱 Enhanced responsive design with better table layouts

- **v0.1.0**: Initial release with basic functionality
- Demo mode for testing
- Databricks integration
- Real-time leaderboard
- Bootstrap UI 
"""Resource manager for SQL warehouse creation and management."""

import logging
import requests
import json
import os
import sys
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Try to import Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, ChannelName, Channel
    HAS_DATABRICKS_SDK = True
except ImportError:
    HAS_DATABRICKS_SDK = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class WarehouseConfig:
    """Configuration for SQL warehouse creation."""
    name: str
    cluster_size: str
    auto_stop_mins: int = 240
    max_num_clusters: int = 1
    warehouse_type: str = "PRO"
    enable_photon: bool = True
    enable_serverless_compute: bool = True

@dataclass
class WarehouseResult:
    """Result of warehouse creation."""
    name: str
    id: str
    http_path: str
    success: bool
    error: Optional[str] = None

class SQLWarehouseManager:
    """Manages SQL warehouse creation and operations."""
    
    def __init__(self, hostname: str, access_token: str):
        """
        Initialize the SQL warehouse manager.
        
        Args:
            hostname: Databricks workspace hostname
            access_token: Databricks access token
        """
        self.hostname = hostname
        self.access_token = access_token
        self.base_url = f"https://{hostname}/api/2.0/sql/warehouses"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Debug logging
        logger.info(f"Initializing SQL Warehouse Manager for {hostname}")
        logger.info(f"Databricks SDK available: {HAS_DATABRICKS_SDK}")
        
        # Check if running in Databricks environment
        self.is_databricks_env = self._is_databricks_environment()
        logger.info(f"Detected Databricks environment: {self.is_databricks_env}")
        
        # Always try to use SDK first if available, regardless of environment
        self.use_sdk = HAS_DATABRICKS_SDK
        self.workspace_client = None
        
        if self.use_sdk:
            try:
                # Try to initialize the SDK
                self.workspace_client = WorkspaceClient(
                    host=f"https://{hostname}",
                    token=access_token
                )
                
                # Test the SDK connection by checking if we can access the client
                try:
                    # Simple test - just check if we can access the warehouses property
                    _ = self.workspace_client.warehouses
                    logger.info(f"✅ Successfully initialized SQL Warehouse Manager with Databricks SDK for {hostname}")
                except Exception as sdk_test_error:
                    logger.warning(f"SDK connection test failed, falling back to HTTP: {sdk_test_error}")
                    self.use_sdk = False
                    self.workspace_client = None
                    
            except Exception as e:
                logger.warning(f"Failed to initialize Databricks SDK, falling back to HTTP: {e}")
                self.use_sdk = False
                self.workspace_client = None
        
        if not self.use_sdk:
            logger.info(f"🔄 Initialized SQL Warehouse Manager with HTTP requests for {hostname}")
        
        logger.info(f"Final configuration - Using SDK: {self.use_sdk}")
        
        # Additional debugging for environment variables
        self._log_environment_info()
    
    def _is_databricks_environment(self) -> bool:
        """Check if running in a Databricks environment."""
        # Check for Databricks environment variables
        databricks_env_vars = [
            'DATABRICKS_RUNTIME_VERSION',
            'DATABRICKS_TOKEN',
            'DB_HOME',
            'DATABRICKS_ROOT_VIRTUALENV_ENV',
            'DATABRICKS_WORKSPACE_ID',
            'DATABRICKS_HOST'
        ]
        
        for var in databricks_env_vars:
            if os.getenv(var):
                logger.info(f"Detected Databricks environment via {var}")
                return True
        
        return False
    
    def _log_environment_info(self):
        """Log environment information for debugging."""
        logger.info("=== Environment Debug Information ===")
        env_vars_to_check = [
            'DATABRICKS_RUNTIME_VERSION',
            'DATABRICKS_TOKEN',
            'DB_HOME',
            'DATABRICKS_ROOT_VIRTUALENV_ENV',
            'DATABRICKS_WORKSPACE_ID',
            'DATABRICKS_HOST',
            'DATABRICKS_SERVER_HOSTNAME',
            'DATABRICKS_HTTP_PATH',
            'DATABRICKS_ACCESS_TOKEN'
        ]
        
        for var in env_vars_to_check:
            value = os.getenv(var)
            if value:
                # Mask sensitive tokens
                if 'TOKEN' in var or 'ACCESS' in var:
                    masked_value = f"{value[:10]}..." if len(value) > 10 else "***"
                    logger.info(f"  {var}: {masked_value}")
                else:
                    logger.info(f"  {var}: {value}")
            else:
                logger.info(f"  {var}: Not set")
        
        logger.info("=== End Environment Debug Information ===")
        
        # Also log the current working directory and other system info
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Python executable: {sys.executable}")
        logger.info(f"User: {os.getenv('USER', 'Unknown')}")
        logger.info(f"Home: {os.getenv('HOME', 'Unknown')}")
    
    def create_warehouse(self, config: WarehouseConfig) -> WarehouseResult:
        """
        Create a single SQL warehouse.
        
        Args:
            config: Warehouse configuration
            
        Returns:
            WarehouseResult with creation details
        """
        if self.use_sdk:
            return self._create_warehouse_with_sdk(config)
        else:
            return self._create_warehouse_with_http(config)
    
    def _create_warehouse_with_sdk(self, config: WarehouseConfig) -> WarehouseResult:
        """Create warehouse using Databricks SDK."""
        try:
            logger.info(f"Creating warehouse '{config.name}' with size '{config.cluster_size}' using Databricks SDK")
            
            # Create channel configuration
            channel = Channel(name=ChannelName.CHANNEL_NAME_CURRENT)
            
            # Make the API call using SDK with keyword arguments
            warehouse_wait = self.workspace_client.warehouses.create(
                name=config.name,
                cluster_size=config.cluster_size,
                auto_stop_mins=config.auto_stop_mins,
                max_num_clusters=config.max_num_clusters,
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                enable_photon=config.enable_photon,
                enable_serverless_compute=config.enable_serverless_compute,
                channel=channel
            )
            
            # Wait for the warehouse to be created
            warehouse_response = warehouse_wait.result()
            
            if warehouse_response.id:
                result = WarehouseResult(
                    name=config.name,
                    id=warehouse_response.id,
                    http_path=f"/sql/1.0/warehouses/{warehouse_response.id}",
                    success=True
                )
                logger.info(f"Successfully created warehouse using SDK: {warehouse_response.id}")
                return result
            else:
                error_msg = "No warehouse ID in SDK response"
                logger.error(f"Error creating warehouse: {error_msg}")
                return WarehouseResult(
                    name=config.name,
                    id="",
                    http_path="",
                    success=False,
                    error=error_msg
                )
                
        except Exception as e:
            error_msg = f"SDK error: {str(e)}"
            logger.error(f"Error creating warehouse with SDK: {error_msg}")
            return WarehouseResult(
                name=config.name,
                id="",
                http_path="",
                success=False,
                error=error_msg
            )
    
    def _create_warehouse_with_http(self, config: WarehouseConfig) -> WarehouseResult:
        """Create warehouse using HTTP requests."""
        try:
            # Prepare the request payload
            payload = {
                "name": config.name,
                "cluster_size": config.cluster_size,
                "auto_stop_mins": config.auto_stop_mins,
                "max_num_clusters": config.max_num_clusters,
                "warehouse_type": config.warehouse_type,
                "enable_photon": config.enable_photon,
                "enable_serverless_compute": config.enable_serverless_compute,
                "channel": {
                    "name": "CHANNEL_NAME_CURRENT"
                }
            }
            
            logger.info(f"Creating warehouse '{config.name}' with size '{config.cluster_size}' using HTTP")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            
            # Make the API request
            response = requests.post(
                self.base_url,
                json=payload,
                headers=self.headers,
                timeout=30
            )
            
            # Log the response for debugging
            logger.info(f"API Response Status: {response.status_code}")
            logger.debug(f"API Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                warehouse_data = response.json()
                warehouse_id = warehouse_data.get("id")
                
                if warehouse_id:
                    result = WarehouseResult(
                        name=config.name,
                        id=warehouse_id,
                        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                        success=True
                    )
                    logger.info(f"Successfully created warehouse: {warehouse_id}")
                    return result
                else:
                    error_msg = "No warehouse ID in response"
                    logger.error(f"Error creating warehouse: {error_msg}")
                    return WarehouseResult(
                        name=config.name,
                        id="",
                        http_path="",
                        success=False,
                        error=error_msg
                    )
            else:
                error_msg = f"API request failed with status {response.status_code}"
                try:
                    error_details = response.json()
                    error_msg += f": {error_details.get('message', 'Unknown error')}"
                    logger.error(f"API Error Details: {json.dumps(error_details, indent=2)}")
                except:
                    error_msg += f": {response.text}"
                    
                logger.error(f"Error creating warehouse: {error_msg}")
                return WarehouseResult(
                    name=config.name,
                    id="",
                    http_path="",
                    success=False,
                    error=error_msg
                )
                
        except requests.exceptions.Timeout:
            error_msg = "Request timeout - API call took too long"
            logger.error(f"Error creating warehouse: {error_msg}")
            return WarehouseResult(
                name=config.name,
                id="",
                http_path="",
                success=False,
                error=error_msg
            )
        except requests.exceptions.ConnectionError:
            error_msg = "Connection error - unable to reach Databricks API"
            logger.error(f"Error creating warehouse: {error_msg}")
            return WarehouseResult(
                name=config.name,
                id="",
                http_path="",
                success=False,
                error=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            logger.error(f"Error creating warehouse: {error_msg}")
            return WarehouseResult(
                name=config.name,
                id="",
                http_path="",
                success=False,
                error=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Error creating warehouse: {error_msg}")
            return WarehouseResult(
                name=config.name,
                id="",
                http_path="",
                success=False,
                error=error_msg
            )
    
    def create_multiple_warehouses(self, base_name: str, cluster_size: str, auto_stop_mins: int, count: int) -> List[WarehouseResult]:
        """
        Create multiple SQL warehouses.
        
        Args:
            base_name: Base name for warehouses
            cluster_size: T-shirt size for warehouses
            auto_stop_mins: Auto-stop time in minutes
            count: Number of warehouses to create
            
        Returns:
            List of WarehouseResult objects
        """
        results = []
        
        for i in range(count):
            if count > 1:
                warehouse_name = f"{base_name}-{i+1}"
            else:
                warehouse_name = base_name
            
            config = WarehouseConfig(
                name=warehouse_name,
                cluster_size=cluster_size,
                auto_stop_mins=auto_stop_mins
            )
            
            result = self.create_warehouse(config)
            results.append(result)
            
            # Add a small delay between requests to avoid rate limiting
            import time
            time.sleep(1)
        
        return results
    
    def list_warehouses(self) -> List[Dict[str, Any]]:
        """
        List all SQL warehouses in the workspace.
        
        Returns:
            List of warehouse information dictionaries
        """
        if self.use_sdk:
            return self._list_warehouses_with_sdk()
        else:
            return self._list_warehouses_with_http()
    
    def _list_warehouses_with_sdk(self) -> List[Dict[str, Any]]:
        """List warehouses using Databricks SDK."""
        try:
            warehouses = list(self.workspace_client.warehouses.list())
            warehouse_dicts = []
            
            for warehouse in warehouses:
                warehouse_dict = {
                    "id": warehouse.id,
                    "name": warehouse.name,
                    "cluster_size": warehouse.cluster_size,
                    "auto_stop_mins": warehouse.auto_stop_mins,
                    "state": warehouse.state,
                    "warehouse_type": warehouse.warehouse_type
                }
                warehouse_dicts.append(warehouse_dict)
            
            logger.info(f"Found {len(warehouse_dicts)} warehouses using SDK")
            return warehouse_dicts
            
        except Exception as e:
            logger.error(f"Error listing warehouses with SDK: {str(e)}")
            return []
    
    def _list_warehouses_with_http(self) -> List[Dict[str, Any]]:
        """List warehouses using HTTP requests."""
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                warehouses = data.get("warehouses", [])
                logger.info(f"Found {len(warehouses)} warehouses")
                return warehouses
            else:
                logger.error(f"Failed to list warehouses: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing warehouses: {str(e)}")
            return []
    
    def delete_warehouse(self, warehouse_id: str) -> bool:
        """
        Delete a SQL warehouse.
        
        Args:
            warehouse_id: ID of the warehouse to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        if self.use_sdk:
            return self._delete_warehouse_with_sdk(warehouse_id)
        else:
            return self._delete_warehouse_with_http(warehouse_id)
    
    def _delete_warehouse_with_sdk(self, warehouse_id: str) -> bool:
        """Delete warehouse using Databricks SDK."""
        try:
            self.workspace_client.warehouses.delete(warehouse_id)
            logger.info(f"Successfully deleted warehouse using SDK: {warehouse_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting warehouse {warehouse_id} with SDK: {str(e)}")
            return False
    
    def _delete_warehouse_with_http(self, warehouse_id: str) -> bool:
        """Delete warehouse using HTTP requests."""
        try:
            response = requests.delete(
                f"{self.base_url}/{warehouse_id}",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully deleted warehouse: {warehouse_id}")
                return True
            else:
                logger.error(f"Failed to delete warehouse {warehouse_id}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting warehouse {warehouse_id}: {str(e)}")
            return False 
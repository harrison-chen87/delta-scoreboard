"""Resource manager for SQL warehouse creation and management using Databricks SDK."""

import logging
import os
import sys
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Import Databricks SDK - required dependency
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, ChannelName, Channel

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
    """Manages SQL warehouse creation and operations using Databricks SDK."""
    
    def __init__(self, hostname: str, access_token: str):
        """
        Initialize the SQL warehouse manager using Databricks SDK.
        
        Args:
            hostname: Databricks workspace hostname
            access_token: Databricks access token
        """
        self.hostname = hostname
        self.access_token = access_token
        
        # Debug logging
        logger.info(f"Initializing SQL Warehouse Manager for {hostname}")
        
        # Initialize the Databricks SDK client
        try:
            # Explicitly use PAT authentication and ignore OAuth environment variables
            # Clean hostname to avoid double https:// prefix
            clean_hostname = hostname.replace("https://", "").replace("http://", "")
            self.workspace_client = WorkspaceClient(
                host=f"https://{clean_hostname}",
                token=access_token,
                auth_type="pat"  # Explicitly set to Personal Access Token authentication
            )
            
            # Test the SDK connection by checking if we can access the client
            _ = self.workspace_client.warehouses
            logger.info(f"✅ Successfully initialized SQL Warehouse Manager with Databricks SDK for {hostname}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Databricks SDK: {e}")
            raise RuntimeError(f"Could not initialize Databricks SDK: {e}")
        
        # Log environment information for debugging
        self._log_environment_info()
    

    
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
        Create a single SQL warehouse using Databricks SDK.
        
        Args:
            config: Warehouse configuration
            
        Returns:
            WarehouseResult with creation details
        """
        return self._create_warehouse_with_sdk(config)
    
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
        List all SQL warehouses in the workspace using Databricks SDK.
        
        Returns:
            List of warehouse information dictionaries
        """
        return self._list_warehouses_with_sdk()
    
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
    

    
    def delete_warehouse(self, warehouse_id: str) -> bool:
        """
        Delete a SQL warehouse using Databricks SDK.
        
        Args:
            warehouse_id: ID of the warehouse to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        return self._delete_warehouse_with_sdk(warehouse_id)
    
    def _delete_warehouse_with_sdk(self, warehouse_id: str) -> bool:
        """Delete warehouse using Databricks SDK."""
        try:
            self.workspace_client.warehouses.delete(warehouse_id)
            logger.info(f"Successfully deleted warehouse using SDK: {warehouse_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting warehouse {warehouse_id} with SDK: {str(e)}")
            return False
    
 
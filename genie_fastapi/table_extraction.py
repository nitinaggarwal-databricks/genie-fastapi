import requests
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Get Databricks workspace URL and access token
workspace_url = os.getenv("WORKSPACE_URL")
access_token = os.getenv("ACCESS_TOKEN")

def get_tables(catalog_name, schema_name):
    """
    Get tables for a given catalog and schema from Databricks SQL
    
    Args:
        catalog_name (str): The name of the catalog
        schema_name (str): The name of the schema
        
    Returns:
        dict: Dictionary with tables information or error
    """
    try:
        # Endpoint for listing tables - using Unity Catalog endpoint
        endpoint = f"{workspace_url}/api/2.1/unity-catalog/tables"
        
        # Headers for authentication
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Parameters for the request
        params = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "max_results": 20,  # optional
            "omit_columns": True,  # optional
            "omit_properties": True  # optional
        }
        
        # Make the request
        response = requests.get(endpoint, headers=headers, params=params)
        
        # Check if request was successful
        if response.status_code == 200:
            tables = response.json().get("tables", [])
            return {
                "success": True,
                "tables": tables
            }
        else:
            return {
                "success": False,
                "error": f"Failed to get tables: {response.text}"
            }
    except Exception as e:
        return {
            "success": False,
            "error": f"Exception occurred: {str(e)}"
        }

def get_table_columns(catalog_name, schema_name, table_name):
    """
    Get columns for a specific table from Databricks SQL
    
    Args:
        catalog_name (str): The name of the catalog
        schema_name (str): The name of the schema
        table_name (str): The name of the table
        
    Returns:
        dict: Dictionary with column information or error
    """
    try:
        # Endpoint for describing table using Unity Catalog
        endpoint = f"{workspace_url}/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}"
        
        # Headers for authentication
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Make the request
        response = requests.get(endpoint, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            # Extract column information from Unity Catalog response
            columns_data = response.json().get("columns", [])
            # Extract column names
            column_names = [col.get("name") for col in columns_data if col.get("name")]
            return {
                "success": True,
                "columns": column_names
            }
        else:
            return {
                "success": False,
                "error": f"Failed to get columns: {response.text}"
            }
    except Exception as e:
        return {
            "success": False,
            "error": f"Exception occurred: {str(e)}"
        }

# Example usage
if __name__ == "__main__":
    # Default values
    default_catalog = "users_trial"
    default_schema = "nitin_aggarwal"
    get_tables(default_catalog, default_schema)

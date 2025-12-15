# tools.py (THE COMPLETE, FINAL, AND FUNCTIONAL VERSION)

from google.cloud import bigquery
from typing import List, Dict, Any, Optional
import os 
import json 

# --- Configuration for BQ Client ---
try:
    # Initialize the BQ client using Application Default Credentials (ADC)
    BQ_CLIENT = bigquery.Client()
    PROJECT_ID = BQ_CLIENT.project
except Exception as e:
    # Fallback definition for PROJECT_ID if client fails to initialize locally
    PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
    BQ_CLIENT = None

# ----------------------------------------------------------------------
# --- CORE BQ TOOL IMPLEMENTATIONS (Custom and Fast) ---------------------
# ----------------------------------------------------------------------

def list_table_ids(dataset_id: str) -> List[str]:
    """Retrieves all table IDs (names) in the specified BigQuery dataset."""
    if not BQ_CLIENT: 
        return [{"error": "BigQuery client not initialized."}]
    try:
        tables = BQ_CLIENT.list_tables(dataset_id)
        return [table.table_id for table in tables]
    except Exception as e:
        return [{"error": str(e), "message": f"Could not list tables in {dataset_id}"}]

def get_table_info(table_id: str, dataset_id: str) -> Dict[str, Any]:
    """Retrieves schema and row count for a BigQuery table."""
    if not BQ_CLIENT: 
        return {"error": "BigQuery client not initialized."}
    full_table_id = f"{dataset_id}.{table_id}"
    try:
        table = BQ_CLIENT.get_table(full_table_id)
        schema_list = []
        for field in table.schema:
            schema_list.append({
                "column_name": field.name, 
                "bq_type": field.field_type, 
                "is_nullable": "YES" if field.is_nullable else "NO"
            })
        return {"row_count": table.num_rows, "schema": schema_list}
    except Exception as e:
        return {"error": str(e), "message": f"Could not get table info for {full_table_id}"}


def get_full_table_profile_sql(dataset_id: str, table_id: str, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Generates and executes a single, optimized SQL query to get ALL aggregates for the Agent.
    
    Args:
        dataset_id: The ID of the dataset.
        table_id: The ID of the table.
        schema: The list of column definitions from get_table_info.

    Returns:
        A list containing a single dictionary of all aggregated statistics.
    """
    if not BQ_CLIENT: 
        return [{"error": "BigQuery client not initialized."}]

    full_table_id = f"`{dataset_id.replace('`', '')}.{table_id.replace('`', '')}`"
    agg_functions = []
    
    for col_info in schema:
        col_name = col_info['column_name']
        col_type = col_info['bq_type'].upper()
        # Quote the column name to handle special characters or reserved words
        col = f"`{col_name.replace('`', '')}`"
        
        # Aggregations: Null Count and Distinct Count (for all types)
        agg_functions.extend([
            f"COUNT(*) - COUNT({col}) AS {col_name}_null_count",
            f"COUNT(DISTINCT {col}) AS {col_name}_distinct_count",
        ])

        # Type-specific Aggregations
        if col_type in ['INTEGER', 'INT64', 'FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC']:
            agg_functions.extend([
                f"MIN({col}) AS {col_name}_min",
                f"MAX({col}) AS {col_name}_max",
                f"AVG(CAST({col} AS FLOAT64)) AS {col_name}_avg"
            ])
        elif col_type == 'STRING':
            agg_functions.extend([
                f"MAX(LENGTH({col})) AS {col_name}_max_len",
                # FIX: Use TO_JSON_STRING directly on APPROX_TOP_COUNT to avoid UNNEST error
                f"TO_JSON_STRING(APPROX_TOP_COUNT({col}, 5)) AS {col_name}_top_values"
            ])
            
    if not agg_functions:
        return [{"error": "Empty schema provided for profiling."}]

    query = f"SELECT {', '.join(agg_functions)} FROM {full_table_id} LIMIT 1"
    
    try:
        results = BQ_CLIENT.query(query).result()
        # Convert results to a list of dictionaries for clean LLM consumption
        return [dict(row) for row in results]
    except Exception as e:
        return [{"error": str(e), "message": f"Could not execute profile query: {e}"}]

def execute_sql(query: str) -> List[Dict[str, Any]]:
    """
    Executes a read-only SQL query on Google BigQuery and returns the result as a list of dictionaries.
    This is included for the Agent's flexibility (execute_sql tool).
    
    Args:
        query: The SQL query string.
        
    Returns:
        A list of dictionaries, where each dictionary is a row.
    """
    if not BQ_CLIENT:
        return [{"error": "BigQuery client not initialized."}]
    try:
        query_job = BQ_CLIENT.query(query)
        results = query_job.result()
        data = [dict(row) for row in results]
        return data
    except Exception as e:
        return [{"error": str(e), "query": query}]
from google.cloud import bigquery
from typing import List, Dict, Any, Optional

# The BigQuery client automatically uses Application Default Credentials (ADC)
# when running in a GCP environment (Cloud Functions, Cloud Run, etc.)
# which is perfect for a hackathon.
try:
    BQ_CLIENT = bigquery.Client()
except Exception as e:
    print(f"Failed to initialize BigQuery client: {e}")
    print("Ensure GOOGLE_CLOUD_PROJECT environment variable is set or ADC is configured.")
    BQ_CLIENT = None


def list_table_ids(dataset_id: str) -> List[str]:
    """
    Retrieves all table IDs (names) in the specified BigQuery dataset.

    Args:
        dataset_id: The full dataset ID (e.g., 'your_project.your_dataset').

    Returns:
        A list of table names (strings).
    """
    if not BQ_CLIENT:
        return [{"error": "BigQuery client not initialized."}]

    print(f"Listing tables in dataset: {dataset_id}")
    try:
        tables = BQ_CLIENT.list_tables(dataset_id)
        return [table.table_id for table in tables]
    except Exception as e:
        return [{"error": str(e), "message": f"Could not list tables in {dataset_id}"}]


def get_table_info(table_id: str, dataset_id: str) -> Dict[str, Any]:
    """
    Retrieves the schema (column name, data type, nullability) and row count.

    Args:
        table_id: The name of the table.
        dataset_id: The full dataset ID (e.g., 'your_project.your_dataset').

    Returns:
        A dictionary containing the schema and row_count.
    """
    if not BQ_CLIENT:
        return {"error": "BigQuery client not initialized."}

    full_table_id = f"{dataset_id}.{table_id}"
    print(f"Getting info for table: {full_table_id}")
    
    try:
        table = BQ_CLIENT.get_table(full_table_id)
        
        schema_list = []
        for field in table.schema:
            schema_list.append({
                "column_name": field.name,
                "bq_type": field.field_type,
                "is_nullable": "YES" if field.is_nullable else "NO"
            })

        return {
            "row_count": table.num_rows,
            "schema": schema_list
        }
    except Exception as e:
        return {"error": str(e), "message": f"Could not get table info for {full_table_id}"}


def execute_sql(query: str) -> List[Dict[str, Any]]:
    """
    Executes a read-only SQL query on Google BigQuery and returns the result as a list of dictionaries.

    Args:
        query: The SQL query string.

    Returns:
        A list of dictionaries, where each dictionary is a row.
    """
    if not BQ_CLIENT:
        return [{"error": "BigQuery client not initialized."}]

    print(f"Executing SQL query:\n{query[:100]}...")
    try:
        query_job = BQ_CLIENT.query(query)
        # Fetch results
        results = query_job.result()
        
        # Convert results to a list of dictionaries for clean LLM consumption
        data = [dict(row) for row in results]
        
        if not data and query_job.total_rows > 0:
             # Should not happen for aggregate queries, but handles edge cases
             return [{"message": "Query executed successfully, but returned no iterable results."}]
        
        return data
    except Exception as e:
        # Return a structured error message so the agent can react
        return [{"error": str(e), "query": query}]
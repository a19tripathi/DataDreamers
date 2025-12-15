import csv
import io
from typing import Any, Dict, List

from google.cloud import bigquery, storage
from google.adk.tools.tool import tool

# Initialize clients to be reused
storage_client = storage.Client()
bigquery_client = bigquery.Client()


@tool
def get_gcs_csv_header(gcs_uri: str) -> List[str]:
    """
    Reads the first line of a CSV file from Google Cloud Storage and returns the header columns.

    Args:
        gcs_uri: The Google Cloud Storage URI of the file (e.g., 'gs://bucket-name/path/to/file.csv').

    Returns:
        A list of strings representing the column headers.
    """
    # Parse the GCS URI
    if not gcs_uri.startswith("gs://"):
        raise ValueError("Invalid GCS URI. Must start with 'gs://'.")
    bucket_name, blob_name = gcs_uri[5:].split("/", 1)

    # Get the blob and read the first line
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the first ~1KB to get the header without reading the whole file
    # This is efficient for large files.
    first_bytes = blob.download_as_bytes(end=1024, raw_download=True)
    first_line = first_bytes.split(b"\n")[0].decode("utf-8")

    # Parse the CSV header
    reader = csv.reader(io.StringIO(first_line))
    return next(reader)


@tool
def load_gcs_csv_to_bigquery(
    gcs_uri: str, dataset_id: str, table_id: str
) -> Dict[str, Any]:
    """
    Loads data from a CSV file in GCS into a BigQuery table.
    The table will be overwritten if it already exists.

    Args:
        gcs_uri: The GCS URI of the CSV file.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID for the staging table.

    Returns:
        A dictionary containing the job result, including output_rows.
    """
    table_ref = f"{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assumes a header row
        autodetect=True,  # Automatically infer schema for the staging table
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite for idempotency
    )

    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    return {
        "status": "completed",
        "output_rows": load_job.output_rows,
        "staging_table": table_ref,
    }


@tool
def run_bigquery_query(query: str) -> List[Dict[str, Any]]:
    """
    Executes a SQL query in BigQuery and returns the results.

    Args:
        query: The SQL query string to execute.

    Returns:
        A list of dictionaries, where each dictionary represents a row.
    """
    query_job = bigquery_client.query(query)
    results = query_job.result()  # Wait for the job to complete
    return [dict(row) for row in results]


@tool
def insert_bigquery_rows(dataset_id: str, table_id: str, rows: List[Dict]) -> Dict[str, Any]:
    """
    Inserts rows into a BigQuery table.

    Args:
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.
        rows: A list of dictionaries representing the rows to insert.

    Returns:
        A dictionary indicating success or failure, including any errors.
    """
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    errors = bigquery_client.insert_rows_json(table_ref, rows)
    if not errors:
        return {"status": "success", "inserted_rows": len(rows)}
    else:
        return {"status": "error", "errors": errors}
from google.cloud import bigquery
import pandas as pd


def add(a: int, b: int) -> int:
  """Adds two numbers together."""
  return a + b


def execute_bigquery_query(query: str) -> str:
  """Executes a SQL query on Google BigQuery and returns the result."""
  try:
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    if df.empty:
      return "Query executed successfully, but returned no results."
    return df.to_string()
  except Exception as e:
    return f"An error occurred: {e}"
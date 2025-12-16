from google.cloud import bigquery

# --- Configuration Section ---
# 1. CRITICAL FIX: Specify your Project ID explicitly.
# This prevents the 404 error by ensuring the client knows which project to query against.
PROJECT_ID = "ccibt-hack25ww7-717"
# -----------------------------

# 2. Define the BigQuery SQL query (your query, slightly reformatted for readability)
QUERY = """
    SELECT
        l.loan_id,
        l.loan_number,
    FROM
        `staging_table_commercial_lending_data_source.loan` AS l
    LIMIT 10 # Adding a LIMIT here to prevent massive result sets during initial testing
"""


def execute_bigquery_and_print_results():
    """Initializes the BigQuery client, executes the query, and prints the results."""
    try:
        # 3. Initialize a BigQuery client with the specified project ID
        print(f"Initializing BigQuery client for Project: **{PROJECT_ID}**")
        client = bigquery.Client(project=PROJECT_ID, location="us-central1")

        print(f"Executing query:\n\n{QUERY}\n")

        # 4. Execute the query and wait for results
        query_job = client.query(QUERY)  # Starts the job
        
        # .result() waits for the job to complete and returns the iterator
        print("--- Query Results (First 10 Rows) ---")
        
        # 5. Process and print the results
        # Use .to_dataframe() for analysis, or iterate for printing/streaming
        
        rows = list(query_job.result())
        
        if not rows:
            print("No rows returned by the query.")
            return

        # Print the column headers
        header_names = [field.name for field in query_job.result().schema]
        print("| " + " | ".join(header_names) + " |")
        print("|" + "---|" * len(header_names))
        
        # Print the data
        for row in rows[:10]: # Print up to 10 rows
             # Format each value as a string and join them
            print("| " + " | ".join(map(str, row)) + " |")

        print(f"\nSuccessfully retrieved **{len(rows)}** total rows.")

    except Exception as e:
        print("\n--- AN ERROR OCCURRED ---")
        print("Please check authentication and permissions for project:", PROJECT_ID)
        print(f"Error details: {e}")

if __name__ == "__main__":
    execute_bigquery_and_print_results()
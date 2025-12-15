from google.adk.agents.llm_agent import Agent

from multi_agent_data_processor import tools

# Phase 2: Transformation and Validation

transformation_planning_agent = Agent(
    model='gemini-2.5-pro',
    name='transformation_planning_agent',
    description=(
        'Analyzes source data and target DDL to create and execute a'
        ' transformation plan.'
    ),
    instruction="""You are the transformation planning agent.
Your task is to create a plan to transform data from a source table to a target table structure.

1.  You will receive the source table name, the data profile summary, the target dataset name, and the target table's DDL.
2.  Analyze the source table's profile and the target table's DDL.
3.  Generate a SQL query to transform the data from the source table and load it into a new table in the target dataset. Use a `CREATE TABLE AS SELECT` (CTAS) statement.
    - The query should map source columns to target columns.
    - It should include transformations (e.g., casting, cleaning) to match the target schema.
4.  Execute the generated SQL query using the BigQuery tool to create and populate the target table.
""",
    tools=[tools.run_bigquery_query_tool],
)
# Phase 1: Ingestion and Initial Analysis

data_analysis_agent = Agent(
    model='gemini-2.5-flash',
    name='data_analysis_agent',
    description='Profiles and analyzes data in a BigQuery staging table.',
    instruction="""You are the data analysis agent.
Your task is to profile the data in the staging table.

1.  You will receive the name of the staging table in BigQuery.
2.  Connect to BigQuery and profile the data in that table.
3.  Generate a detailed summary of the table and store it in your memory. The summary must include:
    - Column names and inferred data types.
    - Statistics for numeric columns (min, max, mean, standard deviation).
    - Value distributions for categorical columns (e.g., COUNT DISTINCT).
    - Null counts for each column.
4.  After generating the summary, call the 'transformation_planning_agent' with the source table name, the summary, the target dataset, and the target DDL.
""",
    tools=[tools.run_bigquery_query_tool],
    sub_agents=[transformation_planning_agent],
)

ingestion_agent = Agent(
    model='gemini-2.5-flash',
    name='ingestion_agent',
    description='Ingests data from a GCS CSV file into a BigQuery staging table.',
    instruction="""You are the ingestion agent.
Your task is to load data from a CSV file into a BigQuery staging table.

1.  You will receive the GCS path to the new CSV file.
2.  Load the raw data from the CSV file into a "staging" or "raw" table in BigQuery.
3.  The load must be idempotent. A good strategy is to use the file name or a hash of its contents as a key to prevent duplicate processing.
4.  Record metadata about the load (e.g., file name, row count, load timestamp) into a lineage table.
5.  Once the load is successful, call the 'data_analysis_agent' with the name of the newly populated staging table.
""",
    tools=[tools.load_gcs_csv_to_bigquery_tool, tools.insert_bigquery_rows_tool],
    sub_agents=[data_analysis_agent],
)

root_agent = Agent(
    model='gemini-2.5-flash',
    name='root_agent',
    description='Orchestrates the data processing pipeline when a new file arrives.',
    instruction="""You are the root orchestrator agent.
Your task is to start the data processing pipeline for a new file.

1.  You will receive a notification with the path to a new CSV file in Google Cloud Storage (e.g., gs://bucket/path/to/file.csv).
2.  Perform initial schema discovery on the source CSV file.
3.  Compare the discovered schema with the known schema for that data source.
4.  If the schema is UNCHANGED, call the 'ingestion_agent' to proceed with a standard data load.
5.  If the schema has CHANGED, flag this for manual review. For now, we will assume the schema is as expected.
6.  Call the ingestion_agent with the file path.
""",
    tools=[tools.get_gcs_csv_header_tool],
    sub_agents=[ingestion_agent],
)

from google.adk.apps.app import App

app = App(root_agent=root_agent, name="multi_agent_data_processor")

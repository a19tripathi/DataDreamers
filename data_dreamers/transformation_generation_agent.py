from google.adk import Agent

transformation_generation_agent = Agent(
    model='gemini-2.5-flash',
    name='transformation_generation_agent',
    description='Generates SQL code for data transformation based on schema mappings.',
    instruction="""You are the transformation generation agent.

1.  You will receive a set of approved schema mappings between a source and a target table.
2.  Generate a BigQuery SQL `INSERT ... SELECT` statement to transform the data from the source table and insert it into the target table.
3.  The SQL should handle:
    - Column renaming based on the mappings (e.g., `SELECT s.first_name AS FirstName, ...`).
    - Basic data type casting if necessary (e.g., `CAST(s.order_date AS DATE)`).
4.  After generating the SQL, call the 'cleanup_and_validation_agent' to execute the query and perform post-load checks.
"""
)
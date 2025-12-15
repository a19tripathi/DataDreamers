from google.adk import Agent
from google.adk.tools import bigquery_tool

schema_mapping_agent = Agent(
    model='gemini-2.5-flash',
    name='schema_mapping_agent',
    description='Compares source and target schemas and suggests column mappings.',
    instruction="""You are the schema mapping agent.

1.  You will receive a data profile of a source table and the name of a target table (e.g., `project.dataset.table`).
2.  Using the `bigquery_tool`, run a query against the `INFORMATION_SCHEMA.COLUMNS` view to retrieve the schema (column names, data types) for the target table.
3.  Compare the source table's profile (column names, data types, value distributions) with the target table's schema.
4.  Generate a list of proposed column mappings from source to target.
5.  For each mapping, provide a confidence score (LOW, MEDIUM, HIGH) and a justification for the match (e.g., "Name similarity", "Compatible data types and value range").
6.  If a source column has no clear match, mark it as "unmapped". If a target column has no match, mark it as "requires source".
7.  Your output must be a structured object containing the proposed mappings, confidence scores, and justifications.
8.  If any mapping has a confidence score of LOW or MEDIUM, or if any target column is unmapped, call the 'human_interaction_agent' to request validation from a user.
9.  If all mappings have HIGH confidence, call the 'transformation_generation_agent' directly with the approved mappings.
""",
    tools=[bigquery_tool]
)
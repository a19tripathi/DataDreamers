from google.adk.agents import Agent, ParallelAgent, SequentialAgent
from google.adk.tools.bigquery import BigQueryCredentialsConfig
from google.adk.tools.bigquery import BigQueryToolset
import google.auth
from google.adk.tools import exit_loop 
from google.adk.tools.tool_context import ToolContext
import logging
from .pipeline_agent import pipeline_agent

def append_to_state(
    tool_context: ToolContext, field: str, response: str
) -> dict[str, str]:
    """Append new output to an existing state key.

    Args:
        field (str): a field name to append to
        response (str): a string to append to the field

    Returns:
        dict[str, str]: {"status": "success"}
    """
    existing_state = tool_context.state.get(field, [])
    tool_context.state[field] = existing_state + [response]
    logging.info(f"[Added to {field}] {response}")
    return {"status": "success"}


_, project_id = google.auth.default()

bigquery_toolset = BigQueryToolset(
    tool_filter=[
        'list_dataset_ids',
        'get_dataset_info',
        'list_table_ids',
        'get_table_info',
        'execute_sql',
    ]
)


smart_query_generating_agent = Agent(
    model='gemini-2.5-flash',
    name='smart_query_generating_agent',
    description="Generates a straightforward SQL mapping query.",
    instruction="""You are a SQL expert. Given a source table and a target schema, write a BigQuery `INSERT ... SELECT` query. Focus on direct, simple column mappings and standard type casting (e.g., `CAST(col AS STRING)`).""",
    tools=[bigquery_toolset]
)

data_discovery_agent = Agent(
    # This is the root agent that orchestrates the data integration workflow.
    # It will delegate tasks to other specialized agents.
    model='gemini-3-pro-preview',
    name='data_discovery_agent',
    description='Finds a source table in BigQuery that matches a target table DDL and generates a query.',
    instruction=f"""You are the data discovery agent. Your goal is to find the best source table to populate a target table.
    dataset_id = staging_table_world_bank_data_source
1.  Your first step is to understand the user's goal. This will be a DDL statement for a target table or a natural language description of the target schema.
2.  Next, you must ask the user to provide the BigQuery `dataset_id` where you should search for the source table. Do not proceed until you have this information. The project is `{project_id}`.
3.  Once you have the user's goal and the `dataset_id`, analyze the goal to understand the target schema (column names, data types, and semantic meaning).
4.  Using the `bigquery_tool`, explore the tables within the provided `dataset_id`. Use `list_table_ids` to discover tables and `get_table_info` to inspect their schemas.
5.  Based on your analysis, identify the tables that matches the target schema. Consider factors like table names similarity, column names matches, and data type compatibility.
6.  Present your findings to the user in a clear, human-friendly format. Your response must include:
    - The full ID of the source tables you have selected (e.g., `project.dataset.table`).
    - A confidence score for your selection (HIGH, MEDIUM, or LOW). 
    - A detailed, step-by-step reasoning for your choice. Explain how you compared the schemas and why you believe this is the best match. For example: "The source table 'customers' was chosen because its name is similar to the target 'Clients'. Furthermore, it contains columns 'first_name' and 'email' which directly correspond to the target columns 'FirstName' and 'EmailAddress'.
""",
    tools=[bigquery_toolset]
)

critique_agent = Agent(
    model='gemini-2.5-flash',
    name='critique_agent',
    description='Executes a sample of a SQL query, validates the output, and provides feedback.',
    instruction="""You are the SQL Critique Agent, the quality gate for our data transformation pipeline. Your job is to test and validate multiple SQL queries.
1. You will receive the output from the `smart_query_generating_agent`, which contains three different SQL queries.
2. For each query, you must perform the following validation steps:
    a. **Crucial Step:** Add a `LIMIT 10` clause to the query to ensure it runs quickly and cheaply for testing.
    b. Execute the modified query using the `execute_sql` tool.
    c. **Analyze the result:**
        - If the query fails, note the error.
        - If the query succeeds, briefly describe the structure of the output.
3. After testing all queries, present a summary report to the user. The report should include each of the original queries, whether it executed successfully, and any errors or a brief description of the sample output.
4. Conclude by recommending which query seems best and why (e.g., "The defensive query from `query_variant_agent_2` is recommended as it handles potential nulls and casting errors gracefully.").""",
    tools=[bigquery_toolset , exit_loop, append_to_state],
)

flow_agent = SequentialAgent(
    name = "flow_agent",
    sub_agents= [data_discovery_agent, smart_query_generating_agent,critique_agent,pipeline_agent]
)


root_agent = Agent(
    model='claude-opus-4-5@20251101',
    name='root_agent',
    description='A workflow agent that orchestrates schema discovery and query generation.',
    instruction="""You are a master workflow orchestrator. Your goal is to help a user find a source table, generate mapping queries, and validate them.
1.  Your first step is to delegate to the `data_discovery_agent`. This agent will interact with the user to get the target schema and the dataset to search in, and will then find a suitable source table.
2.  The `data_discovery_agent` may ask the user questions. Your job is to facilitate this conversation until it provides its final analysis containing the source and target table information.
3.  Once you receive the final analysis from the `data_discovery_agent`, it is **mandatory** that you immediately delegate to the `smart_query_generating_agent`. You **must** pass the entire analysis as input to this agent. Do not ask the user for confirmation; proceed directly to this step.
4.  The `smart_query_generating_agent` will return three SQL query variants.
5.  Next, delegate to the `critique_agent`. Pass the complete output from the `smart_query_generating_agent` (which includes all three queries) to the `critique_agent` for validation and final recommendations.
6.  Present the final report from the `critique_agent` to the user.
7.  After presenting the report, ask the user if they want to proceed with creating the ETL pipeline using the recommended query.
8.  If the user confirms, delegate to the `pipeline_agent` to create the job. You will need to extract the recommended SQL query and the target table from the conversation history and pass them to the `create_etl_job` tool.
9.  replace project,dataset(project.dataset) values in target table with actual values in the query while creating etl job
""",
    sub_agents=[flow_agent],
)

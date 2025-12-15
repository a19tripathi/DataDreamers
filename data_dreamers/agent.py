from google.adk.agents import Agent, ParallelAgent, SequentialAgent, LoopAgent
from google.adk.tools.bigquery import BigQueryCredentialsConfig
from google.adk.tools.bigquery import BigQueryToolset
import google.auth
from google.adk.tools.tool_context import ToolContext
from google.adk.tools import FunctionTool, exit_loop
import logging
from .pipeline_agent import pipeline_agent
from google.genai import types

_, project_id = google.auth.default()

bigquery_toolset = BigQueryToolset()

# tool_filter=[
#     'list_dataset_ids',
#     'get_dataset_info',
#     'list_table_ids',
#     'get_table_info',
#     'execute_sql',
# ]

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


data_discovery_agent = Agent(
    # This is the root agent that orchestrates the data integration workflow.
    # It will delegate tasks to other specialized agents.
    model='gemini-2.5-pro',
    name='data_discovery_agent',
    description='Finds a source table in BigQuery that matches a target table DDL and generates a query.',
    instruction=f"""You are the data discovery agent. Your goal is to find the best source table to populate a target table.
    dataset_id = <can be extracted from ddl provided by user>
1.  **Understand Goal**: First, understand the user's goal from the provided DDL statement for a target table.
2.  **Analyze Target**: Analyze the DDL to understand the target schema (column names, data types, and semantic meaning).
3.  **Discover Sources**: Use the `list_table_ids` tool from the `bigquery_tool` to discover all tables within the `dataset_id` extracted from the DDL.
4.  **Gather Schemas**: For each table discovered in the previous step, you MUST call the `get_table_info` tool to inspect its schema. Wait for ALL `get_table_info` tool calls to complete before proceeding to the next step.
5.  **Analyze and Select**: After you have gathered the schemas for all source tables, analyze them to identify the best match for the target schema. Consider factors like table name similarity, column name matches, and data type compatibility.
6.  **Present Findings**: Present your findings to the user in a clear, human-friendly format. Your response must include:
    - The full ID of the source tables you have selected (e.g., `project.dataset.table`).
    - A confidence score for your selection (HIGH, MEDIUM, or LOW). 
    - A detailed, step-by-step reasoning for your choice. Explain how you compared the schemas and why you believe this is the best match. For example: "The source table 'customers' was chosen because its name is similar to the target 'Clients'. Furthermore, it contains columns 'first_name' and 'email' which directly correspond to the target columns 'FirstName' and 'EmailAddress'.
""",
    tools=[bigquery_toolset]
)

query_generating_loop_agent = Agent(
    model='gemini-2.5-flash',
    name='sql_generator',
    description="Generates multiple versions of a SQL SELECT query to map a source table to a target schema.",
    instruction="""You are a SQL expert. Given a source table, a target schema, and feedback from a previous attempt, write two different BigQuery `SELECT` queries to transform the source data to match the target schema. If you receive no feedback, it means the previous queries were good, and you should call `exit_loop` with the best query from the previous turn as the `final_answer`.

1.  **Query 1 (Simple Mapping):** This query should focus on direct, simple column mappings and standard type casting (e.g., `CAST(col AS STRING)`).
2.  **Query 2 (Efficient/Alternative Mapping):** This query should explore more efficient or alternative ways to achieve the transformation. This could involve using different functions, join strategies (if applicable), or data manipulation techniques that might be more performant or robust.

For each query, provide only the SQL code.

If you received feedback, make sure to incorporate it into your new queries.
If you did NOT receive any feedback, it implies the critique agent was satisfied and no human feedback was given for revision. In this case, you must call the `exit_loop` tool. The `final_answer` for `exit_loop` should be the best query you generated in the previous turn.""",
    tools=[bigquery_toolset, exit_loop]
)

query_critique__loop_agent = Agent(
    model='gemini-2.5-pro',
    name='sql_critique',
    description='Executes a sample of a SQL query, validates the output, and provides feedback.',
    instruction="""You are the SQL Critique Agent, acting as a Lead Data Engineer. Your role is to be the quality gate for our data transformation pipeline. You must perform a thorough peer review of SQL queries, test them, and then seek human approval.

1.  You will receive one or more SQL queries.
2.  For each query, perform the following validation and analysis:
    a.  **Query Type Check:** First, ensure the query is a `SELECT` statement. If it is an `INSERT`, `UPDATE`, `DELETE`, or any other DML/DDL, you must reject it and state that you can only validate `SELECT` queries.
    b.  **Execution and Validation:** If it is a `SELECT` query, you must call the `execute_sql` tool exactly once for that query. This single call will perform a dry run, check syntax, and execute a sample of the query.
    c.  **Code Quality & Best Practices Review:**
        - **Clarity and Readability:** Is the query well-formatted and easy to understand? Are aliases used effectively?
        - **Performance:** Does the query avoid common performance pitfalls? Specifically check for `SELECT *`, inefficient `JOIN` conditions, or `WHERE` clauses on calculated fields.
        - **Correctness:** Based on the sample output, does the logic appear correct for the transformation goal?
    d.  **Analysis:** Based on the tool's output and your code review, analyze the query.
        - If the query fails, note the error.
        - If it succeeds, briefly describe the structure of the output.
    e.  **Scoring:**
        - **Confidence Score (0-100):** How confident are you that this query meets the goal? (100 = perfect).
        - **Risk Score (0-100):** What is the risk of running this in production? Consider cost, performance, and errors. (0 = no risk).

3.  **Summary Report:** Present a summary report of your findings for all queries. For each query, include the original SQL, execution results, confidence and risk scores, and your detailed reasoning. If you have suggestions for improvement (e.g., "Consider replacing `SELECT *` with explicit column names for better performance and maintainability"), include them in your reasoning.""",
    tools=[bigquery_toolset],
    generate_content_config=types.GenerateContentConfig(temperature=0.1),
)

# STEP 2: Refinement Loop Agent
query_refinement_loop = LoopAgent(
    name="query_refinement_loop",
    # Agent order is crucial: Critique first, then Refine/Exit
    sub_agents=[
        query_generating_loop_agent,
        query_critique__loop_agent,
    ],
    max_iterations=5 # Limit loops
)


root_agent = SequentialAgent(
    name="data_pipeline_flow",
    description="A sequential workflow for discovering data, generating and refining a SQL query, and running the final ETL job.",
    sub_agents=[data_discovery_agent, query_refinement_loop, pipeline_agent],
)

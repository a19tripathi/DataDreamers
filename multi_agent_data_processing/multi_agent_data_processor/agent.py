import os
import re
import traceback
from dotenv import load_dotenv
import logging
from google.cloud import bigquery

from google.adk import Agent
from google.adk.agents import SequentialAgent, LoopAgent 
from google.adk.tools.tool_context import ToolContext
from google.adk.tools import exit_loop 
from .callback_logging import log_query_to_model, log_model_response

# Import BigQuery tools for discovery and validation only
from google.adk.tools.bigquery import BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --- CONFIGURATION ---
load_dotenv()
DATASET_ID = os.getenv("DATA_SOURCE") 
DATA_TARGET = os.getenv("DATA_TARGET")

if not DATASET_ID:
    print("WARNING: DATA_SOURCE environment variable not set. Using placeholder.")
    DATASET_ID = "mock_project.mock_dataset" 

# ==============================================================================
# CLASS: ETL Tools with State Management
# ==============================================================================
class ETLTools:
    def __init__(self):
        """
        Initializes the BigQuery client and the internal memory for Job IDs.
        """
        self.project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not self.project_id:
            print("WARNING: GOOGLE_CLOUD_PROJECT not set. Tools may fail.")
            
        self.client = bigquery.Client(project=self.project_id)
        
        # MEMORY: Maps 'target_table' -> { 'job_id': ..., 'location': ... }
        self.job_memory = {}

    def create_etl_job(self, tool_context: ToolContext, target_table: str, sql_query: str) -> dict[str, str]:
        """
        Triggers a BigQuery ETL job for a specific target table using the provided SQL.
        
        Args:
            target_table (str): The full destination table ID (e.g., project.dataset.table).
            sql_query (str): The SELECT query to execute (without CREATE TABLE).
            
        Returns:
            dict: Confirmation message with the Job ID.
        """
        try:
            print(f"--> [Tool] Submitting job for: {target_table}")
            
            job_config = bigquery.QueryJobConfig(
                destination=DATA_TARGET + "." + target_table,
                write_disposition="WRITE_TRUNCATE"
            )
            
            query_job = self.client.query(sql_query, job_config=job_config)
            
            # --- SAVE TO MEMORY ---
            self.job_memory[target_table] = {
                "job_id": query_job.job_id,
                "location": query_job.location
            }
            
            # Also save to agent state for persistence
            tool_context.state['job_id'] = query_job.job_id
            tool_context.state['job_location'] = query_job.location
            
            logging.info(f"[Job Created] {query_job.job_id} for {target_table}")
            
            return {
                "status": "success",
                "message": f"Job successfully submitted. Job ID: {query_job.job_id}. I have remembered this ID for status checks."
            }
        except Exception as e:
            traceback.print_exc()
            return {
                "status": "error",
                "message": f"Failed to create job: {str(e)}"
            }

    def check_etl_status(self, tool_context: ToolContext, target_table: str) -> dict[str, str]:
        """
        Checks the status of the pipeline for a specific table. 
        Uses internal memory to find the Job ID, so the user does not need to provide it.
        
        Args:
            target_table (str): The table name to check.
            
        Returns:
            dict: The current status (RUNNING, SUCCESS, FAILED).
        """
        print(f"--> [Tool] Checking status for: {target_table}")
        
        # 1. Look up memory first, then state
        if target_table in self.job_memory:
            job_info = self.job_memory[target_table]
        elif 'job_id' in tool_context.state and 'job_location' in tool_context.state:
            job_info = {
                'job_id': tool_context.state['job_id'],
                'location': tool_context.state['job_location']
            }
        else:
            return {
                "status": "error",
                "message": f"I don't have a record of a job for '{target_table}'. Please ask me to create the job first."
            }
            
        # 2. Call API
        try:
            job = self.client.get_job(job_info['job_id'], location=job_info['location'])
            
            if job.state == 'DONE':
                if job.error_result:
                    return {
                        "status": "failed",
                        "message": f"FAILED: {job.error_result['message']}"
                    }
                else:
                    return {
                        "status": "success",
                        "message": "SUCCESS: The data load is complete."
                    }
            else:
                return {
                    "status": "running",
                    "message": f"RUNNING: The job is currently in state {job.state}."
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error checking API: {str(e)}"
            }

# --- INSTANTIATE ETL TOOLS ---
etl_tools_instance = ETLTools()

# --- BIGQUERY TOOLSET SETUP (for discovery and validation only) ---
tool_config = BigQueryToolConfig(
    write_mode=WriteMode.ALLOWED
)
bigquery_toolset = BigQueryToolset(
    bigquery_tool_config=tool_config,
    tool_filter=[
        'list_table_ids',
        'get_table_info',
        'execute_sql',  # For validation queries only
    ]
)

# --- CUSTOM TOOLS ---
def append_to_state(
    tool_context: ToolContext, field: str, response: str
) -> dict[str, str]:
    """Append new output to an existing state key. Used for tracking status."""
    existing_state = tool_context.state.get(field, [])
    if not isinstance(existing_state, list):
        existing_state = [existing_state]
        
    tool_context.state[field] = existing_state + [response]
    logging.info(f"[Added to {field}] {response}")
    return {"status": "success"}

def set_initial_target_table_info(
    tool_context: ToolContext, full_description: str
) -> dict[str, str]:
    """
    Parses a CREATE TABLE DDL string to extract the target table ID 
    and save the full DDL as the target schema.
    """
    
    # Regex to find 'CREATE [OR REPLACE] TABLE `project.dataset.table_name`'
    match = re.search(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+TABLE\s+`?([\w.-]+)`?\s*(?:\(|\s+AS\s+)", 
        full_description, 
        re.IGNORECASE | re.DOTALL
    )
    
    full_table_id = None
    table_name = None
    
    if match:
        full_table_id = match.group(1).strip()
        table_name = full_table_id.split('.')[-1]
    else:
        logging.warning("Regex for table ID failed. Using fallback.")
        # Fallback
        first_token_match = re.search(r"[\w.-]+", full_description.split()[0], re.IGNORECASE)
        if first_token_match:
            full_table_id = first_token_match.group(0)
            table_name = full_table_id.split('.')[-1]
        else:
            table_name = "unknown_table"
            full_table_id = f"{DATASET_ID}.{table_name}"

    tool_context.state['target_table_id'] = full_table_id
    tool_context.state['target_table_name'] = table_name
    tool_context.state['target_schema'] = full_description.strip()
    
    logging.info(f"[Target Table ID Set] {full_table_id}")
    logging.info(f"[Target Schema Set] DDL provided.")
    
    return {
        "status": "success",
        "message": f"Table ID '{full_table_id}' and schema saved."
    }

# --- AGENT DEFINITIONS ---

# 1. TABLE DISCOVERY AGENT
table_discovery_agent = Agent(
    model='gemini-2.5-flash',
    name='table_discovery_agent',
    description='Discovers the available tables in the source BigQuery dataset.',
    instruction=f"""
    You are the Table Discovery Agent. 
    1. Use the `list_table_ids` tool to get all tables from the dataset '{DATASET_ID}'. 
    2. Save the list of table names to the 'source_tables' state key using the `append_to_state` tool. 
    3. Inform the user about the discovered tables.
    """,
    tools=[bigquery_toolset, append_to_state],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# 2. TRANSFORMATION PLANNING AGENT
transformation_planning_agent = Agent(
    model='gemini-2.5-flash',
    name='transformation_planning_agent',
    description='Creates a high-level transformation plan.',
    instruction="""You are an expert ETL architect. Your task is to create or revise a high-level transformation plan.
    
    The user wants to create a target table with the following schema: "{target_schema}"
    The available source tables are: {source_tables}
    
    **CRITICAL CHECK:** Look for existing feedback in the state key 'plan_feedback'.
    - If 'plan_feedback' exists, you MUST revise the previous plan based on the feedback. Clear the 'plan_feedback' state after using it.
    - If no feedback exists, you are creating the first version of the plan.

    **Your steps are:**
    1. Formulate a detailed conceptual plan. If revising, explicitly mention what you changed.
    2. Save your new or revised plan to the 'transformation_plan' state key using `append_to_state`.
    3. Your output should only be the confirmation that the plan has been saved. The `plan_validation_agent` agent will validate it.
    """,
    tools=[bigquery_toolset, append_to_state],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# 3. PLAN VALIDATION AGENT (INTERNAL)
plan_validation_agent = Agent(
    model='gemini-2.5-flash',
    name='plan_validation_agent',
    description='Performs an internal-only validation of the transformation plan.',
    instruction="""You are a meticulous ETL Plan Validator. Your job is to perform an automated review of the plan created by the planning agent. You do NOT interact with the user.

**YOUR STEPS:**
1.  Review the latest plan in the 'transformation_plan' state.
2.  Does the plan seem logical, complete, and likely to succeed? Does it correctly map source columns to the target schema?
3.  **If the plan is NOT satisfactory:** 
    - Generate constructive feedback on what needs to be fixed.
    - Save this feedback to the 'plan_feedback' state key.
    - Set the 'plan_approved' state key to "revise".
    - Announce that you are requesting a revision based on your findings.
4.  **If the plan IS satisfactory:** 
    - Set the 'plan_approved' state key to `True`.
    - Announce that the internal plan validation has passed.
    - Exit the loop using the `exit_loop` tool so the user can be prompted for final confirmation.
""",
    tools=[append_to_state, exit_loop],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# 4. PLAN CONFIRMATION AGENT (USER-FACING)
plan_confirmation_agent = Agent(
    model='gemini-2.5-flash',
    name='plan_confirmation_agent',
    description='Presents the validated plan to the user for final approval.',
    instruction="""Present the latest 'transformation_plan' from the state to the user. Include a confidence score from 0 to 1. Ask the user for their final approval ("Does this plan look correct? Please reply with 'yes' to approve, or provide feedback for changes.").""",
)

# 5. SQL GENERATION AGENT
sql_generation_agent = Agent(
    model='gemini-2.5-flash',
    name='sql_generation_agent',
    description='Generates a BigQuery SQL query from the transformation plan.',
    instruction=f"""
    You are a SQL expert. Your task is to write a single, executable BigQuery SELECT query.

    **CONTEXT:** 
    - Transformation Plan: "{{transformation_plan}}"
    - Source Tables: {{source_tables}}
    - Target Schema: "{{target_schema}}"
    
    **CRITICAL:** Look for 'sql_feedback' in state. If it exists, revise your previous query.
    
    **YOUR STEPS:**
    1. Write ONLY a SELECT query (NOT `CREATE TABLE`). The SELECT statement must produce data that matches the target schema.
    2. Fully qualify all source table names using the provided dataset ID.
    3. **Your output MUST be a single tool call** to the `append_to_state` tool to save the generated SELECT query into the 'sql_query' state key. Do not add any other text.
    
    **IMPORTANT:** Generate ONLY the SELECT statement. The CREATE TABLE will be handled separately during execution.
    """,
    tools=[append_to_state],
    before_model_callback=log_query_to_model,
)

# 6. SQL VALIDATION AGENT
sql_validation_agent = Agent(
    model='gemini-2.5-flash',
    name='sql_validation_agent',
    description='Validates the generated SQL query by executing a sample.',
    instruction="""You are a SQL Validation Agent. Your job is to perform an automated test of the generated SQL. You do NOT interact with the user.

**YOUR STEPS:**
    1. Get the sql_query from state.
    2. If it contains a semicolon at the end, remove it.
    3. Add " LIMIT 10" to the end.
    4. **You MUST call the `execute_sql` tool** from the `bigquery_toolset` with this modified query to get sample data.
    5. **If the `execute_sql` tool call fails:** 
        - Use `append_to_state` to save the error to 'sql_feedback'.
        - Set 'sample_approved' to "refine".
        - Announce that you are requesting a revision due to a SQL error.
    6. **If the `execute_sql` tool call succeeds:** 
        - Save the sample data to the 'sql_sample_data' state key.
        - Set 'sample_approved' to `True`.
        - Announce that the SQL validation has passed.
        - Exit the loop using the `exit_loop` tool so the user can be prompted for final confirmation.
""",
    tools=[bigquery_toolset, append_to_state, exit_loop], # bigquery_toolset is essential here
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# 7. SQL CONFIRMATION AGENT (USER-FACING)
sql_confirmation_agent = Agent(
    model='gemini-2.5-flash',
    name='sql_confirmation_agent',
    description='Presents the SQL sample data to the user for final approval.',
    instruction="""Present the sample data from the 'sql_sample_data' state to the user. Ask "Does this sample data look correct? Please reply with 'yes' to approve, or provide feedback for changes." """,
)

# 8. FINAL EXECUTION AGENT
final_execution_agent = Agent(
    model='gemini-2.5-flash',
    name='final_execution_agent',
    description='Executes the final ETL job in BigQuery.',
    instruction="""
    You are the Final Execution Agent.
    
    **CONTEXT:**
    - SQL Query (SELECT only): {sql_query}
    - Target Table: {target_table_id}
    
    **YOUR STEPS:**
    1. Get the SELECT query from 'sql_query' state.
    2. Get the full target table ID from 'target_table_id' state.
    3. Call the `create_etl_job` tool with target_table and sql_query parameters.
    4. The tool will submit an async job and return a job ID.
    5. Announce to the user that the ETL pipeline has been initiated and is running in the background.
    6. Tell them they can check status anytime by asking "what's the status?"
    7. **IMPORTANT:** nce done, return the control to `root_agent`.
    """,
    tools=[etl_tools_instance.create_etl_job, append_to_state],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# 9. JOB STATUS AGENT
job_status_agent = Agent(
    model='gemini-2.5-flash',
    name='job_status_agent',
    description='Checks the status of a running BigQuery job.',
    instruction="""You are the Job Status Agent.
    
    **YOUR STEPS:**
    1. Get the target table ID from 'target_table_id' state.
    2. Call the `check_etl_status` tool with the target_table parameter.
    3. The tool will return the current job status (RUNNING, SUCCESS, or FAILED).
    4. Report the status to the user in a friendly way.
    """,
    tools=[etl_tools_instance.check_etl_status],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)

# --- LOOPS ---

# A. PLAN REVISION LOOP
plan_revision_loop = LoopAgent(
    name="plan_revision_loop",
    description="Iteratively creates and validates a plan until it is satisfactory or rejected.",
    sub_agents=[transformation_planning_agent, plan_validation_agent],
    max_iterations=2
)

# B. SQL REVISION LOOP
sql_revision_loop = LoopAgent(
    name="sql_revision_loop",
    description="Iteratively creates and validates a SQL query until it is approved by the user.",
    sub_agents=[sql_generation_agent, sql_validation_agent],
    max_iterations=2,
)

# --- PIPELINES ---

# A. PLANNING PIPELINE
etl_planning_pipeline = SequentialAgent(
    name="etl_planning_pipeline",
    description="A pipeline to discover tables and then create and validate a transformation plan.",
    sub_agents=[
        table_discovery_agent,
        plan_revision_loop,
        plan_confirmation_agent,
    ],
)

# B. EXECUTION PIPELINE
etl_execution_pipeline = SequentialAgent(
    name="etl_execution_pipeline",
    description="A pipeline to generate and validate the transformation SQL.",
    sub_agents=[
        sql_revision_loop,
        sql_confirmation_agent
    ],
)

# C. ROOT AGENT (Orchestrator)
root_agent = Agent(
    model='gemini-2.5-flash',
    name='etl_orchestrator_agent',
    description='Controls the ETL flow by prioritizing the next required step based on state.',
    instruction="""
    You are the ETL Orchestrator Agent. Guide the user through the ETL creation process.
    
    **Evaluate conditions in strict order and execute the FIRST step that matches:**

    1. **INITIAL GREETING & START:**
        - If 'target_schema_requested' is NOT set:
            - Set 'target_schema_requested' to True.
            - Greet the user, explain your role, and ask for the target table schema (ideally with DDL).
            - Once you get reponse from the user proceed to next step 2.

    2. **SCHEMA CAPTURE:**
        - If 'target_schema' is NOT set AND 'target_schema_requested' is True:
            - Call `set_initial_target_table_info` with the user's message.
            - After that proceed to step 3.

    3. **PLANNING & VALIDATION:**
        - If 'target_schema' is set AND 'plan_approved' is NOT set:
            - Call `etl_planning_pipeline`. This pipeline will handle the entire planning, validation, and revision loop.
    
    4. **PLAN REVISION (from user feedback):**
        - If the user provides feedback on the plan (i.e., their response is not 'yes'):
            - Save their feedback to the 'plan_feedback' state key.
            - Set 'plan_approved' to "revise".
            - Call `etl_planning_pipeline` again to incorporate the feedback.

    5. **SQL GENERATION & SAMPLING:**
        - If 'plan_approved' is True AND 'sample_approved' is NOT set:
            - Call `etl_execution_pipeline`. This will handle the entire SQL generation, validation, and revision loop.

    6. **SQL REVISION (from user feedback):**
        - If the user provides feedback on the SQL sample (i.e., their response is not 'yes'):
            - Save their feedback to the 'sql_feedback' state key.
            - Set 'sample_approved' to "refine".
            - Call `etl_execution_pipeline` again to incorporate the feedback.

    7. **FINAL EXECUTION & STATUS:**
        - If 'plan_approved' is True AND 'sample_approved' is True:
            - If 'job_id' is NOT set: Call `final_execution_agent` to start the async job.

    8. **EXECUTION STATUS CHECK:** 
        - If 'job_id' is True:
            - If asked by user to check status, Call `job_status_agent` to get the status.
    """,
    tools=[append_to_state, set_initial_target_table_info],
    sub_agents=[
        etl_planning_pipeline,
        etl_execution_pipeline,
        final_execution_agent,
        job_status_agent
    ],
    before_model_callback=log_query_to_model,
    after_model_callback=log_model_response
)
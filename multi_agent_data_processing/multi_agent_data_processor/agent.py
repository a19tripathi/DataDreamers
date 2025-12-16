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
    tools=[bigquery_toolset, append_to_state]
)

# 2. TRANSFORMATION PLANNING AGENT
transformation_planning_agent = Agent(
    model='gemini-2.5-pro',
    name='transformation_planning_agent',
    description='Creates a high-level transformation plan.',
    instruction="""
    You are an expert ETL architect. Your task is to create or revise a high-level transformation plan.
    
    The user wants to create a target table with the following schema: "{target_schema}"
    The available source tables are: {source_tables}
    
    **CRITICAL CHECK:** Look for existing feedback in the state key 'plan_feedback'.
    - If 'plan_feedback' exists, you MUST revise the previous plan based on the feedback.
    - If no feedback exists, you are creating the first version of the plan.

    **Your steps are:**
    1. Formulate a detailed conceptual plan. If revising, explicitly mention what you changed.
    2. Save your new or revised plan to the 'transformation_plan' state key using `append_to_state`.
    3. Present the complete plan to the user clearly.
    4. Also present the user the **CONFIDENCE SCORE from 0 to 1** for the plan.
    5. Ask user to review the presented plan.
    """,
    tools=[bigquery_toolset, append_to_state], 
)

# 3. PLAN CONFIRMATION AGENT
plan_confirmation_agent = Agent(
    model='gemini-2.5-flash',
    name='plan_confirmation_agent',
    description='Processes user approval or rejection for the plan.',
    instruction="""
    You are the Plan Confirmation Agent.
    1. If the user explicitly approves ("yes", "proceed", "looks good"), set the 'plan_approved' state key to True.
    2. If the user rejects ("no", "stop"), set 'plan_approved' to False.
    3. If the user provides feedback or asks for changes, save their feedback to the 'plan_feedback' state key and set 'plan_approved' to "revise".
    4. Once done, return the control to `root_agent`.
    """,
    tools=[append_to_state],
)

# 4. SQL GENERATION AGENT
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
    - Source dataset: '{DATASET_ID}'
    
    **CRITICAL:** Look for 'sql_feedback' in state. If it exists, revise your previous query.

    **YOUR STEPS:**
    1. Write ONLY a SELECT query (NOT CREATE TABLE). The SELECT should produce data matching the target schema.
    2. Fully qualify all source table names with project and dataset.
    3. Save the SELECT query to the 'sql_query' state key using `append_to_state`.
    4. Present the query to the user and explain it's ready for validation.
    
    **IMPORTANT:** Generate ONLY the SELECT statement. The CREATE TABLE will be handled separately during execution.
    """,
    tools=[bigquery_toolset, append_to_state]
)

# 5. SQL VALIDATION AGENT
sql_validation_agent = Agent(
    model='gemini-2.5-flash',
    name='sql_validation_agent',
    description='Validates the generated SQL query by executing a sample.',
    instruction="""You are a Data Validation and Sampling Agent.

    **CONTEXT:** The SELECT query is in the 'sql_query' state key.

    **YOUR TASK:** Test the query with a LIMIT to show sample data.

    **STEPS:**
    1. Get the sql_query from state.
    2. If it contains a semicolon at the end, remove it.
    3. Add " LIMIT 10" to the end.
    4. Call execute_sql tool with this modified query to get sample data.
    5. If execute_sql succeeds: Show the sample data and ask "Does this sample look correct? Reply 'yes' to proceed or provide feedback."
    6. If execute_sql fails: Use `append_to_state` to save the error to 'sql_feedback', set 'sample_approved' to "refine", and tell the user.

    **IMPORTANT:** Do NOT write Python code. Just use the tools.
    """,
    tools=[bigquery_toolset, append_to_state, exit_loop],
)

# 6. SAMPLE APPROVAL AGENT
sample_approval_agent = Agent(
    model='gemini-2.5-flash',
    name='sample_approval_agent',
    description='Processes user approval for the data sample.',
    instruction="""You are the Sample Approval Agent.
    1. If the user approves, set 'sample_approved' to True.
    2. If the user rejects or asks for changes, save feedback to 'sql_feedback' and set 'sample_approved' to "refine".
    3. If the user says to stop, set 'sample_approved' to False.
    4. Confirm your action with confidence score.
    5. Once done, return the control to `root_agent`.
    """, 
    tools=[append_to_state],
)

# 7. FINAL EXECUTION AGENT
final_execution_agent = Agent(
    model='gemini-2.5-flash',
    name='final_execution_agent',
    description='Executes the final ETL job asynchronously in BigQuery.',
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
    7. Once done, return the control to `root_agent`.
    """,
    tools=[etl_tools_instance.create_etl_job, append_to_state],
)

# 8. JOB STATUS AGENT
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
    tools=[etl_tools_instance.check_etl_status]
)

# --- PIPELINES ---

# A. PLANNING PIPELINE
etl_planning_pipeline = SequentialAgent(
    name="etl_planning_pipeline",
    description="A pipeline to discover tables and create a transformation plan.",
    sub_agents=[
        table_discovery_agent,
        transformation_planning_agent,
        plan_confirmation_agent
    ],
)

# B. EXECUTION PIPELINE
etl_execution_pipeline = LoopAgent(
    name="etl_execution_pipeline",
    description="A pipeline to generate and validate the transformation SQL.",
    max_iterations=5,
    sub_agents=[
        sql_generation_agent,
        sql_validation_agent,
        sample_approval_agent
    ],
)

# C. ROOT AGENT (Orchestrator)
root_agent = Agent(
    model='gemini-2.5-pro',
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
        - If 'target_schema_requested' is True AND 'target_schema' is NOT set:
            - Call `set_initial_target_table_info` with the user's message.
            - After that proceed to step 3.

    3. **PLANNING & APPROVAL/REVISION:**
        - If 'plan_approved' is NOT True (not set or "revise"):
            - If 'plan_approved' not set: Call `etl_planning_pipeline`.
            - If 'plan_approved' is set to "revise" u repeat step 3.
            - Once the plan is approved by user proceed to step 4.

    4. **SQL GENERATION & SAMPLING:**
        - If 'plan_approved' is True AND 'sample_approved' is NOT True (not set or "refine"):
            - If `sample_approved` is not set: Call `etl_execution_pipeline` to generate/validate SQL and show sample.
            - If `sample_approved` is set to "refine" u repeat step 4.
            - Once the plan is approved by user proceed to step 5.

    5. **FINAL EXECUTION & STATUS:**
        - If 'plan_approved' is True AND 'sample_approved' is True:
            - If 'job_id' is NOT set: Call `final_execution_agent` to start the async job.

    6. **EXECUTION STATUS CHECK:** 
        - If 'job_id' is True:
            - If asked by user to check status, Call `job_status_agent` to get the status.
    """,
    tools=[append_to_state, exit_loop, set_initial_target_table_info],
    sub_agents=[
        etl_planning_pipeline,
        etl_execution_pipeline,
        final_execution_agent,
        job_status_agent
    ]
)
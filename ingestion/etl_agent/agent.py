import os
import traceback
from dotenv import load_dotenv
from google.cloud import bigquery

# Import your ADK Agent
from google.adk.agents.llm_agent import Agent

load_dotenv()

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

    def create_etl_job(self, target_table: str, sql_query: str) -> str:
        """
        Triggers a BigQuery ETL job for a specific target table using the provided SQL.
        
        Args:
            target_table (str): The full destination table ID (e.g., project.dataset.table).
            sql_query (str): The SQL query to execute.
            
        Returns:
            str: Confirmation message with the Job ID.
        """
        try:
            print(f"--> [Tool] Submitting job for: {target_table}")
            
            job_config = bigquery.QueryJobConfig(
                destination=target_table,
                write_disposition="WRITE_TRUNCATE"
            )
            
            query_job = self.client.query(sql_query, job_config=job_config)
            
            # --- SAVE TO MEMORY ---
            self.job_memory[target_table] = {
                "job_id": query_job.job_id,
                "location": query_job.location
            }
            
            return f"Job successfully submitted. Job ID: {query_job.job_id}. I have remembered this ID for status checks."

        except Exception as e:
            traceback.print_exc()
            return f"Failed to create job: {str(e)}"

    def check_etl_status(self, target_table: str) -> str:
        """
        Checks the status of the pipeline for a specific table. 
        Uses internal memory to find the Job ID, so the user does not need to provide it.
        
        Args:
            target_table (str): The table name to check.
            
        Returns:
            str: The current status (RUNNING, SUCCESS, FAILED).
        """
        print(f"--> [Tool] Checking status for: {target_table}")
        
        # 1. Look up memory
        if target_table not in self.job_memory:
            return f"I don't have a record of a job for '{target_table}'. Please ask me to create the job first."
            
        job_info = self.job_memory[target_table]
        
        # 2. Call API
        try:
            job = self.client.get_job(job_info['job_id'], location=job_info['location'])
            
            if job.state == 'DONE':
                if job.error_result:
                    return f"FAILED: {job.error_result['message']}"
                else:
                    return "SUCCESS: The data load is complete."
            else:
                return f"RUNNING: The job is currently in state {job.state}."
        except Exception as e:
            return f"Error checking API: {str(e)}"

# ==============================================================================
# AGENT CONFIGURATION
# ==============================================================================

# 1. Instantiate the Tool Class (This keeps the memory alive)
etl_tools_instance = ETLTools()

# 2. Define the Agent
root_agent = Agent(
    model='gemini-2.5-flash',
    name='root_agent',
    description='A helpful assistant for managing BigQuery ETL pipelines.',
    instruction=(
        "You are an ETL Agent. "
        "1. When asked to load data or run a query, use the 'create_etl_job' tool. "
        "2. When asked for status, use the 'check_etl_status' tool. "
        "3. You do NOT need to ask the user for a Job ID; you have access to it via the tools."
    ),
    # 3. Register the methods as tools
    tools=[
        etl_tools_instance.create_etl_job,
        etl_tools_instance.check_etl_status
    ]
)
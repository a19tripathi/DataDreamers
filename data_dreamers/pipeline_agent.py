import os
import traceback
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.adk.agents import Agent, LoopAgent
from google.adk.tools import FunctionTool, exit_loop

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

    def create_dataset(self, dataset_id: str) -> str:
        """
        Creates a BigQuery dataset if it doesn't exist.

        Args:
            dataset_id (str): The full dataset ID (e.g., project.dataset).

        Returns:
            str: Confirmation message.
        """
        try:
            self.client.get_dataset(dataset_id)
            return f"Dataset {dataset_id} already exists."
        except NotFound:
            print(f"--> [Tool] Dataset {dataset_id} not found. Creating it...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "us-central1"
            self.client.create_dataset(dataset, timeout=30) # type: ignore
            return f"Successfully created dataset {dataset_id}."
        except Exception as e:
            return f"Failed to create dataset: {str(e)}"

    def create_table_from_ddl(self, ddl_statement: str) -> str:
        """
        Creates a BigQuery table using a DDL statement. It ensures the dataset exists first.

        Args:
            ddl_statement (str): The SQL DDL statement to create the table.

        Returns:
            str: Confirmation message.
        """
        try:
            print(f"--> [Tool] Executing DDL to create table...")
            job_config = bigquery.QueryJobConfig(
                default_dataset=f"{self.client.project}.us-central1"
            )
            query_job = self.client.query(ddl_statement, job_config=job_config, location="us-central1")
            query_job.result()  # Wait for the job to complete
            return f"Successfully executed DDL. Table should now exist."
        except Exception as e:
            return f"Failed to create table from DDL: {str(e)}"

    def create_etl_job(self, target_table: str, sql_query: str) -> str:
        """
        Triggers a BigQuery ETL job for a specific target table using the provided SQL.
        
        Args:
            target_table (str): The full destination table ID (e.g., project.dataset.table).
            sql_query (str): The SQL query to execute it should only have select statement.
            
        Returns:
            str: Confirmation message with the Job ID.
        """
        try:
            # --- ENSURE DATASET EXISTS ---
            table_ref = bigquery.Table.from_string(target_table)
            dataset_id = f"{table_ref.project}.{table_ref.dataset_id}"
            try:
                self.client.get_dataset(dataset_id)  # Check if dataset exists
                print(f"--> [Tool] Dataset {dataset_id} already exists.")
            except NotFound:
                print(f"--> [Tool] Dataset {dataset_id} not found. Creating it...")
                dataset = bigquery.Dataset(dataset_id) # type: ignore
                dataset.location = "us-central1"
                self.client.create_dataset(dataset, timeout=30)

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

# 2. Define the Setup Agent
pipeline_setup_agent = Agent(
    model='gemini-2.5-flash',
    name='pipeline_setup_agent',
    description='An agent that sets up the BigQuery environment and creates the ETL job.',
    instruction="""You are a specialized BigQuery ETL setup agent. Your purpose is to prepare the target environment and create a data loading job based on the final, approved SQL query.

Your workflow is as follows:

1.  **Prepare Target Environment**:
    - Your first action is to derive the production target table name from the `target_ddl` in the state. If the table name in the DDL includes 'staging', you must replace 'staging' with 'target'. For example, `my_project.my_dataset.staging_users` becomes `my_project.my_dataset.target_users`.
    - From the derived table name (e.g., `my_project.my_dataset.target_users`), you must extract the dataset ID (`my_project.my_dataset`) and call the `create_dataset` tool to ensure it exists.
    - After ensuring the dataset exists, you must use the `create_table_from_ddl` tool with the modified DDL to create the final target table. This ensures the destination for the ETL job exists with the correct schema.

    - **CRITICAL**: Before using the `create_etl_job` tool, you MUST stop and ask the user if they want to proceed with the job execution for the derived production table.
    - Your response should explicitly state the exact production target table name and the action you are about to take, e.g., "I am ready to start the ETL job to load data into `my_project.my_dataset.target_users`. Do you want to proceed? (yes/no)".
    - Only if the user responds with a clear confirmation (e.g., "yes", "confirm", "proceed"), you may then call the `create_etl_job` tool with the derived production target table name and the provided `sql_query`.

2.  **Monitor Job Status**:
    - After asking for confirmation (or after job creation), if the user asks for the status, you MUST use the `check_etl_status` tool.
    - Provide the `target_table` name to the tool. The tool remembers the Job ID, so DO NOT ask the user for it.
    - Relay the status (RUNNING, SUCCESS, FAILED) to the user. If the job is successful, call `exit_loop` to signify completion.

3.  **Error Handling**:
    - If any tool call fails (e.g., `create_table_from_ddl` or `create_etl_job`), you must output the error message you received from the tool. This will trigger a retry from the parent loop agent.
""",
    tools=[
        etl_tools_instance.create_dataset,
        etl_tools_instance.create_table_from_ddl,
        etl_tools_instance.create_etl_job,
        etl_tools_instance.check_etl_status,
        exit_loop
    ]
)

# 3. Define the Error Handling Loop Agent
pipeline_agent = LoopAgent(
    name="pipeline_agent",
    description="A resilient workflow for setting up and running a BigQuery ETL job. It will retry on failure.",
    sub_agents=[
        pipeline_setup_agent,
    ],
    max_iterations=3, # Retry up to 3 times
)
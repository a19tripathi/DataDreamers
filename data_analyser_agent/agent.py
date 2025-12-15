from google.adk import Agent
from google.adk.tools import FunctionTool
from .prompt import DATA_PROFILING_SYSTEM_PROMPT
from .tools import list_table_ids, get_table_info, execute_sql, BQ_CLIENT
from .memory import AgentMemory
import os
from dotenv import load_dotenv

load_dotenv()

# --- GCP Environment Setup ---
# Get Project ID from environment (standard practice in GCP)
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID', "test_dataset") # Use env var or default
FULL_DATASET_ID = f"{PROJECT_ID}.{DATASET_ID}" if PROJECT_ID else DATASET_ID


# Instantiate Memory (used to store the profile)
AGENT_MEMORY = AgentMemory()

# Define the tools the Agent can use (MANDATORY for LLM function calling)
BQ_TOOLS = [list_table_ids, get_table_info, execute_sql]


def memory_write(key: str, value: dict):
    """Tool to write the final structured data profile to memory."""
    AGENT_MEMORY.write(key, value)
    print(f"Memory write successful for key: {key}")
    return {"status": "success", "message": f"Profile saved to memory with key: {key}"}

# Add memory write function as a tool for the agent
# Note: The agent framework should have a way to define custom tools. 
# We explicitly define the required function signatures here.
CUSTOM_TOOLS = [
    memory_write
]

# The Data Analysis Agent definition
root_agent = Agent(
    model='gemini-2.5-flash',
    name='data_analysis_agent',
    description='A data profiler agent that summarises table information using BigQuery.',
    instruction=DATA_PROFILING_SYSTEM_PROMPT,
    tools=BQ_TOOLS + CUSTOM_TOOLS,
)
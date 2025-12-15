# agent.py (FINAL, CLEANED CODE)

import os
from dotenv import load_dotenv
from typing import List, Any, Dict
import json


# Core ADK and Pydantic (needed only for response schema/tools)
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from pydantic import BaseModel, Field # Pydantic remains for schema definition only

# Assuming memory_write, AgentMemory, prompts, and tools are in sibling modules
from .memory import AgentMemory
from .prompts import DATA_PROFILING_SYSTEM_PROMPT
from .tools import PROJECT_ID, list_table_ids, get_table_info, execute_sql, get_full_table_profile_sql # <-- Re-add custom tools!

load_dotenv()

# --- Pydantic Schema Definition (Must remain for structured output) ---
# NOTE: The implementation of these classes is omitted for brevity but should remain
# exactly as defined in the previous working version.
class ColumnProfile(BaseModel):
    column_name: str = Field(description="Name of the column.")
    summary_description: str = Field(description="A concise, one-sentence summary of the column's content and statistics.")

class TableMetadata(BaseModel):
    table_name: str
    dataset_id: str
    row_count: str
    project_id: str
    overall_summary: str = Field(description="A high-level summary of the entire table's purpose and key characteristics.")

class DataProfile(BaseModel):
    table_metadata: TableMetadata
    column_summaries: List[ColumnProfile]


# --- Agent Configuration and Tools ---

# ... (PROJECT_ID and FULL_DATASET_ID context setup remains) ...

# Instantiate Memory and memory_write function
AGENT_MEMORY = AgentMemory()

def memory_write(key: str, data_profile_json: Dict[str, Any]) -> str:
    """
    Tool to write the final structured data profile to memory AND 
    generate the prompt for the final natural language summary.
    """
    # 1. Save the structured JSON for the Validation Agent
    AGENT_MEMORY.write(key, data_profile_json) 
    
    # 2. Generate the summary instruction text
    profile_text = json.dumps(data_profile_json)

    # 3. Return a strong instruction to the LLM to generate the human report
    return (
        f"SUCCESS: The structured data profile has been saved to memory with key: {key}. "
        f"Your NEW and FINAL instruction is to take this profile data and convert it into a "
        f"clean, professional, human-readable summary. DO NOT output the JSON again. "
        f"The profile data is: {profile_text}"
    )

# 1. Define custom BQ tools (REPLACING THE TOOLSET)
CUSTOM_BQ_TOOLS = [
    list_table_ids,          # For orchestration/discovery
    get_table_info,          # For schema lookup
    execute_sql,             # For general query execution
    get_full_table_profile_sql # For fast, comprehensive statistics
]

# The Data Analysis Agent definition
root_agent = Agent(
    model='gemini-2.5-flash',
    name='custom_bq_profiler',
    description='An agent that profiles and queries BigQuery using custom Python tool functions.',
    instruction=DATA_PROFILING_SYSTEM_PROMPT,
    # CRITICAL FIX: Use the list of custom Python functions
    tools=[*CUSTOM_BQ_TOOLS, FunctionTool(memory_write)], 
    
    # FINAL FIX: Rely on environment variable only.
    
    # Do not include response_schema as it causes validation issues, we rely on the prompt's strict formatting.
)
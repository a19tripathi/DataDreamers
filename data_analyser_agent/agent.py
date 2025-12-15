from google.adk import Agent
from google.adk.tools import FunctionTool
from .prompts import DATA_PROFILING_SYSTEM_PROMPT
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


Main.py
import os
import json
# UPDATED: Import Content and Part from google.genai.types
from google.genai.types import Content, Part
from agent import root_agent, AGENT_MEMORY, FULL_DATASET_ID, PROJECT_ID
from tools import list_table_ids # Import the list function from your tools file
from typing import List, Dict, Any

# --- Define the function that runs your Agent ---

def run_data_analysis_workflow_for_all_tables(dataset_id: str = FULL_DATASET_ID) -> List[Dict[str, Any]]:
    """
    Orchestrates the profiling for ALL tables in the specified BigQuery dataset.
    """
    if not PROJECT_ID:
        print("ERROR: GOOGLE_CLOUD_PROJECT environment variable is not set.")
        return []

    all_profiles = []

    # 1. DISCOVERY: Get the list of all staging tables in the dataset
    print(f"\n--- Starting Discovery in Dataset: {dataset_id} ---")
    
    # Use the BQ tool function to list all tables
    table_list = list_table_ids(dataset_id) 

    if isinstance(table_list, list) and not table_list:
        print(f"Dataset {dataset_id} contains no tables.")
        return []
    elif not isinstance(table_list, list) or "error" in table_list[0]:
        print(f"Error during table listing: {table_list}")
        return []

    print(f"Found {len(table_list)} tables to profile: {table_list}")

    # 2. ITERATION: Loop through each table and run the AI Agent
    for table_name in table_list:
        print(f"\n=============================================")
        print(f"--- Processing Table: {table_name} ---")
        print(f"=============================================")

        # Format the input for the LLM agent (one table at a time)
        input_message = (
            f"Begin the profiling workflow. "
            f"The target dataset is: {dataset_id}. "
            f"The specific table to profile is: {table_name}. "
            f"Follow all steps in your instructions precisely."
        )
        
        # 2a. Run the Data Analysis Agent
        try:
            # The agent executes tool calls, generates SQL, and saves results to memory
            # UPDATED: Construct Content and Part using standard Gen AI SDK syntax
            root_agent.run(
                Content(parts=[Part(text=input_message)])
            )
            
            # 2b. Retrieve the final data profile from memory
            final_key = f"data_profiling::{dataset_id}::{table_name}"
            final_profile = AGENT_MEMORY.read(final_key)
            
            if final_profile:
                all_profiles.append(final_profile)
                print(f"SUCCESS: Profile for {table_name} retrieved and added to list.")
            else:
                print(f"WARNING: Profile for {table_name} was NOT found in memory after run.")

        except Exception as e:
            print(f"CRITICAL ERROR processing {table_name}: {e}")
            # Optionally, log a failed profile object
            all_profiles.append({"table": table_name, "status": "FAILED", "error": str(e)})


    print("\n--- All Tables Processed ---")
    # This list is the FINAL HANDOFF to the Cleanup and Validation Agent
    return all_profiles


# --- Entry Point Simulation for Local Testing ---
if __name__ == "__main__":
    # Ensure environment variables and BQ client are correctly set up
    
    # You will need to ensure your BQ dataset has multiple tables for this test.
    
    # The final output is a list containing the structured profile for *every* table.
    final_data_profiles = run_data_analysis_workflow_for_all_tables(FULL_DATASET_ID)
    
    if final_data_profiles:
        print("\n--- Consolidated Data Analysis Agent Output (JSON Handoff to Validation Agent) ---")
        # Display the list of all profiles
        print(json.dumps(final_data_profiles, indent=2))
        
        # Handoff Complete! Person 3's Cleanup/Validation Agent (your next step) 
        # will now consume this list of JSON objects to generate a Transformation Plan for the entire batch.
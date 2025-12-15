# main_local_test.py (The Orchestrator)

import os
import json
from typing import List, Dict, Any

# UPDATED: Import Content and Part from google.genai.types
from google.genai.types import Content, Part
from google.genai.errors import APIError

# Import the necessary components from sibling modules
from agent import root_agent, AGENT_MEMORY, FULL_DATASET_ID, PROJECT_ID
from tools import list_table_ids, get_table_info, get_full_table_profile_sql # Import fast custom BQ functions


# --- Define the function that runs your Agent ---

def run_data_analysis_workflow_for_all_tables(dataset_id: str = FULL_DATASET_ID) -> List[Dict[str, Any]]:
    """
    Orchestrates the high-speed profiling and summarization for ALL tables in the specified BigQuery dataset.
    """
    if not PROJECT_ID:
        print("ERROR: GOOGLE_CLOUD_PROJECT environment variable is not set. Check .env file.")
        return []

    all_profiles = []

    # 1. DISCOVERY: Get the list of all staging tables in the dataset
    print(f"\n--- Starting Discovery in Dataset: {dataset_id} ---")
    
    # Use the fast, custom BQ tool function to list all tables
    table_list = list_table_ids(dataset_id) 

    if isinstance(table_list, list) and not table_list:
        print(f"Dataset {dataset_id} contains no tables.")
        return []
    elif not isinstance(table_list, list) or "error" in table_list[0]:
        print(f"Error during table listing: {table_list}")
        return []

    print(f"Found {len(table_list)} tables to profile: {table_list}")

    # 2. ITERATION: Loop through each table and run the fast pre-computation
    for i, table_name in enumerate(table_list):
        print(f"\n=============================================")
        print(f"--- Processing Table {i+1}/{len(table_list)}: {table_name} ---")
        print(f"=============================================")

        # --- PHASE 1: FAST PRE-COMPUTATION (NON-LLM) ---
        try:
            # 2a. Get Schema and Row Count (Fast BQ API call)
            schema_info = get_table_info(table_name, dataset_id)
            if 'error' in schema_info: 
                print(f"Skipping {table_name}: Error retrieving schema: {schema_info['error']}")
                continue

            # 2b. Execute comprehensive SQL query (Fast BQ SQL call)
            # This generates one optimized query and runs it.
            raw_stats = get_full_table_profile_sql(dataset_id, table_name, schema_info['schema'])
            if not raw_stats or 'error' in raw_stats[0]: 
                print(f"Skipping {table_name}: Error retrieving raw statistics: {raw_stats[0]['error'] if raw_stats else 'No results.'}")
                continue
            
            # Combine everything into one object to pass to the LLM
            pre_computed_data = {
                "table_metadata": {
                    "table_name": table_name,
                    "dataset_id": dataset_id,
                    "row_count": str(schema_info['row_count']),
                    "project_id": PROJECT_ID,
                },
                "schema": schema_info['schema'],
                "raw_statistics": raw_stats[0] # The single row of aggregates
            }
            
            print("Pre-computation complete. Handing off data to LLM for summarization.")
            
        except Exception as e:
            print(f"CRITICAL ERROR during pre-computation for {table_name}: {e}")
            continue

        # --- PHASE 2: LLM SUMMARIZATION (Single, Fast Synthesis Call) ---

        # 2c. Format the input, asking the LLM to *summarize* the raw data.
        input_message = (
            f"Generate the complete structured Data Summary JSON. The raw data and statistics are provided below. "
            f"Synthesize all summaries based ONLY on this data. "
            f"RAW_DATA: {json.dumps(pre_computed_data)}"
        )
        
        try:
            # Run the Data Analysis Agent (Executes the LLM Synthesis -> memory_write)
            root_agent.run(
                Content(parts=[Part(text=input_message)])
            )
            
            # 2d. Retrieve the final data profile from memory after the run completes
            final_key = f"data_profiling::{dataset_id}::{table_name}"
            final_profile = AGENT_MEMORY.read(final_key)
            
            if final_profile:
                all_profiles.append(final_profile)
                print(f"SUCCESS: Profile for {table_name} retrieved from memory.")
            else:
                print(f"WARNING: Profile for {table_name} was NOT found in memory after run.")

        except APIError as e:
            print(f"CRITICAL GEMINI ERROR during synthesis for {table_name}: {e}")
        except Exception as e:
            print(f"CRITICAL EXECUTION ERROR during synthesis for {table_name}: {e}")


    print("\n--- All Tables Processed ---")
    # This list is the FINAL HANDOFF
    return all_profiles


# --- Entry Point Simulation for Local Testing ---
if __name__ == "__main__":
    
    print("\n--- Starting Data Profiling Workflow ---")
    
    # Run the orchestration loop
    final_data_profiles = run_data_analysis_workflow_for_all_tables(FULL_DATASET_ID)
    
    if final_data_profiles:
        print("\n--- CONSOLIDATED FINAL Handoff (List of all Profiles) ---")
        # Display the list of all profiles
        print(json.dumps(final_data_profiles, indent=2))
    else:
        print("\n--- Workflow finished with no data profiles generated. ---")
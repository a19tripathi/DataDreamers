import os
import sys
import inspect
import json
from dotenv import load_dotenv

# Load environment variables from a .env file (if it exists)
# This is crucial for local testing outside of a GCP runtime environment.
load_dotenv() 

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import the core logic from your project files
# Ensure the import path is correct based on your folder structure
try:
    from data_analyst_agent.main import run_data_analysis_workflow_for_all_tables
    from data_analyst_agent.agent import FULL_DATASET_ID
except ImportError as e:
    print(f"Error importing core modules: {e}")
    print("Ensure you are running this script from the root of your agent directory.")
    exit()

# --- Configuration & Setup ---

# 1. Provide the BigQuery URL/ID here for testing.
# In BQ, the 'URL' is the fully qualified ID: PROJECT_ID.DATASET_ID
# We rely on the environment variables defined in your .env file or system.

# You can hardcode a fallback here for testing if environment variables aren't set:
TEST_PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT') or "ccibt-hack25ww7-717"
TEST_DATASET_ID = os.environ.get('BIGQUERY_DATASET_ID') or "test_dataset"
TEST_FULL_DATASET_ID = f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}"


def setup_local_environment():
    """Prints instructions and sets environment variables if needed."""
    print("--- 🚀 LLL Data Analysis Agent Local Execution Setup ---")
    print("\n[STEP 1: Authentication (Critical)]")
    print("For zero-issue BigQuery access, please ensure you have run ONE of the following:")
    print("  A) Terminal Command: `gcloud auth application-default login`")
    print("  B) If using an IDE: Ensure your IDE is configured with a GCP Service Account.")
    
    print("\n[STEP 2: Environment Variables]")
    if not os.environ.get('GOOGLE_CLOUD_PROJECT'):
        print(f"⚠️ Setting GOOGLE_CLOUD_PROJECT to fallback: {TEST_PROJECT_ID}")
        # Only set here if not found, to ensure the BQ client has context
        os.environ['GOOGLE_CLOUD_PROJECT'] = TEST_PROJECT_ID

    print(f"\n[Configuration Loaded]")
    print(f"  Project ID: {TEST_PROJECT_ID}")
    print(f"  Dataset ID: {TEST_DATASET_ID}")
    print(f"  Full BQ Target: {TEST_FULL_DATASET_ID}")
    print("-----------------------------------------------------")


def main():
    """
    Main execution function to run the Data Analysis Agent independently.
    """
    setup_local_environment()
    
    # 3. Execution: Call the function that runs the iteration loop
    print("\nStarting independent agent run...")
    
    # We pass the full dataset ID to the orchestrating function
    final_data_profiles = run_data_analysis_workflow_for_all_tables(TEST_FULL_DATASET_ID)
    
    # 4. Output: Print the result for verification (Handoff to Person 4)
    if final_data_profiles:
        print("\n✅ AGENT RUN SUCCESSFUL.")
        print("\n--- Final Consolidated Profile Handoff (JSON) ---")
        print(json.dumps(final_data_profiles, indent=2))
    else:
        print("\n❌ AGENT RUN FAILED OR NO TABLES PROCESSED.")
        print("Please check the terminal output for errors or missing tables.")


if __name__ == "__main__":
    main()
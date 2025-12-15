from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound, Conflict
import os
import sys

# --- Configuration ---
PROJECT_ID = "ccibt-hack25ww7-717"
BUCKET_NAME = "datadreamers"
ROOT_STAGING_DATASET_ID = "staging_table" 

# --- Utility Functions (Dataset & Table Management) ---

def create_bigquery_dataset_if_not_exists(client: bigquery.Client, dataset_id: str, location: str = "us-central1") -> bool:
    """Creates a BigQuery Dataset if it does not already exist."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
        return True
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"Successfully created dataset '{dataset_id}'.")
            return True
        except Exception as e:
            print(f"ERROR: Failed to create dataset '{dataset_id}': {e}", file=sys.stderr)
            return False

def create_sub_dataset_if_not_exists(client: bigquery.Client, base_name: str, sub_path: str, location: str = "us-central1") -> str | None:
    """Creates a new dataset for a GCS folder path, e.g., staging_table_commercial_lending_data_source."""
    safe_sub_path = sub_path.replace('-', '_').replace('/', '_').lower()
    new_dataset_id = f"{base_name}_{safe_sub_path}"

    dataset_ref = client.dataset(new_dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Sub-Dataset '{new_dataset_id}' already exists.")
        return new_dataset_id
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"Successfully created sub-dataset '{new_dataset_id}'.")
            return new_dataset_id
        except Exception as e:
            print(f"ERROR: Failed to create sub-dataset '{new_dataset_id}': {e}", file=sys.stderr)
            return None

def delete_table_if_exists(client: bigquery.Client, table_ref: bigquery.TableReference):
    """Deletes a BigQuery table if it exists."""
    try:
        client.delete_table(table_ref, not_found_ok=True)
        print(f"Deleted existing table: {table_ref.dataset_id}.{table_ref.table_id}")
    except Exception as e:
        print(f"WARNING: Could not delete table {table_ref.dataset_id}.{table_ref.table_id}: {e}", file=sys.stderr)


# --- GCS Folder Selection Functions (Unchanged) ---
# [list_gcs_folders] and [get_user_folder_selection] go here...
def list_gcs_folders(project_id: str, bucket_name: str) -> list[str]:
    # ... (content remains the same)
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        all_blobs = bucket.list_blobs() 
        unique_top_level_folders = set()
        
        for blob in all_blobs:
            if '/' in blob.name:
                top_folder = blob.name.split('/')[0]
                unique_top_level_folders.add(top_folder)

        return sorted(list(unique_top_level_folders))

    except NotFound:
        print(f"Error: Bucket '{bucket_name}' not found in project '{project_id}'.", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Failed to initialize Google Cloud clients or list folders: {e}", file=sys.stderr)
        return []


def get_user_folder_selection(folders: list[str]) -> str | None:
    # ... (content remains the same)
    if not folders:
        print("\n❌ No folders containing files found in the bucket.")
        return None

    print("\n--- Available Folders in GCS Bucket ---")
    for i, folder in enumerate(folders, 1):
        print(f"[{i}] {folder}")
    print("---------------------------------------")

    while True:
        try:
            selection = input(
                "Enter the number of the folder you want to load (e.g., 1): "
            )
            if selection.lower() in ('q', 'quit', 'exit'):
                return None
                
            index = int(selection) - 1
            
            if 0 <= index < len(folders):
                selected_folder = folders[index]
                print(f"\n✅ You selected: **{selected_folder}**")
                return selected_folder
            else:
                print("⚠️ Invalid number. Please enter a number from the list.")
        except ValueError:
            print("⚠️ Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            return None


# --- CSV File Handler (Modified to delete table before load) ---
def handle_csv_files(
    bq_client: bigquery.Client, 
    storage_client: storage.Client, 
    bucket_name: str, 
    gcs_prefix: str, 
    target_dataset_id: str
):
    """
    Finds .csv files and loads each into a separate BigQuery table.
    Crucially, it deletes the table before each load to force BigQuery 
    to correctly re-detect the schema and headers from the new file.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=gcs_prefix))

    successful_loads = 0
    failed_loads = 0
    
    for blob in blobs:
        file_name = os.path.basename(blob.name)
        
        if blob.name.endswith('/') or not file_name.lower().endswith('.csv'):
            continue

        table_id = file_name.split('.')[0].lower()
        gcs_uri = f"gs://{bucket_name}/{blob.name}"

        table_ref = bq_client.dataset(target_dataset_id).table(table_id)
        
        # 1. CRITICAL STEP: Delete the existing table to force schema re-detection
        delete_table_if_exists(bq_client, table_ref)

        # 2. Configure the BigQuery load job
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1, # This tells BQ the first row is the header
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        )
        
        try:
            print(f"Starting load job for **{file_name}** into **{target_dataset_id}.{table_id}**...")
            load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job.result()
            successful_loads += 1

        except Exception as e:
            print(f"ERROR loading {file_name}: {e}", file=sys.stderr)
            failed_loads += 1
            
    return successful_loads, failed_loads

# --- Main Loader Function (Orchestration - Simplified) ---

def load_gcs_folder_to_bigquery(
    project_id: str,
    bucket_name: str,
    folder_name: str,
    root_dataset_id: str,
):
    """
    Orchestrates loading only the 'source' subfolder (CSV files).
    """
    
    # Initialize clients
    try:
        storage_client = storage.Client(project=project_id)
        bq_client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize Google Cloud clients: {e}", file=sys.stderr)
        return

    # 1. Ensure the root staging dataset (staging_table) exists
    if not create_bigquery_dataset_if_not_exists(bq_client, root_dataset_id):
        return

    # We only process the 'source' subfolder
    sub_folder = "source"
    gcs_sub_path = f"{folder_name}/{sub_folder}/"
    print(f"\n--- Processing GCS Sub-Folder: gs://{bucket_name}/{gcs_sub_path} ---")

    # 2. Create the folder-specific sub-dataset (e.g., staging_table_commercial_lending_data_source)
    target_dataset_id = create_sub_dataset_if_not_exists(bq_client, root_dataset_id, gcs_sub_path.rstrip('/'))
    
    if not target_dataset_id:
        return
        
    # 3. Call the CSV handler function
    successful, failed = handle_csv_files(
        bq_client, storage_client, bucket_name, gcs_sub_path, target_dataset_id
    )
    
    print(f"\n--- Summary for {gcs_sub_path} ---")
    print(f"Successful loads: {successful}")
    print(f"Failed loads: {failed}")

    print(f"\n\n--- GRAND TOTAL Load Job Summary ---")
    print(f"Total processed: {successful + failed}")
    print(f"Total Successful loads: {successful}")
    print(f"Total Failed loads: {failed}")


if __name__ == "__main__":
    print("--- Starting GCS to BigQuery Load Process ---")
    
    available_folders = list_gcs_folders(PROJECT_ID, BUCKET_NAME)
    FOLDER_NAME = get_user_folder_selection(available_folders)

    if FOLDER_NAME:
        load_gcs_folder_to_bigquery(
            PROJECT_ID, BUCKET_NAME, FOLDER_NAME, ROOT_STAGING_DATASET_ID
        )
    else:
        print("\nOperation aborted by user or no files were found.")
        
    print("--- Process Complete ---")
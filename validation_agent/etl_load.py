import os
from google.cloud import bigquery
from dotenv import load_dotenv
import traceback

load_dotenv()

# 1. Fetch Project ID securely
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not PROJECT_ID:
    print("WARNING: GOOGLE_CLOUD_PROJECT not set. Using default inference.")

def trigger_single_load(project_id, target_table, sql, write_mode="WRITE_TRUNCATE"):
    """
    Submits a job for a SINGLE table and returns ID and LOCATION.
    """
    if not isinstance(target_table, str):
        raise TypeError("Target table must be a string, got " + str(type(target_table)))
        
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.QueryJobConfig(
        destination=target_table,
        write_disposition=write_mode
    )
    
    # Simple print
    print("--> Submitting job for:", target_table)
    
    query_job = client.query(sql, job_config=job_config)
    
    return query_job.job_id, query_job.location

def check_job_status(job_id, location):
    """
    Checks the status of a specific job using the correct location.
    """
    client = bigquery.Client(project=PROJECT_ID)
    try:
        job = client.get_job(job_id, location=location)
        
        if job.state == 'DONE':
            if job.error_result:
                return "FAILED: " + str(job.error_result['message'])
            else:
                return "SUCCESS"
        else:
            return "RUNNING"
    except Exception as e:
        return "Error checking status: " + str(e)

# --- 1. Define the SQL Logic ---
loan_sql_logic = """
SELECT
    l.loan_id,
    l.borrower_id,
    l.facility_id,
    l.index_id,
    l.loan_number,
    l.status,
    l.origination_date,
    l.maturity_date,
    l.principal_amount,
    l.currency,
    l.purpose,
    l.loan_type,
    l.margin_bps,
    l.amortization_type,
    l.payment_frequency,
    l.compounding,
    ri.index_name,
    ri.tenor_months,
    ri.index_currency
FROM
    `ccibt-hack25ww7-717.staging_table_commercial_lending_data_source.loan` AS l
JOIN
    `ccibt-hack25ww7-717.staging_table_commercial_lending_data_source.rate_index` AS ri
ON
    l.index_id = ri.index_id
"""

# --- 2. Define the Tasks ---
tasks = [
    {
        "target": "ccibt-hack25ww7-717.target.final_loan", 
        "sql": loan_sql_logic
    },
]

job_details = []
print("Agent starting submissions...")

# --- 3. Async Loop ---
for task in tasks:
    try:
        # Submit Job
        jid, loc = trigger_single_load(
            project_id=PROJECT_ID, 
            target_table=task["target"], 
            sql=task["sql"]
        )
        print("Location : {} ",loc)
        job_details.append((jid, loc))
        
        # Simple print (No f-string)
        print("Submitted load for " + task['target'] + " (ID: " + str(jid) + " | Loc: " + str(loc) + ")")
        
    except Exception as e:
        print("Error submitting " + task['target'] + ": " + str(e))
        traceback.print_exc()

print("\nAll", len(job_details), "jobs submitted. Agent continues to other work...")

# --- 4. Check Status Example ---
if job_details:
    first_job_id, first_job_loc = job_details[0] 
    status = check_job_status(first_job_id, location=first_job_loc)
    
    # Simple print (No f-string)
    print("Status check for Job " + str(first_job_id) + ": " + str(status))
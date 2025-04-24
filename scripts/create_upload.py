# scripts/create_upload.py

import argparse
import os
import uuid
from datetime import datetime, timezone

# Import helper functions from common_utils
import common_utils
from data_generator import create_dummy_csv

# Define base directory relative to this script's location if needed,
# but common_utils uses os.getcwd(), assuming execution from root.
# We'll rely on common_utils.INCOMING_DIR

def main():
    parser = argparse.ArgumentParser(description="Simulate file upload and create MCP file.")
    parser.add_argument("--department", required=True, help="Uploading department (e.g., Sales, Marketing)")
    parser.add_argument("--filename", required=True, help="Base name for the uploaded file (without extension)")
    args = parser.parse_args()

    department = args.department
    base_filename = args.filename

    # --- Generate unique elements ---
    job_id = str(uuid.uuid4())
    timestamp_now = datetime.now(timezone.utc).isoformat()

    # --- Define filenames and paths ---
    # Add job_id to filename for uniqueness to avoid overwrites
    data_filename = f"{base_filename}_{job_id[:8]}.csv"
    mcp_filename = f"{data_filename}.mcp.json"

    data_filepath = os.path.join(common_utils.INCOMING_DIR, data_filename)
    mcp_filepath = os.path.join(common_utils.INCOMING_DIR, mcp_filename)

    print(f"--- Starting Upload Simulation for Job ID: {job_id} ---")
    print(f"Department: {department}")
    print(f"Base Filename: {base_filename}")
    print(f"Data file target: {data_filepath}")
    print(f"MCP file target: {mcp_filepath}")

    # --- 1. Create the dummy data file ---
    if not create_dummy_csv(data_filepath, department):
        print("Failed to create dummy data file. Aborting.")
        return # Exit if data file creation fails

    # --- 2. Determine processing directives based on department ---
    if department.lower() == 'sales':
        validation_ruleset = "SALES_LEADS_V1" # Example ruleset ID
        load_target = "sales_leads_table"
        file_type = "leads"
    elif department.lower() == 'marketing':
        validation_ruleset = "MARKETING_CONTACTS_V1"
        load_target = "marketing_contacts_table"
        file_type = "contacts"
    else:
        print(f"Warning: Unknown department '{department}'. Using default directives.")
        validation_ruleset = "DEFAULT_RULES_V1"
        load_target = "generic_landing_table"
        file_type = "unknown"

    # --- 3. Construct the initial MCP data dictionary ---
    mcp_data = {
      "mcp_version": "1.0",
      "job_id": job_id,
      "source_filename_original": f"{base_filename}.csv", # Original hypothetical name
      "current_data_filepath": data_filepath, # Initial location
      "current_mcp_filepath": mcp_filepath,   # Initial location
      "upload_timestamp_utc": timestamp_now,
      "source_context": {
        "department": department,
        "file_type": file_type
      },
      "processing_directives": {
        "validation_ruleset_id": validation_ruleset,
        "load_target_type": "SIMULATED_DB",
        "load_target_destination": load_target
      },
      "status_info": {
        "current_status": "Pending", # Will be updated below
        "status_history": [],       # Will be populated below
        "error_message": None
      }
    }

    # --- 4. Set initial status using common_utils ---
    mcp_data = common_utils.update_status(
        mcp_data=mcp_data,
        new_status="Uploaded",
        actor="create_upload.py",
        details=f"Initial upload for {department}"
    )

    if not mcp_data:
        print("Failed to update MCP status. Aborting.")
        # Consider cleanup? For demo, maybe just exit.
        # os.remove(data_filepath) # Example cleanup
        return

    # --- 5. Write the MCP file using common_utils ---
    if common_utils.write_mcp(mcp_data, mcp_filepath):
        print(f"Successfully created MCP file: {mcp_filepath}")
        print(f"--- Upload Simulation Complete for Job ID: {job_id} ---")
    else:
        print(f"Failed to write MCP file: {mcp_filepath}")
        # Consider cleanup
        # os.remove(data_filepath)

if __name__ == "__main__":
    main()
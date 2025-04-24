# scripts/validate_files.py

import argparse
import csv
import os
import json # <-- Add json import
import common_utils

# --- Define Path to Rules Config ---
CONFIG_DIR = os.path.join(common_utils.BASE_DIR, "config")
RULES_FILE_PATH = os.path.join(CONFIG_DIR, "validation_rules.json")

# --- Load Validation Rules ---
def load_validation_rules(filepath):
    """Loads validation rules from a JSON file."""
    if not os.path.exists(filepath):
        print(f"Error: Validation rules file not found at {filepath}")
        return None
    try:
        with open(filepath, 'r') as f:
            rules = json.load(f)
        print(f"Successfully loaded validation rules from {filepath}")
        return rules
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}")
        return None
    except Exception as e:
        print(f"Error reading validation rules file {filepath}: {e}")
        return None

# Load rules globally when the script starts
VALIDATION_RULES = load_validation_rules(RULES_FILE_PATH)
if VALIDATION_RULES is None:
     # Decide how to handle failure: exit or proceed with empty rules?
     print("CRITICAL: Failed to load validation rules. Exiting.")
     exit(1) # Exit if rules are essential

# --- Validation Function (Now uses the loaded VALIDATION_RULES) ---
def validate_csv_header(data_filepath, ruleset_id):
    """
    Performs simple validation based on required header columns.
    Uses the globally loaded VALIDATION_RULES.
    Returns (is_valid: bool, error_message: str | None)
    """
    if not os.path.exists(data_filepath):
        return False, f"Data file not found at {data_filepath}"

    # Use the globally loaded rules
    rules = VALIDATION_RULES.get(ruleset_id)
    if not rules:
        return False, f"No validation rules found for ruleset_id: {ruleset_id}"

    required_columns = set(rules.get("required_columns", []))
    if not required_columns:
        print(f"Warning: No required columns defined for ruleset {ruleset_id}. Assuming valid.")
        return True, None

    try:
        with open(data_filepath, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader) # Read the first row (header)
            actual_columns = set(col.strip() for col in header) # Normalize whitespace

            missing_columns = required_columns - actual_columns
            if missing_columns:
                return False, f"Missing required columns: {sorted(list(missing_columns))}"
            else:
                return True, None

    except StopIteration:
        return False, "CSV file is empty or has no header row."
    except Exception as e:
        return False, f"Error reading or processing CSV file {data_filepath}: {e}"

def main():
    print("--- Starting Validation Script ---")
    processed_files = 0
    skipped_files = 0
    failed_files = 0
    succeeded_files = 0

    # Scan the incoming directory for MCP files
    print(f"Scanning directory: {common_utils.INCOMING_DIR}")
    for filename in os.listdir(common_utils.INCOMING_DIR):
        if filename.endswith(".mcp.json"):
            mcp_filepath = os.path.join(common_utils.INCOMING_DIR, filename)
            print(f"\nFound potential MCP file: {filename}")

            # 1. Read MCP File
            mcp_data = common_utils.read_mcp(mcp_filepath)
            if not mcp_data:
                print(f"  Skipping file - Could not read MCP data.")
                skipped_files += 1
                continue # Skip to next file

            job_id = mcp_data.get('job_id', 'Unknown Job')
            current_status = mcp_data.get('status_info', {}).get('current_status')

            # 2. Check Status
            if current_status != "Uploaded":
                print(f"  Skipping Job {job_id} - Status is '{current_status}', expected 'Uploaded'.")
                skipped_files += 1
                continue # Skip if not in the expected state

            processed_files += 1
            print(f"  Processing Job {job_id} (Status: {current_status})")

            # 3. Update Status to Validating (Locking)
            mcp_data = common_utils.update_status(mcp_data, "Validating", "validate_files.py")
            if not common_utils.write_mcp(mcp_data, mcp_filepath):
                 print(f"  Error: Failed to update MCP status to Validating for Job {job_id}. Skipping.")
                 # Ideally, revert status in mcp_data if possible, but simple skip for now
                 skipped_files += 1
                 continue

            # 4. Perform Validation
            ruleset_id = mcp_data.get('processing_directives', {}).get('validation_ruleset_id')
            data_filepath = mcp_data.get('current_data_filepath')

            if not ruleset_id or not data_filepath:
                error_msg = "Missing validation_ruleset_id or current_data_filepath in MCP."
                print(f"  Validation Failed for Job {job_id}: {error_msg}")
                is_valid = False
            else:
                print(f"  Validating '{os.path.basename(data_filepath)}' using ruleset '{ruleset_id}'...")
                is_valid, error_msg = validate_csv_header(data_filepath, ruleset_id)

            # 5. Handle Validation Result
            if is_valid:
                print(f"  Validation Successful for Job {job_id}.")
                succeeded_files += 1
                # Update status to Validated
                mcp_data = common_utils.update_status(mcp_data, "Validated", "validate_files.py")
                # Move files to processing_loading
                mcp_data = common_utils.move_job_files(mcp_data, "processing_loading")
                target_dir = common_utils.PROCESSING_LOADING_DIR
                target_status = "Validated"
            else:
                print(f"  Validation Failed for Job {job_id}: {error_msg}")
                failed_files += 1
                # Update status to ValidationFailed
                mcp_data = common_utils.update_status(mcp_data, "ValidationFailed", "validate_files.py", error_message=error_msg)
                # Move files to archive/failed
                mcp_data = common_utils.move_job_files(mcp_data, "archive/failed")
                target_dir = common_utils.ARCHIVE_FAILED_DIR
                target_status = "ValidationFailed"

            # 6. Save Final MCP State after potential move
            if mcp_data: # If move was successful, mcp_data has updated paths
                 final_mcp_path = mcp_data['current_mcp_filepath'] # Use path from updated MCP data
                 if not common_utils.write_mcp(mcp_data, final_mcp_path):
                      print(f"  CRITICAL ERROR: Failed to save final MCP state ({target_status}) for Job {job_id} at {final_mcp_path}")
                      # This is problematic - the file moved but MCP state wasn't saved correctly. Manual intervention needed.
            else:
                 print(f"  CRITICAL ERROR: File move failed for Job {job_id}. MCP not saved in target directory.")
                 # File might still be in incoming, but status is Validating. Rerun might retry or require manual fix.

    print("\n--- Validation Script Finished ---")
    print(f"Summary: Processed={processed_files}, Succeeded={succeeded_files}, Failed={failed_files}, Skipped={skipped_files}")


if __name__ == "__main__":
    main()
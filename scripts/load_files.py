# scripts/load_files.py

import argparse
import os
import time # To simulate work
import random # To simulate random failures
import common_utils # Import our helper functions

def simulate_data_load(data_filepath, target_destination, target_type):
    """
    Simulates loading data to the specified target.
    In a real scenario, this would interact with a database, API, etc.
    Returns (load_successful: bool, error_message: str | None)
    """
    print(f"  Attempting to load data from '{os.path.basename(data_filepath)}'")
    print(f"  Target Type: {target_type}, Target Destination: {target_destination}")

    if not os.path.exists(data_filepath):
        return False, f"Data file not found at {data_filepath}"

    try:
        # Simulate work being done
        print("  Simulating load operation...")
        time.sleep(random.uniform(0.5, 1.5)) # Simulate I/O or processing time

        # If no error occurred
        print(f"  Successfully loaded data to '{target_destination}' (Simulated).")
        return True, None

    except Exception as e:
        error_msg = f"Simulated load failure: {e}"
        print(f"  Error during simulated load: {error_msg}")
        return False, error_msg


def main():
    print("--- Starting Loading Script ---")
    processed_files = 0
    skipped_files = 0
    failed_files = 0
    succeeded_files = 0

    # Scan the processing_loading directory for MCP files
    print(f"Scanning directory: {common_utils.PROCESSING_LOADING_DIR}")
    if not os.path.exists(common_utils.PROCESSING_LOADING_DIR):
         print(f"Directory {common_utils.PROCESSING_LOADING_DIR} does not exist. Nothing to load.")
         print("--- Loading Script Finished ---")
         return

    for filename in os.listdir(common_utils.PROCESSING_LOADING_DIR):
        if filename.endswith(".mcp.json"):
            mcp_filepath = os.path.join(common_utils.PROCESSING_LOADING_DIR, filename)
            print(f"\nFound potential MCP file: {filename}")

            # 1. Read MCP File
            mcp_data = common_utils.read_mcp(mcp_filepath)
            if not mcp_data:
                print(f"  Skipping file - Could not read MCP data.")
                skipped_files += 1
                continue

            job_id = mcp_data.get('job_id', 'Unknown Job')
            current_status = mcp_data.get('status_info', {}).get('current_status')

            # 2. Check Status
            if current_status != "Validated":
                print(f"  Skipping Job {job_id} - Status is '{current_status}', expected 'Validated'.")
                skipped_files += 1
                continue

            processed_files += 1
            print(f"  Processing Job {job_id} (Status: {current_status})")

            # 3. Update Status to Loading (Locking)
            mcp_data = common_utils.update_status(mcp_data, "Loading", "load_files.py")
            if not common_utils.write_mcp(mcp_data, mcp_filepath):
                 print(f"  Error: Failed to update MCP status to Loading for Job {job_id}. Skipping.")
                 skipped_files += 1
                 continue

            # 4. Perform "Loading"
            target_type = mcp_data.get('processing_directives', {}).get('load_target_type')
            target_dest = mcp_data.get('processing_directives', {}).get('load_target_destination')
            data_filepath = mcp_data.get('current_data_filepath')

            if not target_type or not target_dest or not data_filepath:
                error_msg = "Missing load_target_type, load_target_destination, or current_data_filepath in MCP."
                print(f"  Loading Failed for Job {job_id}: {error_msg}")
                load_success = False
            else:
                load_success, error_msg = simulate_data_load(data_filepath, target_dest, target_type)

            # 5. Handle Loading Result
            if load_success:
                print(f"  Load Successful for Job {job_id}.")
                succeeded_files += 1
                # Update status to Loaded
                mcp_data = common_utils.update_status(mcp_data, "Loaded", "load_files.py")
                # Move files to archive/success
                mcp_data = common_utils.move_job_files(mcp_data, "archive/success")
                target_dir_base = "archive/success"
                target_status = "Loaded"
            else:
                print(f"  Load Failed for Job {job_id}: {error_msg}")
                failed_files += 1
                # Update status to LoadFailed
                mcp_data = common_utils.update_status(mcp_data, "LoadFailed", "load_files.py", error_message=error_msg)
                # Move files to archive/failed
                mcp_data = common_utils.move_job_files(mcp_data, "archive/failed")
                target_dir_base = "archive/failed"
                target_status = "LoadFailed"

            # 6. Save Final MCP State after potential move
            if mcp_data: # If move was successful
                 final_mcp_path = mcp_data['current_mcp_filepath']
                 if not common_utils.write_mcp(mcp_data, final_mcp_path):
                      print(f"  CRITICAL ERROR: Failed to save final MCP state ({target_status}) for Job {job_id} at {final_mcp_path}")
            else:
                 print(f"  CRITICAL ERROR: File move failed for Job {job_id} during loading stage. MCP not saved in target directory.")


    print("\n--- Loading Script Finished ---")
    print(f"Summary: Processed={processed_files}, Succeeded={succeeded_files}, Failed={failed_files}, Skipped={skipped_files}")

if __name__ == "__main__":
    main()
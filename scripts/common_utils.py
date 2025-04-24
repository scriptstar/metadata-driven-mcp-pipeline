# scripts/common_utils.py

import json
import os
import shutil # For moving files
import uuid
from datetime import datetime, timezone

# --- Configuration (Paths relative to the script execution location) ---
# Assuming scripts are run from the root 'metadata-driven-mcp-pipeline' directory
BASE_DIR = os.getcwd() # Get current working directory where script is run
INCOMING_DIR = os.path.join(BASE_DIR, "incoming")
PROCESSING_LOADING_DIR = os.path.join(BASE_DIR, "processing_loading")
ARCHIVE_SUCCESS_DIR = os.path.join(BASE_DIR, "archive", "success")
ARCHIVE_FAILED_DIR = os.path.join(BASE_DIR, "archive", "failed")

# --- Helper Functions ---

def get_dir_for_status(status):
    """Maps a status to its corresponding directory path."""
    if status in ["Validating", "Validated", "Loading"]:
        return PROCESSING_LOADING_DIR # Intermediate processing often happens in a specific place
    elif status == "Loaded":
        return ARCHIVE_SUCCESS_DIR
    elif status in ["ValidationFailed", "LoadFailed"]:
        return ARCHIVE_FAILED_DIR
    elif status == "Uploaded":
         return INCOMING_DIR
    else:
        # Default or fallback, might need adjustment based on workflow needs
        # Or raise an error if status is unexpected
        print(f"Warning: Unknown status '{status}', cannot determine target directory reliably.")
        return BASE_DIR # Or perhaps None

def read_mcp(mcp_filepath):
    """Reads and parses the MCP JSON file."""
    if not os.path.exists(mcp_filepath):
        print(f"Error: MCP file not found at {mcp_filepath}")
        return None
    try:
        with open(mcp_filepath, 'r') as f:
            mcp_data = json.load(f)
        return mcp_data
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {mcp_filepath}")
        return None
    except Exception as e:
        print(f"Error reading MCP file {mcp_filepath}: {e}")
        return None

def write_mcp(mcp_data, mcp_filepath):
    """Writes the MCP data dictionary to a JSON file."""
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(mcp_filepath), exist_ok=True)
        with open(mcp_filepath, 'w') as f:
            json.dump(mcp_data, f, indent=2) # Use indent for readability
        return True
    except Exception as e:
        print(f"Error writing MCP file {mcp_filepath}: {e}")
        return False

def update_status(mcp_data, new_status, actor, details=None, error_message=None):
    """
    Updates the status fields within the MCP data dictionary.
    Returns the modified mcp_data dictionary.
    """
    if not mcp_data:
        print("Error: Cannot update status on invalid mcp_data.")
        return None

    timestamp = datetime.now(timezone.utc).isoformat()
    mcp_data['status_info']['current_status'] = new_status

    history_entry = {
        "status": new_status,
        "timestamp_utc": timestamp,
        "actor": actor
    }
    if details:
        history_entry["details"] = details
    if error_message:
        # Update the main error message field as well
        mcp_data['status_info']['error_message'] = error_message
        # Optionally add it to history details too
        history_entry["details"] = f"{details or ''} Error: {error_message}".strip()

    # Initialize history if it doesn't exist (robustness)
    if 'status_history' not in mcp_data['status_info']:
        mcp_data['status_info']['status_history'] = []

    mcp_data['status_info']['status_history'].append(history_entry)

    return mcp_data

def move_job_files(mcp_data, target_dir_basename):
    """
    Moves both the data file and MCP file to the target directory.
    Updates the filepath fields within the MCP data dictionary *before* moving.
    Returns the updated mcp_data dictionary, or None on failure.
    """
    if not mcp_data:
        print("Error: Cannot move files for invalid mcp_data.")
        return None

    current_data_path = mcp_data.get('current_data_filepath')
    current_mcp_path = mcp_data.get('current_mcp_filepath')

    if not current_data_path or not current_mcp_path:
        print("Error: Missing current file paths in MCP data.")
        return None
    if not os.path.exists(current_data_path):
         print(f"Error: Data file not found at {current_data_path} for moving.")
         return None
    if not os.path.exists(current_mcp_path):
        print(f"Error: MCP file not found at {current_mcp_path} for moving.")
        return None

    # Determine target directory full path
    target_dir = os.path.join(BASE_DIR, target_dir_basename)

    # Define new file paths
    data_filename = os.path.basename(current_data_path)
    mcp_filename = os.path.basename(current_mcp_path)
    new_data_path = os.path.join(target_dir, data_filename)
    new_mcp_path = os.path.join(target_dir, mcp_filename)

    # --- Crucial: Update paths in MCP data *before* moving ---
    mcp_data['current_data_filepath'] = new_data_path
    mcp_data['current_mcp_filepath'] = new_mcp_path

    try:
        # Ensure target directory exists
        os.makedirs(target_dir, exist_ok=True)

        # Move the files
        print(f"Moving {current_data_path} to {new_data_path}")
        shutil.move(current_data_path, new_data_path)
        print(f"Moving {current_mcp_path} to {new_mcp_path}")
        shutil.move(current_mcp_path, new_mcp_path)

        return mcp_data # Return the dictionary with updated paths

    except Exception as e:
        print(f"Error moving files for job {mcp_data.get('job_id', 'UNKNOWN')}: {e}")
        # Attempt to rollback MCP path updates? Maybe too complex for demo.
        # For now, just report error and return None.
        mcp_data['current_data_filepath'] = current_data_path # Try reverting paths in dict
        mcp_data['current_mcp_filepath'] = current_mcp_path
        return None
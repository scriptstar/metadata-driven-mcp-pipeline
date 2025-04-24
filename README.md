# Building a Metadata-Driven Pipeline (MCP Concept Demo)

Note: **MCP** stands for **[Model Context Protocol](https://modelcontextprotocol.io/introduction)**

## Overview

This project demonstrates building a simple, **metadata-driven** file processing pipeline in Python. It uses **JSON metadata files** as a form of **context passing**, inspired by the **Model Context Protocol (MCP) concept**, to manage state, define processing steps, and ensure traceability for CSV files moving through validation and simulated loading.

**Note:** The term "Protocol" here refers to an agreed-upon **convention or contract** for structuring and exchanging metadata, not a low-level network or I/O protocol (like HTTP or STDIO or SSE).

## Purpose

The primary goal is to illustrate how passing explicit, structured context alongside data can make data pipelines:

- **More Robust:** Reduces reliance on brittle filename parsing or directory structure conventions.
- **More Traceable:** Provides a clear history and state for each processing job within the metadata itself.
- **More Maintainable:** Decouples processing stages, allowing them to operate based on the provided context rather than implicit assumptions.
- **Easier to Configure:** Centralizes instructions (like which validation rules to apply) within the metadata or associated configuration.

## The MCP Concept in this Project

We implement the MCP concept using **JSON metadata files** (e.g., `my_data.csv.mcp.json`) that accompany each data file (`my_data.csv`). This JSON file acts as a "shipping manifest" or context package containing vital information about the data file and the processing job.

**Key elements within our `.mcp.json` file:**

- `job_id`: A unique identifier for the entire processing run of a specific file.
- `source_context`: Information about the origin (e.g., department).
- `processing_directives`: Instructions for subsequent stages (e.g., `validation_ruleset_id`, `load_target_destination`).
- `status_info`: Tracks the current state (`current_status`) and history (`status_history`) of the job.
- `current_*_filepath`: Explicitly tracks the current location of the data and MCP files as they move through the workflow.

```json
// Example MCP Structure (abbreviated)
{
  "mcp_version": "1.0",
  "job_id": "<uuid>",
  "source_filename_original": "<original_filename.csv>",
  "current_data_filepath": "<full_path_to_data_file>",
  "current_mcp_filepath": "<full_path_to_mcp_file>",
  "upload_timestamp_utc": "<iso_timestamp>",
  "source_context": { "department": "Sales", "file_type": "leads" },
  "processing_directives": {
    "validation_ruleset_id": "SALES_LEADS_V1",
    "load_target_type": "SIMULATED_DB",
    "load_target_destination": "sales_leads_table"
  },
  "status_info": {
    "current_status": "Uploaded", // Changes as workflow progresses
    "status_history": [
      /* List of status change events */
    ],
    "error_message": null // Populated on failure
  }
}
```

## Features Demonstrated

- **Context-Driven Processing:** Validation logic selects rules based on `validation_ruleset_id` from the MCP. Loading logic uses `load_target_destination`.

- **State Management:** Workflow progress is tracked via the `current_status` field in the MCP and the file's location (moving between `incoming/`, `processing_loading/`, `archive/success/`, `archive/failed/`).

- **Traceability:** The `status_history` array within the MCP provides an audit trail of the job's lifecycle. Error messages are captured on failure.

- **Failure Handling:** Demonstrates distinct paths for validation failures vs. successful processing.

- **Configuration:** Validation rules are externalized into a JSON configuration file (`config/validation_rules.json`).

- **Separation of Concerns:** Utility functions (`common_utils.py`), data generation (`data_generator.py`), configuration (`config/`), and stage logic (`validate_files.py`, `load_files.py`) are separated.

- **Basic Locking:** Updating the MCP status before processing acts as a simple mechanism to prevent accidental reprocessing by concurrent script runs (though this is not a fully robust locking solution for high-concurrency systems).

## Project Structure

```bash
metadata-driven-mcp-pipeline/
├── archive/                # Final resting place for processed files
│   ├── failed/             # Files that failed validation or loading
│   └── success/            # Files that were successfully processed and loaded
├── config/                 # Configuration files
│   └── validation_rules.json # Defines required CSV columns for different rulesets
├── incoming/               # Landing zone for newly "uploaded" files (data + MCP)
├── processing_loading/     # Staging area for files that passed validation, awaiting loading
└── scripts/                # Python scripts driving the workflow
    ├── __init__.py         # Makes 'scripts' a Python package
    ├── common_utils.py     # Helper functions for MCP I/O, status updates, file moves
    ├── create_upload.py    # Simulates file upload, creates initial data + MCP
    ├── data_generator.py   # Generates dummy CSV data files
    ├── load_files.py       # Performs the (simulated) loading stage
    └── validate_files.py   # Performs the validation stage
```

## Prerequisites

- Python 3.x (Developed with Python 3.10, but should work on most Python 3 versions)
- No external libraries are required (uses only standard Python libraries like `os`, `json`, `shutil`, `uuid`, `datetime`, `csv`, `argparse`).

## Setup

1. Clone this repository or download the source code.
2. Navigate into the project's root directory (`metadata-driven-mcp-pipeline/`) in your terminal.

## How to Run the Workflow

Execute the scripts sequentially from the project's root directory.

**1. Clean Up Previous Runs (Optional but Recommended):**

Ensure the `incoming`, `processing_loading`, `archive/success`, and `archive/failed` directories are empty before starting a new run.

```bash
rm -rf incoming/* processing_loading/* archive/success/* archive/failed/*
```

> (Use with caution! Ensure you are in the correct directory.)

**2. Simulate File Uploads:**

Create some sample data and MCP files in the `incoming/` directory.

```bash
# Example: Create a Sales file
python scripts/create_upload.py --department Sales --filename sales_report

# Example: Create a Marketing file
python scripts/create_upload.py --department Marketing --filename campaign_leads

# Example: Create a Finance file (will fail validation based on current rules)
python scripts/create_upload.py --department Finance --filename fy24_budget
```

> Observe: Check the `incoming/` directory. You should see pairs of `.csv` and `.csv.mcp.json` files.

**3. Run the Validation Stage:**

This script processes files in `incoming/` with status Uploaded.

```bash
python scripts/validate_files.py
```

> Observe: Files passing validation move to `processing_loading/` (MCP status -> _Validated_). Files failing validation move to `archive/failed/` (MCP status -> _ValidationFailed_). Check the script output and the directories.

**4. Run the Loading Stage:**

This script processes files in `processing_loading/` with status Validated.

```bash
python scripts/load_files.py
```

> Observe: Files are "loaded" (simulated). Successful ones move to `archive/success/` (MCP status -> _Loaded_). The `processing_loading/` directory should become empty. Check the script output and final file locations.

## Workflow Explained

**1. Upload (`create_upload.py`):**

- A data file (`.csv`) and its corresponding metadata file (`.mcp.json`) are created in the incoming/ directory.

- The MCP file is populated with a unique `job_id`, source context, processing directives (based on the source, e.g., department), and initial file paths.

- The initial status is set to Uploaded.

**2. Validation (`validate_files.py`):**

- Scans `incoming/` for MCP files with `current_status == "Uploaded"`.
  Reads the MCP.

- Updates the status to `Validating` (in place) to prevent reprocessing.
  Retrieves the `validation_ruleset_id` from the MCP.

- Loads validation rules from `config/validation_rules.json`.

- Performs validation (in this example, checking required CSV headers).

- If **valid**: Updates MCP status to `Validated`, moves both files to `processing_loading/`, updates file paths in MCP, and saves the final MCP in the new location.

- If **invalid**: Updates MCP status to `ValidationFailed` (with error message), moves both files to `archive/failed/`, updates file paths in MCP, and saves the final MCP there.

**3. Loading (`load_files.py`):**

- Scans `processing_loading/` for MCP files with `current_status == "Validated"`.

- Reads the MCP.

- Updates status to Loading (in place).

- Retrieves `load_target_destination` and `load_target_type` from the MCP.

- Performs simulated loading.

- If successful: Updates MCP status to `Loaded`, moves both files to `archive/success/`, updates file paths in MCP, and saves the final MCP.

- If failed (less likely now, but handles errors): Updates MCP status to `LoadFailed` (with error), moves both files to `archive/failed/`, updates file paths in MCP, and saves the final MCP.

## Configuration

The primary configuration point is the validation rules file:

- **config/validation_rules.json:** Defines the required CSV header columns for different `validation_ruleset_id` values referenced in the MCP files. You can modify this file to change validation requirements without altering Python code. Add new ruleset IDs or adjust the `required_columns` for existing ones.

## Potential Next Steps

This is a basic demonstration. It could be extended in many ways:

1. **More Complex Validation:** Use JSON Schema for MCP structure, check data types/formats in CSV rows, integrate libraries like Pandera or Great Expectations.

2. **Real Loading:** Replace `simulate_data_load` with code interacting with SQLite, PostgreSQL, or writing to different output file formats/locations based on MCP directives.

3. **Enhanced Configuration:** Load database connection details or other parameters from environment variables or a dedicated config file.

4. **Robust Error Handling & Retries:** Implement more sophisticated error handling and retry mechanisms within the scripts or via an orchestrator.

5. **Parallelism:** Modify scripts (e.g., using `multiprocessing`) or use a task queue to process multiple files concurrently.

6. **Orchestration:** Integrate the scripts as tasks within a workflow orchestrator (Airflow, Prefect, Dagster) for automated scheduling, dependency management, and monitoring.

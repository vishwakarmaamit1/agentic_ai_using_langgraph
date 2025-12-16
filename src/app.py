import pickle
import json
import os
from fastapi import FastAPI, BackgroundTasks, HTTPException
from typing import Dict, Any
from pydantic import BaseModel, Field
from src.bigquery.create_pickle_file_from_csv import load_metadata_from_pickle, upload_existing_pickles

# --- CONFIGURATION (Must match your environment and previous scripts) ---

# Project & Dataset Info (Used for GCS path structure)
PROJECT_ID = "reflected-radio-438310-s1"
DATASET_ID = "retail_analytics_db"

# Local Directory Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Local path where pickle files are stored: src/bigquery/bigquery_metadata_pickles
PICKLE_DIR_RELATIVE = "bigquery/bigquery_metadata_pickles"
PICKLE_DIR = os.path.join(BASE_DIR, PICKLE_DIR_RELATIVE)

# GCS Configuration
GCS_BUCKET_NAME = "retail_analytics_bucket"
# Desired GCS folder structure: reflected-radio-438310-s1/retail_analytics_db/
GCS_BASE_PREFIX = f"{PROJECT_ID}/{DATASET_ID}/"

DEFAULT_BASE_FOLDER = "bigquery" # The 'bigquery' folder you mentioned

app = FastAPI(
    title="BigQuery Metadata API",
    description="API to serve BigQuery table schema, business context, and sample data from generated pickle files."
)

# --- FASTAPI ENDPOINT (Unchanged) ---

@app.get("/metadata/{table_id}", response_model=Dict[str, Any])
def get_table_metadata(table_id: str):
    """
    Retrieves the structured metadata (schema, context, and sample data)
    for a specific BigQuery table ID.
    """
    # If load_metadata_from_pickle succeeds, it returns a Dict.
    # If it fails, it raises an HTTPException, which FastAPI catches and handles.
    return load_metadata_from_pickle(table_id)


@app.post("/upload-pickles", status_code=202)
def upload_existing_files_to_gcs(background_tasks: BackgroundTasks):
    """
    Reads all existing .pkl files from the local metadata directory and uploads them
    to the configured GCS bucket with the PROJECT/DATASET path structure.
    The task is executed asynchronously in the background.
    """

    # 1. Check if the directory exists before starting the background task
    if not os.path.exists(PICKLE_DIR):
        raise HTTPException(
            status_code=404,
            detail=f"Local pickle directory not found. Please ensure files exist in: {PICKLE_DIR}"
        )

    # 2. Add the upload function to be executed in the background
    background_tasks.add_task(
        upload_existing_pickles,
        PICKLE_DIR,
        GCS_BUCKET_NAME,
        GCS_BASE_PREFIX
    )

    return {
        "status": "Upload started in background",
        "message": f"All files from {PICKLE_DIR_RELATIVE} will be uploaded to gs://{GCS_BUCKET_NAME}/{GCS_BASE_PREFIX}",
        "local_directory": PICKLE_DIR_RELATIVE,
        "gcs_destination": f"gs://{GCS_BUCKET_NAME}/{GCS_BASE_PREFIX}"
    }


# --- 1. PYDANTIC MODEL FOR JSON PAYLOAD ---
class UploadPayload(BaseModel):
    """Defines the structure and validation for the incoming JSON request body."""
    subfolder_name: str
    gcs_bucket_name: str
    gcs_folder_name: str
    # Use Field with default for the optional parameter
    base_folder_name: str = Field(default=DEFAULT_BASE_FOLDER)

@app.post("/upload-files-gcp-bucket", status_code=202)
def upload_subfolder(
    # --- 2. ACCEPT THE PYDANTIC MODEL AS THE REQUEST BODY ---
    payload: UploadPayload,
    background_tasks: BackgroundTasks
):
    """
    Reads all existing .pkl files from the local metadata directory and uploads them
    to the configured GCS bucket with the PROJECT/DATASET path structure.
    The task is executed asynchronously in the background.
    """
    # 3. Access parameters via the payload object
    base_folder_name = payload.base_folder_name
    subfolder_name = payload.subfolder_name

    # Construct the absolute local path to the source directory
    local_source_dir = os.path.join(BASE_DIR, base_folder_name, subfolder_name)

    # 1. Check if the directory exists before starting the background task
    if not os.path.exists(local_source_dir):
        raise HTTPException(
            status_code=404,
            detail=f"Local pickle directory not found. Please ensure files exist in: {PICKLE_DIR}"
        )

    # 2. Add the upload function to be executed in the background
    background_tasks.add_task(
        upload_existing_pickles,
        local_source_dir,
        GCS_BUCKET_NAME,
        payload.gcs_folder_name
    )

    return {
        "status": "Upload started in background",
        "message": f"All files from {base_folder_name}/{subfolder_name} will be uploaded to gs://{GCS_BUCKET_NAME}/{payload.gcs_folder_name}",
        "local_directory": f"{base_folder_name}/{subfolder_name}",
        "gcs_destination": f"gs://{GCS_BUCKET_NAME}/{payload.gcs_folder_name}"
    }
# --- RUNNING THE APPLICATION (Unchanged) ---
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
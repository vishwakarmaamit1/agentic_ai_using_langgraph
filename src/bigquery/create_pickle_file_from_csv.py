import config
import os
import csv
import pickle
import json
import pandas as pd
import datetime  # <-- NEW: Import datetime
from google.cloud import bigquery
from typing import List, Dict, Any
from fastapi import HTTPException

from src.bucket.read_bucket_file import upload_file_to_gcs

# --- CONFIGURATION (unchanged) ---
PROJECT_ID = "reflected-radio-438310-s1"
DATASET_ID = "retail_analytics_db"
INPUT_DIR_CSV = "bigquery_schemas_with_business_ctx_csv"
OUTPUT_DIR_PICKLE = "bigquery_metadata_pickles"
SAMPLE_LIMIT = 2
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PICKLE_DIR_RELATIVE = "bigquery/bigquery_metadata_pickles"
PICKLE_DIR = os.path.join(BASE_DIR, PICKLE_DIR_RELATIVE)

# --- NEW: Custom JSON Serializer ---
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    # Handle datetime, date, and NaT types
    if isinstance(obj, (datetime.datetime, datetime.date, pd.NaT)):
        # Convert date and datetime objects to ISO format string
        return obj.isoformat()

    # Handle other non-serializable Pandas types (e.g., NaNs in non-float columns)
    if pd.isna(obj):
        return None

    # Raise the standard error for everything else
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


# --- CORE LOGIC FUNCTIONS ---

# (The get_sample_data function remains the same as the previous correct version,
# as the fix is now focused on the serialization step)
def get_sample_data(project_id: str, dataset_id: str, table_id: str, limit: int = 5) -> List[Dict]:
    """
    Fetches a sample of data, converts complex types directly to strings
    (though this step is largely made redundant by the custom JSON serializer),
    and returns a list of dictionaries.
    """
    full_table_ref = f"`{project_id}.{dataset_id}.{table_id}`"

    try:
        client = bigquery.Client(project=project_id)

        # 1. Get column names and prepare the WHERE clause (using relaxed conditions)
        table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        column_names = [field.name for field in table.schema]

        not_null_conditions = [f"`{col}` IS NOT NULL" for col in column_names]
        where_clause = " AND ".join(not_null_conditions)

        # critical_ids = ['sale_id', 'product_id', 'vendor_id', 'date_id']
        # existing_critical_ids = [col for col in critical_ids if col in column_names]
        #
        # if existing_critical_ids:
        #     not_null_conditions = [f"`{col}` IS NOT NULL" for col in existing_critical_ids]
        #     where_clause = " AND ".join(not_null_conditions)
        #     sql_query = f"SELECT * FROM {full_table_ref} WHERE {where_clause} LIMIT {limit}"
        # else:
        #     sql_query = f"SELECT * FROM {full_table_ref} LIMIT {limit}"

        sql_query = f"SELECT * FROM {full_table_ref} WHERE {where_clause} LIMIT {limit}"
        print(f"  -> Executing query: {sql_query}")

        df = client.query(sql_query).to_dataframe()

        # 2. Cleanup Step: Convert datetimes to strings for safety, though the custom serializer is key.
        for col in df.columns:
            dtype = df[col].dtype

            # Convert any datetime or timezone-aware types to standard strings (ISO format)
            if pd.api.types.is_datetime64_any_dtype(dtype):
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

            elif pd.api.types.is_timedelta64_dtype(dtype):
                df[col] = df[col].astype(str)

            elif dtype == 'object':
                df[col] = df[col].fillna('').astype(str)

            # Convert NaT/NaN to None
            df[col] = df[col].mask(pd.isna(df[col]), None)

        # 3. Convert the cleaned DataFrame rows to a list of dictionaries
        sample_data = df.to_dict('records')

        return sample_data

    except Exception as e:
        print(f"  ❌ Error fetching sample data for {table_id}: {e}")
        return []


def create_pickle_structure(csv_filepath: str) -> Dict[str, Any]:
    # ... (read CSV logic remains the same) ...
    # ... (logic to get table_id, full_table_name, sample_values, and columns_structure remains the same) ...

    # [Placeholder for brevity - keep your existing CSV reading and column structuring logic here]
    schema_rows = []
    try:
        with open(csv_filepath, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            schema_rows = list(reader)
    except Exception as e:
        return {"filename": os.path.basename(csv_filepath), "data": None, "success": False,
                "error": f"Failed to read CSV: {e}"}

    filename = os.path.basename(csv_filepath)
    table_id = filename.replace(".csv", "")

    full_table_name = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    sample_values = get_sample_data(PROJECT_ID, DATASET_ID, table_id, SAMPLE_LIMIT)

    columns_structure = {}
    for row in schema_rows:
        columns_structure[row['name']] = {
            "data_type": "STRING",
            "description": row.get('description', ''),
            "business_context": row.get('business_context', '')
        }

    # NEW DATA STRUCTURE
    data_content = {
        "columns": columns_structure,
        "sample_values": sample_values
    }

    # NEW FINAL DICTIONARY STRUCTURE (Stored in the pickle file)
    final_pickle_dict = {
        "success": True,
        "filename": full_table_name,
        "data": {
            full_table_name: data_content
        }
    }

    # 🚩 THE CRITICAL FIX IS HERE
    try:
        # Use the custom json_serial function to handle date and datetime objects
        json_string = json.dumps(
            final_pickle_dict,
            indent=4,
            default=json_serial  # <-- PASS THE CUSTOM SERIALIZER
        )
    except Exception as e:
        return {
            "filename": filename,
            "data": None,
            "success": False,
            "error": f"JSON serialization error: {e}"
        }

    return {
        "filename": filename,
        "data": json_string,  # Store the successfully serialized JSON string
        "success": True
    }


def export_metadata_to_pickle(input_dir_csv: str, output_dir_pickle: str):
    # ... (This function remains unchanged) ...
    print(f"Starting pickle generation from CSV files in: {input_dir_csv}")

    os.makedirs(output_dir_pickle, exist_ok=True)
    processed_count = 0

    for filename in os.listdir(input_dir_csv):
        if filename.endswith(".csv"):
            csv_filepath = os.path.join(input_dir_csv, filename)
            pickle_filename = filename.replace(".csv", ".pkl")
            pickle_filepath = os.path.join(output_dir_pickle, pickle_filename)

            print(f"  Processing {filename}...")

            pickle_data = create_pickle_structure(csv_filepath)

            if pickle_data["success"]:
                try:
                    with open(pickle_filepath, 'wb') as f:
                        pickle.dump(pickle_data, f)

                    print(f"  ✅ Successfully saved pickle file: {pickle_filepath}")
                    processed_count += 1
                except Exception as e:
                    print(f"  ❌ Failed to write pickle file {pickle_filepath}: {e}")
            else:
                print(f"  ❌ Skipping {filename} due to an error: {pickle_data.get('error', 'Unknown error')}")

    print("-" * 50)
    print(f"Pickle generation complete. Total files processed: {processed_count}")


# --- UTILITY FUNCTION WITH ROBUST ERROR HANDLING ---

def load_metadata_from_pickle(table_id: str) -> Dict[str, Any]:
    """
    Constructs the expected filename, loads the pickle file, and parses the
    internal JSON string data, raising HTTPExceptions on failure.
    """
    pickle_filename = f"{table_id}.pkl"
    pickle_path = os.path.join(PICKLE_DIR, pickle_filename)

    # 1. Path Check (Handles 404)
    if not os.path.exists(pickle_path):
        raise HTTPException(
            status_code=404,
            detail=f"Metadata file not found for table: {table_id}. Looked in: {PICKLE_DIR}"
        )

    try:
        # 2. Load Pickle Data
        with open(pickle_path, 'rb') as f:
            pickled_data = pickle.load(f)

        # 3. Check Internal Success Flag
        if not pickled_data.get("success"):
            raise HTTPException(
                status_code=500,
                detail=f"Pickle file for {table_id} indicates a previous failure. Error: {pickled_data.get('error', 'No error detail available')}"
            )

        # 4. Decode JSON String
        metadata_json_string = pickled_data.get("data")
        # outer_metadata_dict = {"success": true, "filename": "...", "data": {...}}
        outer_metadata_dict = json.loads(metadata_json_string)

        # 5. Navigate New Nested Structure
        full_table_name = outer_metadata_dict.get("filename")
        data_content = outer_metadata_dict.get("data", {}).get(full_table_name, {})

        # 6. Return Structured Response
        return {
            "success": outer_metadata_dict.get("success"),
            "filename": full_table_name,
            "data": {
                full_table_name: data_content
            }
        }

    except pickle.UnpicklingError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unpickle file for {table_id}. File might be corrupted. Error: {e}"
        )
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to decode internal JSON string in pickle file for {table_id}. Error: {e}"
        )
    except Exception as e:
        # Catch all other exceptions (permissions, corruption, internal logic errors)
        # 🚩 CRITICAL: Ensure we raise HTTPException for EVERY failure path
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected internal error occurred while processing {table_id}: {e.__class__.__name__}: {e}"
        )


# --- ORCHESTRATION LOGIC ---

def upload_existing_pickles(local_pickle_dir: str, gcs_bucket: str, gcs_prefix: str) -> List[Dict]:
    """
    Reads all .pkl files from the local directory and uploads them to GCS.
    """
    if not os.path.exists(local_pickle_dir):
        raise FileNotFoundError(f"Local pickle directory not found: {local_pickle_dir}")

    uploaded_files = []
    ALLOWED_EXTENSIONS = ('.pkl', '.txt', '.csv')
    # Iterate through locally generated Pickle files
    for filename in os.listdir(local_pickle_dir):
        if filename.endswith(ALLOWED_EXTENSIONS):
            local_file_path = os.path.join(local_pickle_dir, filename)

            # Construct GCS destination path: prefix/filename.pkl
            gcs_blob_name = f"{gcs_prefix}/{filename}"

            try:
                upload_url = upload_file_to_gcs(
                    local_file_path,
                    gcs_bucket,
                    gcs_blob_name
                )
                uploaded_files.append({"file": filename, "gcs_url": upload_url})
            except Exception as e:
                print(f"  ❌ GCS Upload Failed for {filename}. Error: {e}")
                uploaded_files.append({"file": filename, "gcs_url": None, "error": str(e)})

    return uploaded_files


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    export_metadata_to_pickle(INPUT_DIR_CSV, OUTPUT_DIR_PICKLE)
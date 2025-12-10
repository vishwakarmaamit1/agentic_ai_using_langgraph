import config
import os
import csv
import pickle
import json
import pandas as pd
import datetime  # <-- NEW: Import datetime
from google.cloud import bigquery
from typing import List, Dict, Any

# --- CONFIGURATION (unchanged) ---
PROJECT_ID = "reflected-radio-438310-s1"
DATASET_ID = "retail_analytics_db"
INPUT_DIR_CSV = "bigquery_schemas_with_business_ctx_csv"
OUTPUT_DIR_PICKLE = "bigquery_metadata_pickles"
SAMPLE_LIMIT = 2


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


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    export_metadata_to_pickle(INPUT_DIR_CSV, OUTPUT_DIR_PICKLE)
import pickle
import json
import os
from fastapi import FastAPI, HTTPException
from typing import Dict, Any

# --- CONFIGURATION (Ensure this is correct for your environment) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PICKLE_DIR_RELATIVE = "bigquery/bigquery_metadata_pickles"
PICKLE_DIR = os.path.join(BASE_DIR, PICKLE_DIR_RELATIVE)

app = FastAPI(
    title="BigQuery Metadata API",
    description="API to serve BigQuery table schema, business context, and sample data from generated pickle files."
)


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


# --- RUNNING THE APPLICATION (Unchanged) ---

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
import config
import os
from google.cloud import bigquery
from google import genai
from google.genai.errors import APIError
from typing import List, Dict

# --- CONFIGURATION ---
PROJECT_ID = "reflected-radio-438310-s1"  # Replace with your GCP Project ID
DATASET_ID = "retail_analytics_db"  # Replace with your BigQuery Dataset ID
TABLE_ID = "dimension_business_group"  # Replace with your BigQuery Table ID
MODEL_NAME = "gemini-2.5-flash"  # Use a fast model for this task

INPUT_FILENAME = f"{TABLE_ID}_schema_raw.txt"
OUTPUT_FILENAME = f"{TABLE_ID}_schema_described.txt"

OUTPUT_DIR = "bigquery_schemas"


def get_table_schema_fields(client: bigquery.Client, dataset_id: str, table_id: str) -> List[Dict]:
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        schema_fields = []
        for field in table.schema:
            # We use the existing description if it exists, otherwise a placeholder
            description = field.description if field.description else "[REQUIRES DESCRIPTION]"
            schema_fields.append({
                "name": field.name,
                "data_type": field.field_type,
                "mode": field.mode,
                "description": description
            })
        return schema_fields
    except Exception as e:
        print(f"Error fetching schema for {dataset_id}.{table_id}: {e}")
        return []


def format_schema_for_file(dataset_id: str, table_id: str, schema: List[Dict]) -> str:
    header = f"--- BigQuery Table Schema: {dataset_id}.{table_id} ---\n\n"
    field_lines = []

    if not schema:
        return header + "SCHEMA NOT FOUND OR ERROR OCCURRED."

    for field in schema:
        line = (
            f"Field Name: {field['name']}\n"
            f"Data Type: {field['data_type']}\n"
            f"Mode: {field['mode']}\n"
            f"Description: {field['description']}\n"
            f"{'-' * 30}\n"
        )
        field_lines.append(line)

    return header + "".join(field_lines)


def export_dataset_schemas_to_files(project_id: str, dataset_id: str, output_dir: str):
    print(f"Connecting to BigQuery and preparing to export schemas from {dataset_id}...")

    # 1. Initialize Client and Output Directory
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return

    os.makedirs(output_dir, exist_ok=True)
    dataset_ref = client.dataset(dataset_id)

    table_count = 0
    exported_count = 0

    try:
        # 2. List all tables in the dataset
        tables_iterator = client.list_tables(dataset_ref)

        for table in tables_iterator:
            table_count += 1
            table_id = table.table_id

            # 3. Fetch the schema for the current table
            print(f"  Processing schema for table: {table_id}...")
            schema_data = get_table_schema_fields(client, dataset_id, table_id)

            if schema_data:
                # 4. Format the schema content
                file_content = format_schema_for_file(dataset_id, table_id, schema_data)

                # 5. Write the content to a dedicated file
                file_name = f"{table_id}_schema.txt"
                output_path = os.path.join(output_dir, file_name)

                with open(output_path, "w") as f:
                    f.write(file_content)

                print(f"    -> Schema saved to {output_path}")
                exported_count += 1
            else:
                print(f"    -> Skipping {table_id} due to fetch error or empty schema.")

    except Exception as e:
        print(f"\nFATAL ERROR during table listing: {e}")

    print("-" * 50)
    print(f"Schema export complete.")
    print(f"Total tables found: {table_count}")
    print(f"Total files exported: {exported_count} to the '{output_dir}/' directory.")

def generate_descriptions_with_gemini(schema_text: str) -> str:
    """Uses the Gemini model to generate enhanced descriptions for the schema."""
    print(f"\nCalling Gemini API ({MODEL_NAME}) to generate descriptions...")

    # 1. Initialize the client
    # The client automatically picks up the GEMINI_API_KEY environment variable.
    try:
        client = genai.Client()
    except Exception as e:
        print("Error initializing Gemini client. Ensure GEMINI_API_KEY is set.")
        print(e)
        return schema_text  # Return the original schema if client fails to init

    # 2. Define the prompt
    # The prompt instructs the model to only fill in the descriptions.
    prompt = f"""
    You are an expert data analyst. Review the BigQuery table schema provided below.
    Your task is to replace the placeholder text "[REQUIRES DESCRIPTION]" with a concise,
    clear, and accurate description for each field, based on typical data warehousing best practices.
    Keep the existing structure exactly the same, only modifying the text after "Description:".
    If a field already has a description, you may enhance it, but do not delete it.

    --- SCHEMA TO DESCRIBE ---
    {schema_text}
    --- END OF SCHEMA ---

    Please return ONLY the final, modified schema text.
    """

    # 3. Generate the content
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={"temperature": 0.2}  # Use a lower temperature for factual tasks
        )
        print("Successfully received description from Gemini.")
        return response.text.strip()

    except APIError as e:
        print(f"Gemini API Error: {e}")
        print("Returning the original schema without AI descriptions.")
        return schema_text
    except Exception as e:
        print(f"An unexpected error occurred during Gemini call: {e}")
        return schema_text


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # 1. Fetch the Schema from BigQuery
    # bq_schema = get_bigquery_schema(PROJECT_ID, DATASET_ID, TABLE_ID)

    export_dataset_schemas_to_files(PROJECT_ID, DATASET_ID, OUTPUT_DIR)

    # if not bq_schema:
    #     print("Could not retrieve schema. Exiting.")
    # else:
    #     # 2. Format and Save the Initial (Raw) Schema
    #     raw_schema_content = format_schema_for_file(bq_schema)
    #     write_to_file(INPUT_FILENAME, raw_schema_content)
    #
    #     # 3. Generate Descriptions using Gemini
    #     # We pass the formatted string content to the model
    #     described_schema_content = generate_descriptions_with_gemini(raw_schema_content)
    #
    #     # 4. Save the Final (Described) Schema
    #     write_to_file(OUTPUT_FILENAME, described_schema_content)
    #
    #     print("\n✅ Process complete. Check the generated files for results.")
import config
import os
import csv
from google.cloud import bigquery
from google import genai
from google.genai.errors import APIError
from typing import List, Dict

# --- CONFIGURATION ---
PROJECT_ID = "reflected-radio-438310-s1"  # Replace with your GCP Project ID
DATASET_ID = "retail_analytics_db"  # The dataset containing all the tables you want to process
MODEL_NAME = "gemini-2.5-flash"

OUTPUT_DIR = "bigquery_schemas"


# --- CORE LOGIC FUNCTIONS ---

def get_table_schema_fields(client: bigquery.Client, dataset_id: str, table_id: str) -> List[Dict]:
    """Fetches the schema fields (name and description) for a single BigQuery table."""
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        schema_fields = []
        for field in table.schema:
            description = field.description if field.description else "[REQUIRES DESCRIPTION]"
            schema_fields.append({
                "name": field.name,
                "data_type": field.field_type,  # Keep data type for context in the prompt
                "description": description
            })
        return schema_fields
    except Exception as e:
        print(f"Error fetching schema for {dataset_id}.{table_id}: {e}")
        return []


def format_schema_for_gemini(dataset_id: str, table_id: str, schema: List[Dict]) -> str:
    """Formats the schema into a string that Gemini can easily parse and modify."""
    header = f"--- BigQuery Table Schema: {dataset_id}.{table_id} ---\n\n"
    field_lines = []

    if not schema:
        return header + "SCHEMA NOT FOUND OR ERROR OCCURRED."

    for field in schema:
        line = (
            f"Field Name: {field['name']}\n"
            f"Data Type: {field['data_type']}\n"
            f"Description: {field['description']}\n"
            f"{'-' * 30}\n"
        )
        field_lines.append(line)

    return header + "".join(field_lines)


def generate_descriptions_with_gemini(schema_text: str) -> str:
    """Uses the Gemini model to generate enhanced descriptions for the schema."""
    print(f"\nCalling Gemini API ({MODEL_NAME}) to generate descriptions...")

    try:
        client = genai.Client()
    except Exception as e:
        print("Error initializing Gemini client. Ensure GEMINI_API_KEY is set.")
        print(e)
        return schema_text

    prompt = f"""
    You are an expert data analyst. Review the BigQuery table schema provided below.
    Your task is to replace the placeholder text "[REQUIRES DESCRIPTION]" with a concise,
    clear, and accurate description for each field, based on typical data warehousing best practices.
    Keep the existing structure exactly the same, only modifying the text after "Description:".
    Do not add or remove any fields.

    --- SCHEMA TO DESCRIBE ---
    {schema_text}
    --- END OF SCHEMA ---

    Please return ONLY the final, modified schema text, ensuring the original Field Name and Data Type lines are preserved.
    """

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={"temperature": 0.2}
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


def parse_ai_response_and_merge(ai_text: str, original_schema: List[Dict]) -> List[Dict]:
    """
    Parses the text output from Gemini to extract the updated descriptions,
    and merges them back into a structured list containing only 'name' and 'description'.
    """
    name_to_description = {}
    current_name = None

    for line in ai_text.split('\n'):
        line = line.strip()
        if line.startswith("Field Name: "):
            current_name = line.replace("Field Name: ", "").strip()
        elif line.startswith("Description: ") and current_name:
            new_description = line.replace("Description: ", "").strip()
            name_to_description[current_name] = new_description
            current_name = None

    # Create the final list containing only the required columns
    final_output = []
    for field in original_schema:
        name = field['name']
        final_output.append({
            "column_name": name,
            "description": name_to_description.get(name, field['description'])
        })

    return final_output


def write_schema_to_csv(output_path: str, schema_data: List[Dict]) -> bool:
    """Writes a list of schema dictionaries (name and description) to a CSV file."""
    # Define the column headers (must match the keys in the dictionary)
    fieldnames = ["column_name", "description"]

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(schema_data)

        return True
    except Exception as e:
        print(f"Error writing CSV to {output_path}: {e}")
        return False


# --- MAIN EXPORT LOGIC (Iterates over all tables) ---

def export_all_dataset_schemas_to_csv(project_id: str, dataset_id: str, output_dir: str):
    print(f"Connecting to BigQuery and preparing to export schemas from all tables in {dataset_id}...")

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

        # 3. Process each table individually
        for table in tables_iterator:
            table_count += 1
            table_id = table.table_id
            print("-" * 50)
            print(f"  Processing table {table_count}: {table_id}...")

            # A. Fetch the schema (Structured data)
            schema_data = get_table_schema_fields(client, dataset_id, table_id)

            if schema_data:
                # B. Format the schema content for Gemini (String for LLM)
                gemini_input_content = format_schema_for_gemini(dataset_id, table_id, schema_data)

                # C. Generate AI-enhanced schema with Gemini (String output)
                ai_description_text = generate_descriptions_with_gemini(gemini_input_content)

                # D. Parse the AI text back into structured data and filter to name/description
                final_structured_schema = parse_ai_response_and_merge(ai_description_text, schema_data)

                # E. Write the final structured data to a CSV file
                ai_file_name = f"{table_id}.csv"
                ai_output_path = os.path.join(output_dir, ai_file_name)

                success = write_schema_to_csv(ai_output_path, final_structured_schema)

                if success:
                    print(f"    -> AI-enhanced schema saved successfully to {ai_output_path}")
                    exported_count += 1
                else:
                    print(f"    -> Failed to save CSV for {table_id}.")

            else:
                print(f"    -> Skipping {table_id} due to fetch error or empty schema.")

    except Exception as e:
        print(f"\nFATAL ERROR during table listing: {e}")

    print("-" * 50)
    print(f"Schema export complete.")
    print(f"Total tables found: {table_count}")
    print(f"Total files exported: {exported_count} to the '{output_dir}/' directory.")
    print(f"✅ Process Complete. AI-enhanced schema saved to: {output_dir}")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Note: Removed the unused TABLE_ID configuration variable
    export_all_dataset_schemas_to_csv(PROJECT_ID, DATASET_ID, OUTPUT_DIR)
import config
import os
import csv
import re
from google.cloud import bigquery
from google import genai
from google.genai.errors import APIError
from typing import List, Dict

# --- CONFIGURATION ---
PROJECT_ID = "reflected-radio-438310-s1"
DATASET_ID = "retail_analytics_db"
MODEL_NAME = "gemini-2.5-flash"

# 🚩 FIX 1: Define separate output directories
OUTPUT_DIR_CSV = "bigquery_schemas_with_business_ctx_csv"
OUTPUT_DIR_TXT = "bigquery_schemas_with_business_ctx_txt"


# --- CORE UTILITY FUNCTIONS (Same as before) ---

def get_table_schema_fields(client: bigquery.Client, dataset_id: str, table_id: str) -> List[Dict]:
    """Fetches the schema fields, including data type, for a single BigQuery table."""
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        schema_fields = []
        for field in table.schema:
            description = field.description if field.description else "[REQUIRES DESCRIPTION]"
            schema_fields.append({
                "name": field.name,
                "data_type": field.field_type,
                "description": description
            })
        return schema_fields
    except Exception as e:
        print(f"Error fetching schema for {dataset_id}.{table_id}: {e}")
        return []


def format_schema_for_gemini(project_id: str, dataset_id: str, table_id: str, schema: List[Dict]) -> str:
    """Formats the schema into a string that Gemini can easily parse and modify, including business context placeholder."""
    header = f"--- BigQuery Table Schema: {dataset_id}.{table_id} ---\n\n"
    field_lines = []

    if not schema:
        return header + "SCHEMA NOT FOUND OR ERROR OCCURRED."

    for field in schema:
        line = (
            f"Field Name: {field['name']}\n"
            f"Data Type: {field['data_type']}\n"
            f"Description: {field['description']}\n"
            f"Business Context: [REQUIRES BUSINESS CONTEXT]\n"
            f"{'-' * 30}\n"
        )
        field_lines.append(line)

    return header + "".join(field_lines)


def generate_descriptions_with_gemini(schema_text: str) -> str:
    """Uses the Gemini model to generate enhanced descriptions AND business context for the schema."""
    print(f"\nCalling Gemini API ({MODEL_NAME}) to generate descriptions and context...")

    try:
        client = genai.Client()
    except Exception as e:
        print("Error initializing Gemini client. Ensure GEMINI_API_KEY is set.")
        print(e)
        return schema_text

    prompt = f"""
    You are an expert data analyst and business intelligence specialist. Review the BigQuery table schema provided below.
    Your task is to fill in the missing information for each field.

    1. Replace the placeholder text "[REQUIRES DESCRIPTION]" with a concise, clear, and accurate technical description.
    2. Replace the placeholder text "[REQUIRES BUSINESS CONTEXT]" with a detailed explanation of what this field is used for in the context of retail analytics.

    Keep the existing structure exactly the same, preserving all 'Field Name', 'Data Type', 'Description:', and 'Business Context:' lines. Do not add or remove any fields.

    --- SCHEMA TO DESCRIBE ---
    {schema_text}
    --- END OF SCHEMA ---

    Please return ONLY the final, modified schema text.
    """

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={"temperature": 0.2}
        )
        print("Successfully received enhanced data from Gemini.")
        return response.text.strip()

    except APIError as e:
        print(f"Gemini API Error: {e}")
        print("Returning the original schema without AI enhancements.")
        return schema_text
    except Exception as e:
        print(f"An unexpected error occurred during Gemini call: {e}")
        return schema_text


def parse_ai_response_and_merge(ai_text: str, original_schema: List[Dict]) -> List[Dict]:
    """
    🚩 FIX 2: Parses the text output using a dictionary lookup for robustness against
    LLM reordering or minor formatting errors, and ensures placeholders are cleaned up.
    """
    name_to_data = {}

    # Use a generic regex to capture blocks reliably
    field_blocks = re.split(r'\n-{30}\n', ai_text)

    for block in field_blocks:
        block = block.strip()
        if not block:
            continue

        # Extract fields using regex lookups on the block
        name_match = re.search(r"Field Name:\s*(.*)", block)
        desc_match = re.search(r"Description:\s*(.*)", block)
        ctx_match = re.search(r"Business Context:\s*(.*)", block)

        if name_match:
            name = name_match.group(1).strip()

            # Extract and clean descriptions/contexts
            description = desc_match.group(1).strip() if desc_match else "[FAILED TO PARSE]"
            business_context = ctx_match.group(1).strip() if ctx_match else "[FAILED TO PARSE]"

            # Clean up residual placeholders/quotes if LLM missed them
            description = description.replace('[REQUIRES DESCRIPTION]', '').strip()
            business_context = business_context.replace('[REQUIRES BUSINESS CONTEXT]', '').strip()

            name_to_data[name] = {
                "description": description,
                "business_context": business_context
            }

    # Merge with original schema data
    final_output = []
    for field in original_schema:
        name = field['name']
        merged_data = name_to_data.get(name, {})

        final_output.append({
            "name": name,
            "data_type": field['data_type'],  # Retain Data Type

            # 🚩 FIX 3: Prioritize parsed data, but ensure we don't carry over placeholders
            "description": merged_data.get("description") or field['description'].replace('[REQUIRES DESCRIPTION]',
                                                                                          '').strip(),
            "business_context": merged_data.get("business_context") or ""  # Fallback to empty string for context
        })

    return final_output


# --------------------------------------------------------------------------------
# TXT File Writer (Almost identical, using new directory)
# --------------------------------------------------------------------------------
def write_schema_to_txt(output_path: str, project_id: str, dataset_id: str, table_id: str, schema_data: List[Dict]):
    """Writes the final schema documentation to a TXT file in the specified format."""

    lines = []

    lines.append(f"Project Name: {project_id}")
    lines.append(f"Dataset Name: {dataset_id}")
    lines.append(f"Table Name: {table_id}")
    lines.append(f"Table Description: [REQUIRES MANUAL TABLE DESCRIPTION]")
    lines.append("\nTable Schema:\n")

    for i, field in enumerate(schema_data, 1):
        lines.append(f"{i}. Column Name: {field['name']}")
        lines.append(f"   Description: {field['description']}")
        lines.append(f"   Business Context: {field['business_context']}\n")

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return True
    except Exception as e:
        print(f"Error writing TXT file to {output_path}: {e}")
        return False


# --------------------------------------------------------------------------------
# CSV File Writer (Almost identical, using new directory)
# --------------------------------------------------------------------------------
def write_schema_to_csv(output_path: str, schema_data: List[Dict]) -> bool:
    """Writes a list of schema dictionaries (name, description, business_context) to a CSV file."""
    fieldnames = ["name", "description", "business_context"]

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

            writer.writeheader()

            csv_rows = [{
                "name": row['name'],
                "description": row['description'],
                "business_context": row['business_context']
            } for row in schema_data]

            writer.writerows(csv_rows)

        return True
    except Exception as e:
        print(f"Error writing CSV to {output_path}: {e}")
        return False


# --- MAIN EXPORT LOGIC (Iterates over all tables) ---

def export_all_dataset_schemas_to_files(project_id: str, dataset_id: str, output_dir_csv: str, output_dir_txt: str):
    print(f"Connecting to BigQuery and preparing to export schemas from all tables in {dataset_id}...")

    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return

    # 🚩 FIX 1: Create both directories
    os.makedirs(output_dir_csv, exist_ok=True)
    os.makedirs(output_dir_txt, exist_ok=True)

    dataset_ref = client.dataset(dataset_id)

    table_count = 0
    exported_count = 0

    try:
        tables_iterator = client.list_tables(dataset_ref)

        for table in tables_iterator:
            table_count += 1
            table_id = table.table_id
            print("-" * 50)
            print(f"  Processing table {table_count}: {table_id}...")

            schema_data = get_table_schema_fields(client, dataset_id, table_id)

            if schema_data:
                gemini_input_content = format_schema_for_gemini(project_id, dataset_id, table_id, schema_data)
                ai_description_text = generate_descriptions_with_gemini(gemini_input_content)
                final_structured_schema = parse_ai_response_and_merge(ai_description_text, schema_data)

                # --- Write TXT File ---
                txt_file_name = f"{table_id}.txt"
                txt_output_path = os.path.join(output_dir_txt, txt_file_name)  # Use TXT dir

                txt_success = write_schema_to_txt(
                    txt_output_path, project_id, dataset_id, table_id, final_structured_schema
                )
                if txt_success:
                    print(f"    -> Full documentation saved to {txt_output_path}")

                # --- Write CSV File (using the same data) ---
                csv_file_name = f"{table_id}.csv"
                csv_output_path = os.path.join(output_dir_csv, csv_file_name)  # Use CSV dir

                csv_success = write_schema_to_csv(csv_output_path, final_structured_schema)

                if csv_success and txt_success:
                    print(f"    -> Data dictionary CSV saved to {csv_output_path}")
                    exported_count += 1
                else:
                    print(f"    -> Failed to save files for {table_id}.")

            else:
                print(f"    -> Skipping {table_id} due to fetch error or empty schema.")

    except Exception as e:
        print(f"\nFATAL ERROR during table listing: {e}")

    print("-" * 50)
    print(f"Schema export complete.")
    print(f"Total tables found: {table_count}")
    print(f"Total files exported: {exported_count} pairs.")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    export_all_dataset_schemas_to_files(PROJECT_ID, DATASET_ID, OUTPUT_DIR_CSV, OUTPUT_DIR_TXT)
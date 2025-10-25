import os
from google.cloud import bigquery
from google import genai
from google.genai.errors import APIError

# --- CONFIGURATION ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP Project ID
DATASET_ID = "your_dataset_id"  # Replace with your BigQuery Dataset ID
TABLE_ID = "your_table_id"  # Replace with your BigQuery Table ID
MODEL_NAME = "gemini-2.5-flash"  # Use a fast model for this task

INPUT_FILENAME = f"{TABLE_ID}_schema_raw.txt"
OUTPUT_FILENAME = f"{TABLE_ID}_schema_described.txt"


def get_bigquery_schema(project_id: str, dataset_id: str, table_id: str) -> list:
    """Fetches the schema of a specified BigQuery table."""
    print(f"Connecting to BigQuery and fetching schema for {dataset_id}.{table_id}...")
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        # Extract the fields
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
        print(f"Successfully fetched {len(schema_fields)} fields.")
        return schema_fields
    except Exception as e:
        print(f"Error fetching BigQuery schema: {e}")
        return []


def format_schema_for_file(schema: list) -> str:
    """Formats the schema list into a clean string for a text file."""
    header = f"--- BigQuery Table Schema: {DATASET_ID}.{TABLE_ID} ---\n\n"
    field_lines = []
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


def write_to_file(filename: str, content: str):
    """Writes content to a specified file."""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Schema successfully written to '{filename}'")


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
    bq_schema = get_bigquery_schema(PROJECT_ID, DATASET_ID, TABLE_ID)

    if not bq_schema:
        print("Could not retrieve schema. Exiting.")
    else:
        # 2. Format and Save the Initial (Raw) Schema
        raw_schema_content = format_schema_for_file(bq_schema)
        write_to_file(INPUT_FILENAME, raw_schema_content)

        # 3. Generate Descriptions using Gemini
        # We pass the formatted string content to the model
        described_schema_content = generate_descriptions_with_gemini(raw_schema_content)

        # 4. Save the Final (Described) Schema
        write_to_file(OUTPUT_FILENAME, described_schema_content)

        print("\n✅ Process complete. Check the generated files for results.")
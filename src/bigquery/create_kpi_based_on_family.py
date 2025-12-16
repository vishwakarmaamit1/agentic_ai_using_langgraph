import config
import os
import re
import csv
from google import genai
from google.genai.errors import APIError
from typing import List, Dict, Any

# --- CONFIGURATION ---
MODEL_NAME = "gemini-2.5-flash"
INPUT_DIR_CSV = "bigquery_schemas_with_business_ctx_csv"
# NEW OUTPUT DIRECTORY AND FILE PATH
OUTPUT_DIR = "biqquery_table_kpi"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, "kpi_definitions_by_family.txt")

MASTER_FILE_NAME = "kpi_definitions_by_family.txt"
MASTER_FILE_PATH = os.path.join(OUTPUT_DIR, MASTER_FILE_NAME)

# --- CORE LOGIC (Aggregate function remains the same) ---

def aggregate_schema_data(input_dir: str) -> str:
    """
    Reads all CSV files and aggregates column data into a single string for Gemini.
    """
    aggregated_data = []

    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            filepath = os.path.join(input_dir, filename)
            table_name = filename.replace(".csv", "")

            try:
                with open(filepath, mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        if row.get('name', '').lower() == 'name':
                            continue

                        field_entry = (
                            f"Table: {table_name}\n"
                            f"Column: {row.get('name', 'N/A')}\n"
                            f"Description: {row.get('description', 'N/A')}\n"
                            f"Business Context: {row.get('business_context', 'N/A')}\n"
                            f"{'-' * 30}\n"
                        )
                        aggregated_data.append(field_entry)

            except Exception as e:
                print(f"Warning: Could not read CSV file {filename}: {e}")

    return "\n".join(aggregated_data)


def generate_kpi_families_with_gemini(schema_text: str) -> str:
    """
    Uses the Gemini model to categorize metrics, define them, and apply the required format.
    """
    print(f"\nCalling Gemini API ({MODEL_NAME}) to generate KPI families and definitions...")

    try:
        client = genai.Client()
    except Exception as e:
        print("Error initializing Gemini client. Ensure GEMINI_API_KEY is set.")
        return ""

    # -------------------------------------------------------------------
    # NEW PROMPT: Incorporates the required output format and SQL aggregation
    # -------------------------------------------------------------------
    prompt = f"""
    You are an expert Data Governance Analyst for Retail Analytics.
    Review the following collection of BigQuery table columns and their business context definitions.

    Your task is to organize this information into a structured document.

    --- INSTRUCTIONS ---
    1. **Identify KPI Families:** Create 5 to 8 high-level categories (e.g., Sales Performance, Inventory Health).
    2. **Group Metrics:** Under each KPI Family, list all relevant columns from the input data.
    3. **Define Metrics and Formulas:** For each column, provide its definition and its corresponding **default SQL aggregation** for dashboard reporting.

    4. **Maintain Strict Output Format:** Output the result ONLY in the following structured text format.
        * **For primary quantitative metrics (e.g., total_sales, margin, quantity_sold), use the Coalesce(SUM(...), 0) pattern.**
        * **For dimensions (non-aggregatable fields), use the column name itself as the default formula.**

    ## [KPI FAMILY NAME 1]
    Metric Family Definition: [1-sentence business definition of this KPI family]

    **Metric/Dimension: [Column Name from Input]**
    * Metric Definition: [Clear definition based on Description/Context]
    * Default SQL Formula: [Coalesce(SUM([Column Name]), 0) or [Column Name]]

    **Metric/Dimension: total_sales**
    * Metric Definition: The total revenue generated from the sale of goods/services before any discounts or taxes.
    * Default SQL Formula: Coalesce(SUM(total_sales), 0)

    ## [KPI FAMILY NAME 2]
    Metric Family Definition: [1-sentence business definition of this KPI family]

    **Metric/Dimension: margin**
    * Metric Definition: The profit earned from a sale, calculated as total_sales minus cost_of_goods_sold.
    * Default SQL Formula: Coalesce(SUM(margin), 0)

    --- INPUT DATA ---
    {schema_text}
    --- END OF INPUT DATA ---

    Return ONLY the final structured documentation.
    """
    # -------------------------------------------------------------------

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={"temperature": 0.3}
        )
        print("Successfully received KPI definitions from Gemini.")
        return response.text.strip()

    except APIError as e:
        print(f"Gemini API Error: {e}")
        return ""
    except Exception as e:
        print(f"An unexpected error occurred during Gemini call: {e}")
        return ""


def write_output_file(content: str, output_path: str):
    """Writes the final categorized content to a text file, creating the directory if necessary."""

    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"\n✅ Success! KPI definitions saved to: {output_path}")
    except Exception as e:
        print(f"\n❌ Error writing output file to {output_path}: {e}")


# --- MAIN EXECUTION ---

def generate_and_save_kpi_definitions():
    """Main function to run the aggregation, AI generation, and saving process."""

    if not os.path.isdir(INPUT_DIR_CSV):
        print(
            f"Error: Input directory not found at '{INPUT_DIR_CSV}'. Please ensure your CSV files are in the correct location.")
        return

    # 1. Aggregate all data dictionary CSV files
    aggregated_schema = aggregate_schema_data(INPUT_DIR_CSV)

    if not aggregated_schema:
        print("Error: Aggregated schema data is empty. Check your CSV files.")
        return

    # 2. Generate categorized definitions using Gemini
    kpi_definitions = generate_kpi_families_with_gemini(aggregated_schema)

    if kpi_definitions:
        # 3. Write the final structured output file
        write_output_file(kpi_definitions, OUTPUT_FILE_PATH)
    else:
        print("Error: Failed to generate KPI definitions from Gemini.")

# --- CORE LOGIC ---

def split_kpi_definitions_by_family(master_file_path: str, output_dir: str):
    """
    Reads the master file, splits content by '## [KPI FAMILY NAME]',
    and saves each family's content to a separate file.
    """
    if not os.path.exists(master_file_path):
        print(f"Error: Master file not found at '{master_file_path}'. Please run 'generate_kpi_definitions.py' first.")
        return

    try:
        with open(master_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading master file: {e}")
        return

    # 1. Use regex to split the content based on the '##' header pattern.
    # The regex pattern: (## .*) finds the header and saves the content following it.
    # re.split keeps the delimiters (headers) in the results, which is helpful.

    # This splits the content, resulting in a list where:
    # [0] is usually empty or pre-header text
    # [1] is the content of Family 1 (including its header)
    # [2] is the content of Family 2 (including its header)

    # We use a pattern that captures the header line: (##.*?)(?=##|$)
    # (##.*?) captures the header line lazily
    # (?=##|$) is a positive lookahead: it asserts that the match is followed by '##' OR the end of the file ($)
    sections = re.findall(r'(##.*?)(?=##|$)', content, re.DOTALL)

    if not sections:
        print("Warning: No '##' KPI Family headers found in the master file. Check Gemini's output format.")
        return

    print(f"Found {len(sections)} KPI families. Starting separation...")

    count = 0
    for section in sections:
        # 2. Extract the clean Family Name from the header
        header_match = re.match(r'##\s*(.*?)\n', section)
        if header_match:
            # Clean the name to use as a filename (replace spaces with underscores, remove non-alphanumeric)
            family_name = header_match.group(1).strip()
            safe_filename = re.sub(r'[^a-zA-Z0-9_]+', '', family_name.replace(' ', '_')).lower()

            if not safe_filename:
                safe_filename = f"unnamed_family_{count}"

            output_file_name = f"{safe_filename}_kpi.txt"
            output_file_path = os.path.join(output_dir, output_file_name)

            # 3. Write the content to the new file
            try:
                with open(output_file_path, 'w', encoding='utf-8') as outfile:
                    outfile.write(section.strip())

                print(f"  ✅ Created file: {output_file_name}")
                count += 1
            except Exception as e:
                print(f"  ❌ Error writing file {output_file_name}: {e}")
        else:
            print(f"Warning: Could not parse header for a section. Skipping.")

    print(f"\nCompleted. Total separated files created: {count}")

if __name__ == "__main__":
    generate_and_save_kpi_definitions()
    split_kpi_definitions_by_family(MASTER_FILE_PATH, OUTPUT_DIR)
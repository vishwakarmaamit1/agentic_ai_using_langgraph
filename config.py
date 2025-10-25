import os

# --- Configuration ---
# Set the path to your downloaded service account key file
# NOTE: Replace the placeholder path below with the actual path to your JSON key file.
SERVICE_ACCOUNT_KEY_PATH = "/Users/kiara/Documents/GCP/reflected-radio-438310-s1-de3bb2446281.json"

# --- Environment Setup ---
# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# This is how Google Cloud client libraries (like 'google-cloud-storage')
# automatically find the key file.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH

# Optional: Add a check to confirm the variable is set (for debugging)
print(f"Set GOOGLE_APPLICATION_CREDENTIALS to: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")

# You can now import this file in your main application script.
# Any subsequent code that uses a Google Cloud client library
# (e.g., from google.cloud import storage) will automatically use
# the specified service account key for authentication.
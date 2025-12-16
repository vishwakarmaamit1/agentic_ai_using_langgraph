import config
import os
from google.cloud import storage # Example GCP client library
from google.cloud.storage import Client as GCSClient
from typing import Dict, Any

def list_buckets():
    """Lists all buckets in the project authenticated by GOOGLE_APPLICATION_CREDENTIALS."""

    # The storage client automatically finds the credentials
    # because 'config.py' set the environment variable.
    storage_client = storage.Client()

    print("Buckets:")
    for bucket in storage_client.list_buckets():
        print(f"- {bucket.name}")

# --- NEW GCS UPLOAD FUNCTION ---
def upload_file_to_gcs(local_file_path: str, bucket_name: str, destination_blob_name: str):
    """Uploads a single file to a Google Cloud Storage bucket."""
    storage_client = GCSClient()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Use client-side logging for background tasks
    print(f"  ⬆️ Uploading {os.path.basename(local_file_path)} to gs://{bucket_name}/{destination_blob_name}")
    blob.upload_from_filename(local_file_path)
    return blob.public_url

if __name__ == "__main__":
    list_buckets()
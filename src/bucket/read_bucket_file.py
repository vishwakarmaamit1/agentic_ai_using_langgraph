import config
from google.cloud import storage # Example GCP client library

def list_buckets():
    """Lists all buckets in the project authenticated by GOOGLE_APPLICATION_CREDENTIALS."""

    # The storage client automatically finds the credentials
    # because 'config.py' set the environment variable.
    storage_client = storage.Client()

    print("Buckets:")
    for bucket in storage_client.list_buckets():
        print(f"- {bucket.name}")

if __name__ == "__main__":
    list_buckets()
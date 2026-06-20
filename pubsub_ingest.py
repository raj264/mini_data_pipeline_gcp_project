"""Module: pubsub_ingest.py
Cloud Function to ingest Pub/Sub messages into raw GCS zone."""
import base64
import os
from google.cloud import storage

def pubsub_handler(event, context):
    """Background Cloud Function for Pub/Sub events."""
    client = storage.Client()
    bucket = client.bucket(os.environ['RAW_BUCKET'])
    data = base64.b64decode(event['data'])
    # Use timestamp or message ID for naming
    blob_name = f"pubsub/{context.event_timestamp}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)
    return f"Stored to {blob_name}"

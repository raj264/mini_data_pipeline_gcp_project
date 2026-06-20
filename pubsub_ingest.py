"""Module: pubsub_ingest.py
Cloud Function to ingest Pub/Sub messages into raw GCS zone."""
import base64
import logging
import os

from google.cloud import storage

logger = logging.getLogger(__name__)


def pubsub_handler(event, context):
    """Background Cloud Function for Pub/Sub events."""
    if 'data' not in event:
        raise ValueError("Pub/Sub event is missing 'data' field")

    try:
        data = base64.b64decode(event['data'])
    except (TypeError, ValueError) as e:
        raise ValueError(f"Could not base64-decode Pub/Sub event data: {e}") from e

    client = storage.Client()
    bucket = client.bucket(os.environ['RAW_BUCKET'])
    # Use timestamp or message ID for naming
    blob_name = f"pubsub/{context.event_timestamp}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)
    logger.info("Stored Pub/Sub message to %s", blob_name)
    return f"Stored to {blob_name}"

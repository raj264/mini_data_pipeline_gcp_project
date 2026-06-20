"""Module: handler.py
Cloud Function orchestrator: ingest → validate → route → transform → curate."""
import os
import json
from google.cloud import storage, pubsub_v1
from validation import run_validation_pipeline
from transformation import run_transformation_pipeline

def orchestrator(event, context):
    # Environment variables
    raw_bucket = os.environ['RAW_BUCKET']
    staging_prefix = os.environ['STAGING_PREFIX']
    quarantine_prefix = os.environ['QUARANTINE_PREFIX']
    enriched_prefix = os.environ['ENRICHED_PREFIX']
    curated_prefix = os.environ['CURATED_PREFIX']
    project = os.environ['GCP_PROJECT']
    temp_location = os.environ['TEMP_LOCATION']
    dataflow_region = os.environ['DATAFLOW_REGION']

    storage_client = storage.Client()
    bucket = storage_client.bucket(raw_bucket)

    # Ingestion handled via Cloud Functions and Pub/Sub triggers

    # Validation stage: launch Dataflow job
    run_validation_pipeline(
        input_path=f'gs://{raw_bucket}/{staging_prefix}',
        temp_location=temp_location,
        project=project,
        registry_schema=os.environ['DATAFLOW_SCHEMA'],
        output_topic=os.environ['PUBSUB_FAILURE_TOPIC']
    )

    # Transformation stage: launch Dataflow job
    run_transformation_pipeline(
        input_path=f'gs://{raw_bucket}/{staging_prefix}',
        output_path=f'gs://{raw_bucket}/{enriched_prefix}',
        project=project,
        temp_location=temp_location,
        lookup_table=os.environ['LOOKUP_BIGQUERY_TABLE']
    )

    # Curated stage and metadata handled in curated_zone and metadata_catalog modules
    return {'status': 'Triggered Dataflow jobs'}

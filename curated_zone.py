"""Module: curated_zone.py
Functions to load enriched data into BigQuery."""
from google.cloud import bigquery

def load_parquet_to_bigquery(dataset: str, table: str, source_uri: str):
    """Load Parquet files from GCS into BigQuery table."""
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field='timestamp')
    )
    job = client.load_table_from_uri(source_uri, f'{dataset}.{table}', job_config=job_config)
    job.result()  # Wait for completion
    return job.state

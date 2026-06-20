# Mini Data Ingestion Pipeline on GCP

## Purpose
This project demonstrates a full end-to-end data pipeline using GCP services:
1. **Data Acquisition & Ingestion** from FTP/SFTP, REST, SOAP, GraphQL, gRPC, Pub/Sub.
2. **Validation (Quality Gate)** via Apache Beam/Dataflow or Dataproc snapshots, Dataflow pipelines, and custom rules.
3. **Transformation & Enrichment** in Apache Beam (Dataflow) or Dataproc PySpark.
4. **Curated/"Gold" Zone** stored as partitioned Parquet in Cloud Storage, queried through BigQuery.
5. **Metadata Management & Catalog** using Data Catalog, automated crawlers, and IAM.
6. **Monitoring & Alerting** via Cloud Monitoring, Pub/Sub notifications, and custom drift detection.

## Architecture Overview
- **Cloud Functions** orchestrate ingestion and routing.
- **Dataflow** (Apache Beam) or **Dataproc** handle validation and transformation.
- **Cloud Storage** buckets organized in prefixes: `raw/`, `staging/`, `quarantine/`, `enriched/`, `curated/`.
- **BigQuery** tables are created over curated datasets.
- **Data Catalog** registers metadata; **Cloud IAM** secures access.
- **Pub/Sub** topics for real-time streaming and alerts.

## Project Structure
```
mini_data_pipeline_gcp/
|-- README.md
|-- requirements.txt
|-- ftp_ingest.py
|-- api_ingest.py
|-- pubsub_ingest.py
|-- validation.py
|-- transformation.py
|-- handler.py
|-- curated_zone.py
|-- metadata_catalog.py
|-- monitoring.py
|-- .gitignore
```

## Usage
1. Configure GCP credentials & environment variables:
   - `RAW_BUCKET`, `STAGING_PREFIX`, `QUARANTINE_PREFIX`, `ENRICHED_PREFIX`, `CURATED_PREFIX`
   - API endpoints & credentials: `REST_URL`, `SOAP_WSDL`, etc.
   - `LOOKUP_JDBC_CONN`, `LOOKUP_TABLE`
   - `PROJECT_ID`, `DATASET`, `PUBSUB_TOPIC`, `DATAFLOW_REGION`
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Deploy Cloud Functions and Dataflow templates via Terraform or gcloud.
4. Trigger the orchestrator manually or schedule via Cloud Scheduler.

## Example Usage
```bash
# Invoke ingestion Cloud Function
gcloud functions call ingestHandler --data '{}'
# Run Dataflow template
gcloud dataflow jobs run validate-job --gcs-location gs://my-templates/validate_template
# Query curated data in BigQuery
bq query --use_legacy_sql=false 'SELECT * FROM dataset.curated_table LIMIT 10;'
```

## Requirements
- Python 3.8+
- GCP IAM service account with roles: Storage Admin, Dataflow Admin, BigQuery Admin, Data Catalog Admin, Pub/Sub Editor, Monitoring Editor.
- GCP SDKs: `google-cloud-storage`, `google-cloud-pubsub`, `google-cloud-bigquery`, `apache-beam[gcp]`, `zeep`, `grpcio`

## Final Output
- Cleaned and enriched Parquet datasets in `gs://<bucket>/curated/...`
- BigQuery tables created over curated datasets.
- Notifications on data quality failures or schema drift via Pub/Sub.

## Notes
- Customize Cloud Scheduler or Cloud Workflows for orchestration.
- Extend monitoring for advanced drift detection.
- Ensure Data Catalog IAM policies are configured for secure access.

# Mini Data Ingestion Pipeline on GCP

## 🎯 Purpose

An end-to-end data pipeline on GCP:

1. **Ingestion** from FTP/SFTP, REST, SOAP, GraphQL, gRPC, and Pub/Sub, with retry-with-backoff on every call.
2. **Validation (quality gate)** via Apache Beam: classifies each record as valid or invalid based on required fields + an email-format check, writes invalid records to a quarantine GCS path, and publishes them to Pub/Sub.
3. **Transformation & enrichment** via Apache Beam: type casts, derives year/month from the timestamp, and joins a BigQuery lookup table.
4. **Curated zone**: enriched records loaded into BigQuery.
5. **Metadata management** via Data Catalog entries and IAM policy bindings.
6. **Monitoring & alerting** via Cloud Monitoring uptime checks and Pub/Sub alerts.

## ⚙️ Architecture Overview

- **Cloud Functions** (`handler.py`, `pubsub_ingest.py`) orchestrate ingestion and routing.
- **Dataflow** (Apache Beam) runs validation and transformation.
- **Cloud Storage** is organized by prefix: `raw/`, `staging/`, `quarantine/`, `enriched/`.
- **BigQuery** tables sit over the curated data.
- **Data Catalog** registers metadata; **Cloud IAM** secures access.
- **Pub/Sub** carries real-time ingestion and failure-alert topics.

## 📁 Project Structure

```
mini_data_pipeline_gcp_project/
├── ftp_ingest.py          # FTP/SFTP -> GCS, with retries
├── api_ingest.py          # REST/SOAP/GraphQL/gRPC -> GCS, with retries
├── pubsub_ingest.py       # Pub/Sub message -> raw GCS zone
├── validation.py          # Beam: classify valid/invalid, quarantine + Pub/Sub on invalid
├── transformation.py      # Beam: type casts, year/month derivation, BigQuery lookup enrichment
├── handler.py             # Cloud Function orchestrator: triggers the validation/transformation Dataflow jobs
├── curated_zone.py        # Load enriched Parquet into BigQuery
├── metadata_catalog.py    # Data Catalog entries, IAM policy bindings
├── monitoring.py          # Cloud Monitoring uptime checks, Pub/Sub alerts
├── tests/                 # pytest suite
└── requirements.txt
```

## 🚀 Usage

1. Configure GCP credentials and environment variables:
   - `RAW_BUCKET`, `STAGING_PREFIX`, `QUARANTINE_PREFIX`, `ENRICHED_PREFIX`, `CURATED_PREFIX`
   - `GCP_PROJECT`, `TEMP_LOCATION`, `DATAFLOW_REGION`, `DATAFLOW_SCHEMA` (JSON list of required fields, e.g. `["id","email"]`), `PUBSUB_FAILURE_TOPIC`
   - `LOOKUP_BIGQUERY_TABLE`

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Deploy the Cloud Functions and trigger Dataflow jobs via `gcloud` or your IaC of choice.

## 🧪 Running Tests

```bash
pytest tests/ -v
```

Covers ingestion retry behavior (mocked FTP/HTTP/GCS), the validation classifier and transformation enrichment logic (run directly as plain Python, no live Beam runner needed), and Pub/Sub message handling. CI runs this on every push.

## 🧭 Example Usage

```bash
# Invoke the ingestion Cloud Function
gcloud functions call ingestHandler --data '{}'

# Run a Dataflow validation/transformation job
gcloud dataflow jobs run validate-job --gcs-location gs://my-templates/validate_template

# Query curated data in BigQuery
bq query --use_legacy_sql=false 'SELECT * FROM dataset.curated_table LIMIT 10;'
```

## 🔧 Requirements

- Python 3.8+
- GCP service account with roles: Storage Admin, Dataflow Admin, BigQuery Admin, Data Catalog Admin, Pub/Sub Editor, Monitoring Editor
- Key SDKs: `google-cloud-storage`, `google-cloud-pubsub`, `google-cloud-bigquery`, `google-cloud-datacatalog`, `google-cloud-monitoring`, `apache-beam[gcp]`, `zeep`, `grpcio`

## 📊 Output

- Quarantined records in `gs://<bucket>/<quarantine_prefix>` plus a Pub/Sub failure notification
- Enriched, queryable records in BigQuery
- Cloud Monitoring alerts on uptime/schema-drift issues

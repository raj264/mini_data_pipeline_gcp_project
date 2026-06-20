"""Module: transformation.py
Apache Beam pipeline for transformation & enrichment."""
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)


class EnrichDoFn(beam.DoFn):
    """Casts types, derives year/month from the timestamp, and enriches each
    record with matching fields from a BigQuery lookup table keyed by id."""

    def __init__(self, lookup_table):
        self.lookup_table = lookup_table
        self._lookup_by_id = None

    def start_bundle(self):
        from google.cloud import bigquery
        client = bigquery.Client()
        rows = client.query(f'SELECT * FROM `{self.lookup_table}`').result()
        self._lookup_by_id = {str(row['id']): dict(row.items()) for row in rows}

    def process(self, element):
        record = dict(element)
        try:
            record['id'] = str(record['id'])
            timestamp = datetime.fromisoformat(record['timestamp'])
            record['year'] = timestamp.year
            record['month'] = timestamp.month
        except (KeyError, ValueError) as e:
            logger.error("Could not normalize record %s: %s", record, e)
            return

        lookup_match = (self._lookup_by_id or {}).get(record['id'], {})
        record.update({k: v for k, v in lookup_match.items() if k not in record})
        yield record


def run_transformation_pipeline(input_path, output_path, project, temp_location, lookup_table):
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        temp_location=temp_location
    )
    with beam.Pipeline(options=options) as p:
        (p | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)
           | 'ParseJSON' >> beam.Map(json.loads)
           | 'Enrich' >> beam.ParDo(EnrichDoFn(lookup_table))
           | 'SerializeJSON' >> beam.Map(json.dumps)
           | 'WriteEnriched' >> beam.io.WriteToText(output_path, file_name_suffix='.json')
        )

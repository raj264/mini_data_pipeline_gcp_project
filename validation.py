"""Module: validation.py
Defines Dataflow (Apache Beam) pipelines for quality checks."""
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)

DEFAULT_REQUIRED_FIELDS = ['id', 'email']


class ClassifyRecordDoFn(beam.DoFn):
    """Tags each record as valid (main output) or invalid (tagged output)
    based on JSON parseability, required-field presence, and an email
    format check when 'email' is a required field."""

    OUTPUT_TAG_INVALID = 'invalid'

    def __init__(self, required_fields):
        self.required_fields = required_fields

    def process(self, element):
        try:
            record = json.loads(element)
        except (TypeError, ValueError) as e:
            logger.error("Could not parse record as JSON: %s", e)
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, element)
            return

        has_required_fields = all(record.get(field) for field in self.required_fields)
        email_ok = '@' in record.get('email', '') if 'email' in self.required_fields else True

        if has_required_fields and email_ok:
            yield element
        else:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, element)


def run_validation_pipeline(input_path, temp_location, project, registry_schema,
                             output_topic, quarantine_path):
    """Beam pipeline to validate JSON records:
    - Parses each record and checks it against the required fields (passed
      as a JSON-encoded list via registry_schema, e.g. '["id", "email"]')
    - Valid records are written back out for the transformation stage
    - Invalid records are written to the quarantine GCS path and published
      to Pub/Sub so downstream consumers can alert on them
    """
    required_fields = json.loads(registry_schema) if registry_schema else DEFAULT_REQUIRED_FIELDS

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        temp_location=temp_location
    )
    with beam.Pipeline(options=options) as p:
        records = p | 'ReadJSON' >> beam.io.ReadFromText(input_path)
        classified = (
            records
            | 'ClassifyRecords' >> beam.ParDo(ClassifyRecordDoFn(required_fields)).with_outputs(
                ClassifyRecordDoFn.OUTPUT_TAG_INVALID, main='valid'
            )
        )
        classified.valid | 'WriteValid' >> beam.io.WriteToText(f'{input_path}/valid/')
        classified.invalid | 'WriteQuarantine' >> beam.io.WriteToText(quarantine_path)
        classified.invalid | 'PublishFailures' >> beam.io.WriteToPubSub(topic=output_topic)

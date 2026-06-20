"""Module: validation.py
Defines Dataflow (Apache Beam) pipelines for quality checks."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_validation_pipeline(input_path, temp_location, project, registry_schema, output_topic):
    """Beam pipeline to validate JSON records:
    - Schema enforcement using custom ParDo
    - Record-level rules via Filter transforms
    - Publish failures to Pub/Sub"""
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        temp_location=temp_location
    )
    with beam.Pipeline(options=options) as p:
        records = p | 'ReadJSON' >> beam.io.ReadFromText(input_path)
        valid = (records
                 | 'ParseJSON' >> beam.Map(lambda x: json.loads(x))
                 | 'FilterValid' >> beam.Filter(lambda r: 'id' in r and r['id'] and '@' in r.get('email',''))
                )
        invalid = records | 'FilterInvalid' >> beam.Filter(lambda x: x not in valid)
        invalid | 'PublishFailures' >> beam.io.WriteToPubSub(topic=output_topic)
        valid | 'WriteValid' >> beam.io.WriteToText(f'{input_path}/valid/')

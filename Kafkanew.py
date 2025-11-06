import argparse
import io
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from fastavro import schemaless_reader, parse_schema


class DecodeAvroMessage(beam.DoFn):
    """Decode Avro binary using the provided .avsc schema."""
    def __init__(self, schema_path):
        self.schema_path = schema_path
        self.schema = None

    def setup(self):
        with open(self.schema_path, "r") as f:
            self.schema = parse_schema(json.load(f))
        logging.info("Loaded Avro schema with fields: %s", [f['name'] for f in self.schema['fields']])

    def process(self, element):
        """element = raw bytes of Avro record."""
        # If your message source provides a tuple (key, value), adjust accordingly:
        value_bytes = element if isinstance(element, bytes) else element[1]
        record = schemaless_reader(io.BytesIO(value_bytes), self.schema)
        yield record


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        help='Pub/Sub topic to read from. Format: projects/PROJECT/topics/TOPIC',
        required=True)
    parser.add_argument(
        '--schema_path',
        help='Path to Avro schema file (.avsc)',
        required=True)
    parser.add_argument(
        '--output_table',
        help='BigQuery output table: PROJECT:dataset.table',
        required=True)
    parser.add_argument(
        '--region', required=True)
    parser.add_argument(
        '--temp_location', required=True)
    parser.add_argument(
        '--staging_location', required=True)
    parser.add_argument(
        '--project', required=True)
    parser.add_argument(
        '--runner', default='DataflowRunner')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Configure Beam pipeline options
    options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        streaming=True,
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            # 1️⃣ Read Avro bytes (for example, from Pub/Sub)
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            
            # 2️⃣ Decode Avro bytes using schema
            | 'DecodeAvro' >> beam.ParDo(DecodeAvroMessage(known_args.schema_path))
            
            # 3️⃣ Write dict records to BigQuery
            | 'WriteToBQ' >> WriteToBigQuery(
                table=known_args.output_table,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=known_args.temp_location
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


python pipeline.py \
  --runner=DataflowRunner \
  --project=my-gcp-project \
  --region=us-central1 \
  --staging_location=gs://my-bucket/staging \
  --temp_location=gs://my-bucket/temp \
  --schema_path=schema.avsc \
  --input_topic=projects/my-gcp-project/topics/avro-topic \
  --output_table=my-gcp-project:dataset.avro_events


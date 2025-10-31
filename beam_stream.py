import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, AfterWatermark
from datetime import datetime
import pytz
import csv
import os


# 1️⃣ Parse Kafka message
class ParseKafkaMessage(beam.DoFn):
    def process(self, element):
        key, value = element
        data = json.loads(value.decode("utf-8"))
        yield data


# 2️⃣ Add event-time timestamp
class AddEventTimestamp(beam.DoFn):
    def process(self, element):
        event_time = datetime.fromisoformat(element["timestamp"].replace("Z", "+00:00"))
        yield TimestampedValue(element, event_time.timestamp())


# 3️⃣ CombineFn for aggregation
class AggregateAmounts(beam.CombineFn):
    def create_accumulator(self):
        return {"count": 0, "total_amount": 0.0}

    def add_input(self, acc, element):
        acc["count"] += 1
        acc["total_amount"] += float(element.get("amount", 0))
        return acc

    def merge_accumulators(self, accs):
        merged = {"count": 0, "total_amount": 0.0}
        for acc in accs:
            merged["count"] += acc["count"]
            merged["total_amount"] += acc["total_amount"]
        return merged

    def extract_output(self, acc):
        return acc


# ✅ Custom DoFn for live CSV write
class WriteToCSVFn(beam.DoFn):
    def __init__(self, file_path):
        self.file_path = file_path

    def process(self, element):
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["user_id", "count", "total_amount", "window_end"])
                writer.writeheader()

        with open(self.file_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["user_id", "count", "total_amount", "window_end"])
            writer.writerow(element)
            f.flush()  # force flush so you can see updates live

        yield element  # allow further downstream use (optional)


def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True,
        runner="DirectRunner",
    )
    options.view_as(StandardOptions).streaming = True

    output_file = "aggregated_output.csv"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": "localhost:9092"
                },
                topics=["test"],
            )
            | "ParseJSON" >> beam.ParDo(ParseKafkaMessage())
            | "AddEventTimestamp" >> beam.ParDo(AddEventTimestamp())
            | "Window2MinEarly1Min" >> beam.WindowInto(
                FixedWindows(30),
                trigger=AfterWatermark(early=AfterProcessingTime(15)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x))
            | "CombinePerUser" >> beam.CombinePerKey(AggregateAmounts())
            | "FormatOutput" >> beam.Map(
                lambda kv: {
                    "user_id": kv[0],
                    "count": kv[1]["count"],
                    "total_amount": kv[1]["total_amount"],
                    "window_end": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            | "PrintResults" >> beam.Map(print)
            | "WriteToCSV" >> beam.ParDo(WriteToCSVFn(output_file))
        )


if __name__ == "__main__":
    print("🚀 Apache Beam streaming job started (printing + writing CSV live)...")
    print("Press Ctrl+C to stop.")
    run()

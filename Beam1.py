import argparse
import logging
import random
from datetime import datetime

import apache_beam as beam
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, window_size, num_shards):
        # Set window size to 60 seconds.
        self.window_size = window_size
        self.num_shards = num_shards
    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | 'window' >> beam.WindowInto(FixedWindows(self.window_size))
        )

class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])

        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body in batch:
                f.write("{}\n".format(message_body).encode("utf-8"))

class CustomPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--output_path",
            type=str,
            help="Path of the output GCS file including the prefix.",
        )

def run(input_topic, num_shards, window_size):


    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    with Pipeline(options=custom_options) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(custom_options.output_path))
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
            "--input_topic",
            help="The Cloud Pub/Sub topic to read from."
            '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
        )
    parser.add_argument(
            "--num_shards",
            default=5,
            type=int,
            help="Number of shards to use when writing windowed elements to GCS.",
        )
    parser.add_argument(
            "--window_size",
            default=1,
            type=int,
            help="Output file's window size in minutes.",
        )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.num_shards,
        known_args.window_size
        )
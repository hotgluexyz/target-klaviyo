"""Klaviyo target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_klaviyo.sinks import (
    KlaviyoSink,
)


class TargetKlaviyo(Target):
    """Sample target for Klaviyo."""

    name = "target-klaviyo"
    config_jsonschema = th.PropertiesList(
        th.Property("api_private_key", th.StringType)
    ).to_dict()
    default_sink_class = KlaviyoSink


if __name__ == "__main__":
    TargetKlaviyo.cli()

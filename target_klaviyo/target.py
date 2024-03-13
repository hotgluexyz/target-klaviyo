"""Klaviyo target class."""

from pathlib import PurePath
from typing import List, Optional, Union

from singer_sdk import typing as th
from singer_sdk.sinks import Sink
from target_hotglue.target import TargetHotglue

from target_klaviyo.sinks import ContactsSink, FallbackSink


class TargetKlaviyo(TargetHotglue):
    """Sample target for Klaviyo."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

    name = "target-klaviyo"
    SINK_TYPES = [ContactsSink]

    config_jsonschema = th.PropertiesList(
        th.Property("api_private_key", th.StringType),
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("refresh_token", th.StringType),
    ).to_dict()

    def get_sink_class(self, stream_name: str):
        """Get sink for a stream."""
        # Use fallback sink based on flag
        if self.config.get("use_fallback_sink", False):
            return FallbackSink

        for sink_class in self.SINK_TYPES:
            if sink_class.name.lower() == stream_name.lower():
                return sink_class
            # Search for streams with multiple names
            elif stream_name.lower() in sink_class.available_names:
                return sink_class


if __name__ == "__main__":
    TargetKlaviyo.cli()

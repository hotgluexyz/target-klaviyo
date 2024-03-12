from typing import Dict, List, Optional

from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueSink

from target_klaviyo.auth import KlaviyoApiKeyAuthenticator, KlaviyoAuthenticator


class KlaviyoSink(HotglueSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    auth_state = {}
    base_url = "https://a.klaviyo.com/api"
    available_names = []

    @property
    def authenticator(self):
        # auth with hapikey
        if self.config.get("api_private_key"):
            api_key = self.config.get("api_private_key")
            return KlaviyoApiKeyAuthenticator(self._target, api_key)
        # auth with acces token
        url = "https://a.klaviyo.com/oauth/token"
        return KlaviyoAuthenticator(self._target, self.auth_state, url)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "accept": "application/json",
            "revision": "2023-07-15",
            "content-type": "application/json",
        }
        headers.update(self.authenticator.auth_headers or {})
        return headers

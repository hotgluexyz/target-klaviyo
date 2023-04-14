"""Klaviyo target sink class, which handles writing streams."""


from singer_sdk.sinks import RecordSink
import time
import requests
import phonenumbers

class KlaviyoSink(RecordSink):
    """Klaviyo target sink class."""

    base_url = "https://a.klaviyo.com/api"

    @property
    def get_headers(self):
        headers = {
            "accept": "application/json",
            "revision": "2023-02-22",
            "content-type": "application/json",
            "Authorization": f"Klaviyo-API-Key {self.config.get('api_private_key')}",
        }
        return headers

    def _post(self, stream, payload, req_type="POST"):
        url = f"{self.base_url}/{stream}"
        res = requests.request(
            method=req_type, url=url, json=payload, headers=self.get_headers
        )
        if res.status_code == 429:
            time.sleep(int(res.headers["RateLimit-Reset"]))
            self._post(stream, payload, req_type)
        res.raise_for_status()
        print(res.text)
        return res.json()

    def assosiate_list_profile(self, profile, list_id):
        stream = f"lists/{list_id}/relationships/related_resource/profiles"
        payload = {"data": {"type": "profile", "id": profile}}
        self._post(stream, payload)

    def process_profile(self, record):
        first_name, *last_name = record["name"].split()
        last_name = " ".join(last_name)

        phone_number = record.get("phone") 
        if phone_number:
            phone_number = phonenumbers.parse(phone_number)
            if phonenumbers.is_valid_number(phone_number):             
                payload["phone_number"] = phone_number
        payload = {
            "email": record.get("email"),
            "first_name": first_name,
            "last_name": last_name,
            # "organization": "Klaviyo",
            # "title": "Engineer",
        }
        if "addresses" in record:
            if len(record["addresses"]) > 0:
                address = record["addresses"][0]
                payload.update(
                    {
                        "location": {
                            "address1": address.get("line1"),
                            "address2": address.get("line2"),
                            "city": address.get("city"),
                            "region": address.get("state"),
                            "zip": address.get("postal_code"),
                            "country": address.get("country"),
                        }
                    }
                )
        payload = {"data": {"type": "profile", "attributes": payload}}
        profile = self._post("profiles", payload)
        # TODO assosiate a profile to a list
        # if record.get("list_id"):
        #     if "data" in profile:
        #         if "id" in profile["data"]:
        #             self.assosiate_list_profile(profile["data"]['id'],record.get("list_id"))

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if self.stream_name == "Customers":
            self.process_profile(record)

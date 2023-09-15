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
        api_key = self.config.get('api_private_key',self.config.get("api_key"))
        headers = {
            "accept": "application/json",
            "revision": "2023-07-15",
            "content-type": "application/json",
            "Authorization": f"Klaviyo-API-Key {api_key}",
        }
        return headers

    def _post(self, stream, payload, req_type="POST", params={}):
        url = f"{self.base_url}/{stream}"
        res = requests.request(
            method=req_type,
            url=url,
            json=payload,
            headers=self.get_headers,
            params=params,
        )
        if res.status_code == 429:
            time.sleep(int(res.headers["RateLimit-Reset"]))
            self._post(stream, payload, req_type)
        res.raise_for_status()
        if res.status_code == 202:
            return True
        print(res.text)
        return res.json()

    def associate_list_profile(self, profile, list_id, subscribe=True):
        # stream = f"lists/{list_id}/relationships/related_resource/profiles"
        if subscribe:
            stream = f"profile-subscription-bulk-create-jobs"
            endpoint_type = "profile-subscription-bulk-create-job"
        elif not subscribe:
            stream = f"profile-subscription-bulk-delete-jobs"
            endpoint_type = "profile-subscription-bulk-delete-job"
        # payload = {"data": {"type": "profile", "id": profile}}
        payload = {
            "data": {
                "type": endpoint_type,
                "attributes": {
                    "profiles": {
                        "data": [
                            {
                                "type": "profile",
                                "attributes": {
                                    "email": profile["data"]["attributes"].get("email")
                                },
                                "id": profile["data"].get("id"),
                            }
                        ]
                    }
                },
                "relationships": {"list": {"data": {"type": "list", "id": list_id}}},
            }
        }
        if profile["data"]["attributes"].get("phone_number") and subscribe:
            payload["data"]["attributes"]["profiles"]["data"][0]["attributes"].update(
                {"phone_number": profile["data"]["attributes"].get("phone_number")}
            )
        if not subscribe:
            del payload["data"]["attributes"]["profiles"]["data"][0]["id"]
        response = self._post(stream, payload)
    def search_profile(self, email):

        params = {"filter": f"equals(email,'{email}')"}
        profile = self._post(f"profiles", None, "GET", params)
        if len(profile.get("data")) > 0:
            return profile["data"][0]
        return None

    def process_profile(self, record):
        if "first_name" in record:
            first_name = record.get("first_name")
            last_name = record.get("last_name")
        if "name" in  record:
            try:
                first_name, *last_name = record["name"].split()
            except:
                first_name = record["name"]
                last_name = ""
        last_name = " ".join(last_name)
        profile_search = self.search_profile(record.get("email"))
        if profile_search:
            if "id" in profile_search:
                record["id"] = profile_search["id"]
        payload = {
            "email": record.get("email"),
            "first_name": first_name,
            "last_name": last_name,
            # "organization": "Klaviyo",
            # "title": "Engineer",
        }
        phone_number = record.get("phone")
        if phone_number:
            try:
                phone_number = phonenumbers.parse(phone_number)
                if phonenumbers.is_valid_number(phone_number):
                    payload["phone_number"] = phone_number
            except:
                TypeError
                print(f"Invalid phone {phone_number}. Skipping.")
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
        
        if "custom_fields" in record: 
            custom_fields = {}
            for field in record['custom_fields']:
                custom_fields[field['name']] = field['value']

            payload.update({'properties':custom_fields})
        
        payload = {"data": {"type": "profile", "attributes": payload}}
        if record.get("id"):
            payload["data"].update({"id": record.get("id")})
            profile = self._post(f"profiles/{record.get('id')}", payload, "PATCH")
        else:
            profile = self._post("profiles", payload)

        if self.config.get("list_id") and record.get("subscribe_status"):
            if record["subscribe_status"] == "unsubscribed":
                subscribe_status = False
            else: 
                subscribe_status = True
            
            if "data" in profile:
                if "id" in profile["data"]:
                    self.associate_list_profile(
                        profile, self.config.get("list_id"), subscribe_status
                    )

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if self.stream_name.lower() in ["customers", "customer", "contacts", "contact"]:
            self.process_profile(record)

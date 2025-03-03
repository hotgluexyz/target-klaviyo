"""Klaviyo target sink class, which handles writing streams."""

import phonenumbers

from target_klaviyo.client import KlaviyoSink


class ContactsSink(KlaviyoSink):
    """Klaviyo target sink class."""

    name = "contacts"
    available_names = ["customers", "customer", "contacts", "contact"]
    endpoint = "/profiles"

    def search_profile(self, email):
        params = {"filter": f"equals(email,'{email}')"}
        profile = self.request_api("GET", self.endpoint, params)
        profile = profile.json()
        if len(profile.get("data")) > 0:
            return profile["data"][0]

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
        self.request_api("POST", f"/{stream}", request_data=payload)

    def preprocess_record(self, record: dict, context: dict) -> None:

        if "first_name" in record:
            first_name = record.get("first_name")
            last_name = record.get("last_name")
        if "name" in record:
            try:
                first_name, *last_name = record["name"].split()
            except:
                first_name = record["name"]
                last_name = ""
        last_name = " ".join(last_name)
        payload = {
            "email": record.get("email"),
            "first_name": first_name,
            "last_name": last_name,
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
            for field in record["custom_fields"]:
                custom_fields[field["name"]] = field["value"]
            payload.update({"properties": custom_fields})

        payload = {"data": {"type": "profile", "attributes": payload}}

        profile_search = self.search_profile(record.get("email"))
        if profile_search:
            if "id" in profile_search:
                payload["data"]["id"] = profile_search["id"]

        return payload

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"
        if record:
            # post or put record
            id = record.get("data", {}).get("id")
            if id:
                method = "PATCH"
                endpoint = f"{endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            res_json = response.json()
            id = res_json["data"][pk]

            # associate profile
            if self.config.get("list_id") and record.get("subscribe_status"):
                if record["subscribe_status"] == "unsubscribed":
                    subscribe_status = False
                else:
                    subscribe_status = True

                if "data" in res_json:
                    if "id" in res_json["data"]:
                        self.associate_list_profile(
                            res_json, self.config.get("list_id"), subscribe_status
                        )
            return id, True, state_updates


class FallbackSink(KlaviyoSink):
    """Handles generic Klaviyo data sync, including profiles and list_members."""

    @property
    def endpoint(self):
        """Dynamically determine the endpoint based on the stream name."""
        endpoint_mapping = {
            "list_members": "/profiles",  # Both profiles and list_members should write to /profiles
        }

        # Return the mapped endpoint if it exists, otherwise default to /{stream_name}
        return endpoint_mapping.get(self.stream_name, f"/{self.stream_name}")

    @property
    def name(self):
        """Use the stream name as the sink name."""
        return self.stream_name
    
    def search_profile(self, email):
        """Search for a profile in Klaviyo by email."""
        params = {"filter": f"equals(email,'{email}')"}
        response = self.request_api("GET", self.endpoint, params)

        try:
            profile_data = response.json().get("data", [])
            return profile_data[0] if profile_data else None
        except ValueError:
            print(f"Error parsing JSON response while searching for profile: {email}")
            return None
        
    def _build_profile_payload(self, record: dict) -> dict:
        """Extract and structure profile payload from a flat record."""
        first_name = record.get("first_name", "")
        last_name = record.get("last_name", "")

        # Extract names if only `name` field is provided
        if "name" in record and not first_name:
            try:
                first_name, *last_name = record["name"].split()
                last_name = " ".join(last_name)
            except ValueError:
                first_name, last_name = record["name"], ""

        payload = {
            "email": record.get("email"),
            "first_name": first_name,
            "last_name": last_name,
        }

        # Validate and add phone number
        phone_number = record.get("phone")
        if phone_number:
            try:
                parsed_number = phonenumbers.parse(phone_number, None)
                if phonenumbers.is_valid_number(parsed_number):
                    payload["phone_number"] = phonenumbers.format_number(
                        parsed_number, phonenumbers.PhoneNumberFormat.E164
                    )
            except phonenumbers.NumberParseException:
                print(f"Invalid phone {phone_number}. Skipping.")

        # Add address if available
        if "addresses" in record and record["addresses"]:
            address = record["addresses"][0]  # Use the first address
            payload["location"] = {
                "address1": address.get("line1"),
                "address2": address.get("line2"),
                "city": address.get("city"),
                "region": address.get("state"),
                "zip": address.get("postal_code"),
                "country": address.get("country"),
            }

        # Add custom properties
        if "custom_fields" in record:
            properties = {field["name"]: field["value"] for field in record["custom_fields"]}
            payload["properties"] = properties

        # Transform into Klaviyo's expected payload format
        klaviyo_payload = {"data": {"type": "profile", "attributes": payload}}
        
        existing_profile = self.search_profile(record.get("email"))
        if existing_profile and "id" in existing_profile:
            klaviyo_payload["data"]["id"] = existing_profile["id"]

        return klaviyo_payload


    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Transforms flat payloads into Klaviyo's expected nested format for profiles and list_members."""
        
        if self.stream_name in ["profiles", "list_members"]:
            return self._build_profile_payload(record)

        return record  # Keep default behavior for other streams

    def associate_list_profile(self, profile, list_id, subscribe=True):
        """Associates a profile with a list, subscribing or unsubscribing based on `subscribe` flag."""
        stream = "profile-subscription-bulk-create-jobs" if subscribe else "profile-subscription-bulk-delete-jobs"
        endpoint_type = "profile-subscription-bulk-create-job" if subscribe else "profile-subscription-bulk-delete-job"

        payload = {
            "data": {
                "type": endpoint_type,
                "attributes": {
                    "profiles": {
                        "data": [
                            {
                                "type": "profile",
                                "attributes": {
                                    "email": profile["data"]["attributes"].get("email"),
                                },
                                "id": profile["data"].get("id"),
                            }
                        ]
                    }
                },
                "relationships": {"list": {"data": {"type": "list", "id": list_id}}},
            }
        }

        # Include phone number if available and subscribing
        phone_number = profile["data"]["attributes"].get("phone_number")
        if phone_number and subscribe:
            payload["data"]["attributes"]["profiles"]["data"][0]["attributes"]["phone_number"] = phone_number

        # Remove `id` for unsubscribe operation
        if not subscribe:
            del payload["data"]["attributes"]["profiles"]["data"][0]["id"]

        self.request_api("POST", f"/{stream}", request_data=payload)

    def upsert_record(self, record: dict, context: dict):
        """Handles upsert operation for Klaviyo profiles and other streams."""
        state_updates = {}
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"

        if record:
            # Determine whether to POST or PATCH
            id = record.get("data", {}).get("id") if self.stream_name in ["profiles", "list_members"] else record.get(pk)
            if id:
                method = "PATCH"
                endpoint = f"{endpoint}/{id}"

            response = self.request_api(method, endpoint=endpoint, request_data=record)
            res_json = response.json()
            id = res_json["data"][pk]

            # Associate profile with a list if `list_id` is provided
            if self.config.get("list_id") and self.stream_name in ["profiles", "list_members"]:
                subscribe_status = record.get("subscribe_status", "subscribed") != "unsubscribed"
                self.associate_list_profile(res_json, self.config["list_id"], subscribe_status)

            return id, True, state_updates

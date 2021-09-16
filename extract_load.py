import os
import requests
from retry import retry
from typing import List
from loguru import logger
from inspect import signature
from dataclasses import dataclass
from google.cloud import bigquery as bq


@dataclass
class ThingSpeak:
    primary_id_a: int
    primary_key_a: str
    primary_id_b: int
    primary_key_b: str
    secondary_id_a: int
    secondary_key_a: str
    secondary_id_b: int
    secondary_key_b: str

    @classmethod
    def from_dict(self, input):
        """
        ignores extraneous keys from input dict on new instance creation
        """
        return self(
            **{k: v for k, v in input.items() if k in signature(self).parameters}
        )

    @property
    def id_key_pairs(self):
        return (
            (self.primary_id_a, self.primary_key_a),
            (self.secondary_id_a, self.secondary_key_a),
            (self.primary_id_b, self.primary_key_b),
            (self.secondary_id_b, self.secondary_key_b),
        )

    @property
    def urls(self) -> List[str]:
        base_url = "https://api.thingspeak.com/channels"
        return [
            f"{base_url}/{id}/feeds.json?api_key={key}" for id, key in self.id_key_pairs
        ]

    @retry(Exception, tries=3, delay=1, backoff=5)
    def get_feed(self, url) -> List[dict]:

        logger.debug("Fetching feeds from ThingSpeak API")

        response = requests.get(url + "&days=3")
        response.raise_for_status()
        response = response.json()
        field_names = {
            f"{k}_name": v for k, v in response["channel"].items() if k[:5] == "field"
        }
        channel_name = {"name": response["channel"]["name"]}
        feeds = [{**channel_name, **field_names, **feed} for feed in response["feeds"]]

        logger.debug("Successfully retreived feeds from ThingSpeak")
        return feeds

    @property
    def feeds(self):
        return [feed for url in self.urls for feed in self.get_feed(url)]


@retry(Exception, tries=3, delay=1, backoff=5)
def get_keys_from_purple() -> object:
    logger.debug("Fetching ThingSpeak keys from Purple API")

    PURPLE_API_KEY = os.environ.get("PURPLE_API_KEY")
    url = f"https://api.purpleair.com/v1/sensors/20957?api_key={PURPLE_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()

    logger.debug("Successfully retreived ThingSpeak keys")
    return response.json()["sensor"]


def extract():
    purple_air_response = get_keys_from_purple()
    thingspeak = ThingSpeak.from_dict(purple_air_response)

    return thingspeak.feeds


def load(data):
    logger.debug("Connecting to BigQuery")
    client = bq.Client()
    logger.debug("Connected to BigQuery Successfully")

    job_config = bq.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=False)
    destination = "databuildtool.air_quality.raw_thingspeak"
    load_job = client.load_table_from_json(
        data, destination=destination, job_config=job_config
    )
    load_job.result()
    logger.debug(f"Loaded into BigQuery {destination} successfully")


def main(http_trigger=None):
    extracted = extract()
    logger.debug(f"Extracted {len(extracted)} feeds successfully")
    load(extracted)
    return "Success"


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    main()

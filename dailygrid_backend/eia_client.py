import requests
import logging
from typing import Optional

from dailygrid_backend.config import EIA_API_KEY, EIA_BASE_URL


class EIAClient:
    """Client to interact with the EIA API."""

    def __init__(self, api_key: str = EIA_API_KEY):
        self.api_key = api_key
        self.base_url = EIA_BASE_URL
        self.session = requests.Session()

    def fetch_daily_generation(
        self, start_date: str, end_date: Optional[str] = None
    ) -> dict:
        try:
            response = self.session.get(
                self.base_url,
                params={
                    "api_key": self.api_key,
                    "frequency": "daily",
                    "data[0]": "value",
                    "facets[respondent][]": ["US48"],
                    "start": start_date,
                    # "end": "2025-05-25T12",
                    "end": end_date,
                    "sort[0][column]": "period",
                    "sort[0][direction]": "desc",
                    "offset": 0,
                    "length": 5000,
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"EIA API error: {e}")
            raise

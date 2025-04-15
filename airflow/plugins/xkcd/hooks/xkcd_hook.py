"""Hook for interacting with XKCD API"""

import logging
import time
from typing import Optional, Dict, Any

import requests
from airflow.hooks.base import BaseHook
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import (
    XKCD_API_BASE_URL,
    XKCD_API_SUFFIX,
    RETRY_ATTEMPTS,
    RETRY_DELAY_SECONDS,
    REQUEST_DELAY_SECONDS,
    LOG_LEVEL
)

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

class XKCDHook(BaseHook):
    """
    Hook for fetching data from XKCD API with rate limiting and retry mechanism
    """

    def __init__(self):
        super().__init__()
        self.base_url = XKCD_API_BASE_URL

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_DELAY_SECONDS),
        reraise=True
    )
    def _make_request(self, url: str) -> Dict[str, Any]:
        """
        Make HTTP request to XKCD API with rate limiting and retry logic

        Args:
            url: API endpoint URL
        Returns:
            JSON response from API
        Raises:
            requests.exceptions.RequestException: If request fails after all retries
        """
        logger.debug(f"Making request to: {url}")
        response = requests.get(url)
        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response.json()

    def get_latest_comic_num(self) -> int:
        """
        Fetch the number of the latest XKCD comic

        Returns:
            Latest comic number
        """
        url = f"{self.base_url}/{XKCD_API_SUFFIX}"
        response = self._make_request(url)
        latest_num = response.get('num')
        logger.info(f"Latest comic #{latest_num}")
        return latest_num

    def get_comic_by_num(self, num: int) -> Optional[Dict[str, Any]]:
        """
        Fetch specific comic by number

        Args:
            num: Comic number to fetch
        Returns:
            Comic data as dictionary or None if comic doesn't exist
        """
        url = f"{self.base_url}/{num}/{XKCD_API_SUFFIX}"
        logger.info(f"Fetching comic #{num}")
        try:
            return self._make_request(url)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch comic #{num}: {str(e)}")
            return None
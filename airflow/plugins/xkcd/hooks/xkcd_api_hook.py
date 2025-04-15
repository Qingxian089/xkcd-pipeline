import time
from typing import Optional, Dict, Any
import requests
from airflow.hooks.base import BaseHook
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from xkcd.config import (
    XKCD_BASE_URL,
    XKCD_INFO_ENDPOINT,
    DEFAULT_RATE_LIMIT_DELAY,
    DEFAULT_RETRY_LIMIT,
    DEFAULT_RETRY_MIN_DELAY,
    DEFAULT_RETRY_MAX_DELAY,
    DEFAULT_RETRY_MULTIPLIER,
    DEFAULT_LOG_LEVEL,
    HTTP_OK,
)


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)


class XKCDApiHook(BaseHook):
    """
    Hook for interacting with XKCD API
    """

    def __init__(
            self,
            retry_limit: int = DEFAULT_RETRY_LIMIT,
            retry_delay: int = DEFAULT_RETRY_MIN_DELAY,
            rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY,
    ) -> None:
        """
        Initialize XKCDApiHook

        Args:
            retry_limit: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            rate_limit_delay: Delay between API calls in seconds
        """
        super().__init__()
        self.base_url = XKCD_BASE_URL
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.rate_limit_delay = rate_limit_delay

    @retry(
        stop=stop_after_attempt(DEFAULT_RETRY_LIMIT),
        wait=wait_exponential(
            multiplier=DEFAULT_RETRY_MULTIPLIER,
            min=DEFAULT_RETRY_MIN_DELAY,
            max=DEFAULT_RETRY_MAX_DELAY
        ),
        reraise=True
    )
    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """
        Make HTTP request to XKCD API with retry logic

        Args:
            endpoint: API endpoint to call
        Returns:
            API response as dictionary
        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Making request to: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

    def get_latest_comic_num(self) -> int:
        """
        Get the number of the latest comic

        Returns:
            Latest comic number
        """
        response = self._make_request(XKCD_INFO_ENDPOINT)
        latest_num = response.get("num")
        logger.info(f"Latest comic from API: {latest_num}")
        return latest_num

    def get_comic_by_num(self, num: int) -> Optional[Dict[str, Any]]:
        """
        Get comic data by number

        Args:
            num: Comic number to fetch
        Returns:
            Comic data as dictionary or None if not found
        """
        logger.info(f"Fetching comic #{num}")
        try:
            return self._make_request(f"{num}/{XKCD_INFO_ENDPOINT}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch comic #{str(e)}")
            return None
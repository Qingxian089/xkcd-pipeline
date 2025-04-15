"""
XKCD Data Pipeline Utilities
Handles API communication, rate limiting, and database operations
"""

from psycopg2.extras import Json
import requests
import time
import logging
from datetime import datetime, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional
from psycopg2.extras import Json

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class RateLimiter:
    """Global rate limiter for API calls (100 calls/minute)"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.reset()
        return cls._instance

    def reset(self):
        self.calls = 0
        self.window_start = time.time()

    def check(self):
        elapsed = time.time() - self.window_start
        if elapsed > 60:
            self.reset()
        elif self.calls >= 99:
            sleep_time = 60 - elapsed
            logger.warning(f"Rate limit reached. Sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
            self.reset()

# Global rate limiter instance
rate_limiter = RateLimiter()

def get_api_max() -> int:
    """Get latest comic number from API"""
    try:
        rate_limiter.check()
        response = requests.get("https://xkcd.com/info.0.json", timeout=10)
        response.raise_for_status()
        rate_limiter.calls += 1
        return response.json()['num']
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def get_db_max(conn_id: str) -> int:
    """Get maximum comic number from database"""
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(num), 0) FROM xkcd.raw_xkcd_comics;")
        return cursor.fetchone()[0]

def fetch_comic(num: int) -> Optional[dict]:
    """Fetch single comic data with rate limiting"""
    try:
        rate_limiter.check()
        response = requests.get(f"https://xkcd.com/{num}/info.0.json", timeout=10)
        response.raise_for_status()
        rate_limiter.calls += 1
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Comic {num} not found")
            return None
        logger.error(f"HTTP error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return None

def insert_comic(conn_id: str, data: dict) -> bool:
    """Insert comic data into database"""
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            pub_date = datetime(
                int(data['year']),
                int(data['month']),
                int(data['day'])
            ).date()

            cursor.execute("""
                INSERT INTO xkcd.raw_xkcd_comics 
                (num, title, alt_text, img_url, published_date, transcript, fetched_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (num) DO NOTHING;
            """, (
                data['num'],
                data['title'],
                data['alt'],
                data['img'],
                pub_date,
                data.get('transcript', ''),
                datetime.now(timezone.utc),
                Json(data)
            ))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Insert failed: {str(e)}")
        conn.rollback()
        return False


def unified_processor(conn_id: str, batch_size: int = 90):
    """Unified processing for all scenarios"""
    db_max = get_db_max(conn_id)
    api_max = get_api_max()

    # Scenario 1: Data up-to-date. Start polling.
    polling_interval = 7200  # poll every 2 hours
    polling_start = time.time()
    while time.time() - polling_start < 16 * 60 * 60: # poll for 16 hours
        if db_max < api_max:
            break
        logger.info(f"No new comics. Next check in {polling_interval // (60*60)} hours")
        time.sleep(polling_interval)
        api_max = get_api_max()

    # Scenario 2: Polling failed.
    if db_max >= api_max:
        logger.info("No new comics. Polling failed.")
        return "Polling failed. No new comics."
    # Scenario 3: Database empty or incomplete.
    else:
        logger.info(f"ingesting comics {db_max + 1}-{api_max}")
        for start in range(db_max + 1, api_max + 1, batch_size):
            end = min(start + batch_size - 1, api_max)
            for num in range(start, end + 1):
                data = fetch_comic(num)
                if data:
                    insert_comic(conn_id, data)
            time.sleep(0.5)  # Short pause between batches

        logger.info(f"Ingested comics {db_max + 1}-{api_max}")
        return f"Ingested successfully"





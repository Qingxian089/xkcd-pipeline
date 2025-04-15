from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

from xkcd.hooks.xkcd_api_hook import XKCDApiHook
from xkcd.hooks.xkcd_postgres_hook import XKCDPostgresHook
from xkcd.utils.comic_parser import ComicParser, ComicData
from xkcd.config import (
    TASK_RETRY_DELAY,
    MAX_ACTIVE_RUNS,
    POLLING_INTERVAL_MINUTES,
    MAX_POLLING_RETRIES,
    DEFAULT_LOG_LEVEL

)

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)

default_args = {
    'owner': 'Qingxian',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025, 4, 16),
    'retries': 0,
}


@dag(
    dag_id='xkcd_incremental_update',
    default_args=default_args,
    description='Incrementally fetch and load new XKCD comics',
    schedule_interval='0 8 * * 1,3,5',  # Run at 8 AM on Monday, Wednesday, Friday
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=['xkcd', 'incremental'],
)
def xkcd_incremental_dag():
    @task(
        retries=MAX_POLLING_RETRIES,  # 16 retries (8:00 - 24:00)
        retry_delay=timedelta(minutes=POLLING_INTERVAL_MINUTES),
        retry_exponential_backoff=False,
    )
    def get_comic_numbers_to_process() -> List[int]:
        """
        Get all comic numbers

        Returns:
            Dictionary containing new comic numbers or empty list
        Raises:
            AirflowSkipException: When no new comics found
        """
        api_hook = XKCDApiHook()
        pg_hook = XKCDPostgresHook()

        try:
            latest_api_num = api_hook.get_latest_comic_num()
            latest_db_num = pg_hook.get_max_comic_num()

            if latest_api_num <= latest_db_num:
                logger.info("No new comics found, will retry in 60 minutes")
                # This will trigger a retry if retries remaining, otherwise task will fail
                raise AirflowSkipException("No new comics found")

            new_comics = list(range(latest_db_num + 1, latest_api_num + 1))
            logger.info(f"Found {len(new_comics)} new comics to fetch")
            return new_comics

        except Exception as e:
            if not isinstance(e, AirflowSkipException):
                logger.error(f"Failed to check for updates: {str(e)}")
            raise

    @task
    def fetch_comic(comic_num: int) -> Optional[ComicData]:
        """Fetch single comic data"""
        api_hook = XKCDApiHook()
        parser = ComicParser()

        try:
            raw_data = api_hook.get_comic_by_num(comic_num)
            if not raw_data:
                return None

            comic_data = parser.parse_comic_data(raw_data)
            if not comic_data:
                return None

            return comic_data

        except Exception as e:
            logger.error(f"Error fetching comic {comic_num}: {str(e)}")
            raise

    @task
    def load_comic(comic_data: Optional[ComicData]) -> bool:
        """Save comic data to database"""
        if not comic_data:
            return False

        pg_hook = XKCDPostgresHook()

        try:
            return pg_hook.insert_single_comic(comic_data)
        except Exception as e:
            logger.error(f"Error loading comic {comic_data.num}: {str(e)}")
            raise

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results() -> None:
        """Simple summary of ingestion results"""
        pg_hook = XKCDPostgresHook()
        max_num = pg_hook.get_max_comic_num()
        logger.info(
            f"Ingestion completed. Latest comic in database: #{max_num}"
        )

    # Create start and end markers
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE  # Ensure this runs regardless of upstream states
    )

    # Build task flow
    comic_nums = get_comic_numbers_to_process()
    comic_data_list = fetch_comic.expand(comic_num=comic_nums)
    load_results = load_comic.expand(comic_data=comic_data_list)
    summary = summarize_results()

    # Define task dependencies
    start >> comic_nums >> comic_data_list >> load_results >> summary >> end

# Create DAG instance
dag = xkcd_incremental_dag()
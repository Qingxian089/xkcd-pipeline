from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import logging
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

from xkcd.hooks.xkcd_api_hook import XKCDApiHook
from xkcd.hooks.xkcd_postgres_hook import XKCDPostgresHook
from xkcd.utils.comic_parser import ComicParser, ComicData
from xkcd.config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_LOG_LEVEL
)

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)

default_args = {
    'owner': 'Qingxian',
    'depends_on_past': True,  # Ensure sequential processing
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='xkcd_backfill',
    default_args=default_args,
    description='Backfill historical XKCD comics',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 4, 14),
    catchup=False,
    max_active_runs=1,  # Ensure only one backfill runs at a time
    tags=['xkcd', 'backfill'],
)
def xkcd_backfill_dag():
    @task
    def get_comic_numbers_to_process() -> List[List[int]]:
        """
        Get all missing comic numbers and split them into batches

        Returns:
            Dictionary containing batches of comic numbers
        """
        api_hook = XKCDApiHook()
        pg_hook = XKCDPostgresHook()

        try:
            latest_api_num = api_hook.get_latest_comic_num()
            latest_db_num = pg_hook.get_max_comic_num() or 0

            # Generate list of comics to process
            comics_to_process = list(range(latest_db_num + 1, latest_api_num + 1))

            # Split into batches
            batches = [
                comics_to_process[i:i + DEFAULT_BATCH_SIZE]
                for i in range(0, len(comics_to_process), DEFAULT_BATCH_SIZE)
            ]
            logger.info(f"Found {len(comics_to_process)} comics to process in {len(batches)} batches")
            return batches

        except Exception as e:
            logger.error(f"Failed to get comic numbers: {str(e)}")
            raise

    @task
    def process_batch(batch: List[int]) -> List[bool]:
        """
        Process a batch of comics

        Args:
            batch: List of comic numbers to process
        Returns:
            List of success/failure flags
        """
        api_hook = XKCDApiHook()
        pg_hook = XKCDPostgresHook()
        parser = ComicParser()
        results = []

        logger.info(f"Processing batch: {batch[0]} - {batch[-1]}")
        for comic_num in batch:
            try:
                # Fetch comic data
                raw_data = api_hook.get_comic_by_num(comic_num)
                if not raw_data:
                    results.append(False)
                    continue

                # Parse comic data
                comic_data = parser.parse_comic_data(raw_data)
                if not comic_data:
                    results.append(False)
                    continue

                # Save to database
                success = pg_hook.insert_single_comic(comic_data)
                results.append(success)

            except Exception as e:
                logger.error(f"Error processing comic #{comic_num}: {str(e)}")
                results.append(False)
        logger.info(f"Batch {batch[0]} - {batch[-1]} processed with {sum(results)} successes")
        return results

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
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Build task flow
    batches = get_comic_numbers_to_process()
    # Process each batch
    batch_results = process_batch.expand(batch=batches)

    # Create single summary task
    summary = summarize_results()

    # Define task dependencies
    start >> batches
    batches >> batch_results >> summary >> end

# Create DAG instance
dag = xkcd_backfill_dag()
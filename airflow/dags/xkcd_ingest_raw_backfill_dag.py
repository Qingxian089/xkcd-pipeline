from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import logging
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

from xkcd.hooks.xkcd_api_hook import XKCDApiHook
from xkcd.hooks.xkcd_postgres_hook import XKCDPostgresHook
from xkcd.utils.comic_parser import ComicParser, ComicData
from xkcd.config import (
    MAX_ACTIVE_RUNS,
    DEFAULT_BATCH_SIZE,
    DEFAULT_LOG_LEVEL
)

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)

default_args = {
    'owner': 'Qingxian',
    'depends_on_past': True,
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
    start_date=datetime(2025, 4, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,  # Ensure only one backfill runs at a time
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
    def process_batch(batch: List[int]) -> bool:
        """
        Process a batch of comics with optimized database operations.

        Args:
            batch: List of comic numbers to process
        Returns:
            Dictionary mapping comic numbers to processing success status
        """
        api_hook = XKCDApiHook()
        pg_hook = XKCDPostgresHook()
        parser = ComicParser()

        # List to collect comic data for batch insertion
        comics_data = []

        batch_range = f"Batch {batch[0]}-{batch[-1]}"
        logger.info(f"{batch_range}: Starting fetching of of {len(batch)} comics")
        # Fetch comic data individually
        for comic_num in batch:
            try:
                # API call already includes rate limit delay
                raw_data = api_hook.get_comic_by_num(comic_num)
                if not raw_data:
                    logger.warning(f"{batch_range}: No data returned for comic #{comic_num}")
                    continue

                # Parse comic data
                comic_data = parser.parse_comic_data(raw_data)
                if comic_data:
                    comics_data.append(parser.to_db_record(comic_data))
                else:
                    logger.warning(f"{batch_range}: Failed to parse data for comic #{comic_num}")

            except Exception as e:
                logger.error(f"{batch_range}: Error processing comic #{comic_num}: {str(e)}")
        logger.info(f"{batch_range}: Completed fetching comics.")

        # Use batch insertion for collected comics
        if comics_data:
            logger.info(f"{batch_range}: Starting database insertion.")
            # Insert all comics in a single database operation
            success = pg_hook.insert_batch_comics(comics_data)
            if success:
                logger.info(f"{batch_range}: Batch insertion successful")
            else:
                logger.error(f"{batch_range}: Batch insertion failed")
            return success
        else:
            logger.warning(f"{batch_range}: No valid comics to insert")
            return False

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results(batch_results=None) -> None:
        """Summarize ingestion results"""
        pg_hook = XKCDPostgresHook()
        max_num = pg_hook.get_max_comic_num()

        if batch_results:
            successful_batches = sum(1 for result in batch_results if result)
            total_batches = len(batch_results)
            logger.info(
                f"Ingestion completed. {successful_batches}/{total_batches} batches successfully processed. "
                f"Latest comic in database: #{max_num}"
            )
        else:
            logger.info(f"Ingestion completed. No batches processed. "
                        f"Latest comic in database: #{max_num}")

    # Build task flow
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    batches = get_comic_numbers_to_process()
    batch_results = process_batch.expand(batch=batches)
    summary = summarize_results(batch_results)

    # Trigger DBT transformation DAG after ingestion
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="dbt_models_run_and_test",
        wait_for_completion=False,  # do not wait for completion
        deferrable=False  # do not use deferrable operator
    )

    # Define task dependencies
    start >> batches >> batch_results >> summary
    summary >> trigger_dbt
    summary >> end

# Create DAG instance
dag = xkcd_backfill_dag()
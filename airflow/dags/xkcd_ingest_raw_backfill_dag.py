from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import logging
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.api.common.trigger_dag import trigger_dag
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
    def process_batch(batch: List[int]) -> Dict[int, bool]:
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
        logger.info(f"{batch_range}: Starting processing of {len(batch)} comics")
        # Fetch comic data individually (maintaining existing rate limiting)
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
                    comics_data.append(comic_data)
                else:
                    logger.warning(f"{batch_range}: Failed to parse data for comic #{comic_num}")

            except Exception as e:
                logger.error(f"{batch_range}: Error processing comic #{comic_num}: {str(e)}")

        # Use batch insertion for collected comics
        if comics_data:
            # Insert all comics in a single database operation
            results = pg_hook.insert_comics_batch(comics_data)
            success_count = sum(1 for success in results.values() if success)
            logger.info(f"{batch_range}: Completed with {success_count}/{len(batch)} successes")
        else:
            logger.warning(f"{batch_range}: No valid comics to insert")
            results = {num: False for num in batch}

        return results

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results() -> None:
        """Simple summary of ingestion results"""
        pg_hook = XKCDPostgresHook()
        max_num = pg_hook.get_max_comic_num()
        logger.info(
            f"Ingestion completed. Latest comic in database: #{max_num}"
        )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def trigger_dbt_transformation() -> None:
        """Trigger DBT transformation DAG"""
        logger.info("Triggering DBT transformation DAG")
        trigger_dag(
            dag_id='dbt_models_run_and_test',
            run_id=None
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
    summary >> trigger_dbt_transformation()

# Create DAG instance
dag = xkcd_backfill_dag()
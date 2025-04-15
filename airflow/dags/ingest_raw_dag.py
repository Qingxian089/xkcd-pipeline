"""
XKCD Unified Data Pipeline
Simplified DAG with single processing task
"""

import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from xkcd_helpers.fetch_xkcd_data import unified_processor

DEFAULT_CONN_ID = 'xkcd_postgres'
BATCH_SIZE = 90  # 90% of rate limit capacity

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@dag(
    dag_id='xkcd_raw_ingestion',
    start_date=datetime(2025, 4, 14, 7, 0),  # 07:00 UTC
    schedule_interval='0 8 * * 1,3,5',  # Mon/Wed/Fri at 08:00 UTC
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=15),
        'max_active_tasks': 1
    },
    tags=['xkcd'],
)
def ingest_pipeline():
    @task(task_id='process_comics')
    def process_comics_task():
        """Unified processing entry point"""
        return unified_processor(DEFAULT_CONN_ID, BATCH_SIZE)

    @task(task_id='post_processing')
    def post_task(result: str):
        """Handle post-processing"""
        logger.info(result)
        if "failed" in result.lower():
            raise ValueError("Processing issues detected")
        return "Pipeline completed"

    # Define workflow
    process_result = process_comics_task()
    post_task(process_result)


# Instantiate the DAG
dag = ingest_pipeline()
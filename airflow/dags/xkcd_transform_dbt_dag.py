from datetime import datetime, timedelta
import subprocess
import logging

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from xkcd.config import (
    DEFAULT_LOG_LEVEL,
    DBT_PROJECT_DIR,
    DEFAULT_MODEL_NAME,
)

logger = logging.getLogger(__name__)
logger.setLevel(DEFAULT_LOG_LEVEL)

# Default configuration
default_args = {
    'owner': 'Qingxian',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dbt_models_run_and_test',
    default_args=default_args,
    description='Execute DBT transformations for XKCD data pipeline',
    schedule_interval='30 8 * * 1,3,5',  # Run at 8:30 AM on Monday, Wednesday, Friday。 30 minutes after the ingestion DAG
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'xkcd'],
    params={
        'model_name': Param(
            default=DEFAULT_MODEL_NAME,
            type='string',
            description='DBT model(s) to run (use "+" for downstream models)'
        )
    }
)
def dbt_xkcd_transformation_dag():
    """
    DAG for running DBT transformations on XKCD comic data.

    This DAG is designed to be triggered after new data is ingested.
    It runs DBT models with the specified selector pattern and
    ensures that data flows correctly through the transformation layers.
    """

    @task
    def run_dbt_models(model_name: str) -> bool:
        """
        Run the specified DBT models.

        Args:
            model_name: Name of the DBT model(s) to run
        Returns:
            dict: Execution information including command and output
        Raises:
            Exception: If DBT run fails
        """

        logging.info(f"DBT Project Directory: {DBT_PROJECT_DIR}")
        logging.info(f"Running model: {model_name}")

        # Build dbt run command
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --models {model_name}"
        logging.info(f"Executing DBT run command: {cmd}")

        # Execute command and capture output
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        # Log command execution results
        logging.info(f"Command exit code: {result.returncode}")
        logging.info(f"Command stdout: {result.stdout}")

        # Check for success
        if result.returncode != 0:
            raise Exception(f"DBT run failed: {result.stderr}")

        logging.info("DBT run completed successfully")
        return True

        # # Execute command and capture output
        # result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        #
        # # Check for success
        # if result.returncode != 0:
        #     logging.error(f"DBT run failed: {result.stderr}")
        #     raise Exception(f"DBT run failed: {result.stderr}")
        #
        # logging.info("DBT run completed successfully")
        #
        # return True

    @task
    def test_dbt_models(model_name: str) -> bool:
        """
        Test the specified DBT models to ensure data quality.

        Args:
            model_name: Name of the DBT model(s) to test
        Returns:
            dict: Test results information
        Raises:
            Exception: If DBT tests fail
        """
        cmd = f"cd {DBT_PROJECT_DIR} && dbt test --models {model_name}"
        logging.info(f"Executing DBT test command: {cmd}")

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        # Log command execution results
        logging.info(f"Command exit code: {result.returncode}")
        logging.info(f"Command stdout: {result.stdout}")

        if result.returncode != 0:
            raise Exception(f"DBT tests failed: {result.stderr}")

        logging.info("DBT tests completed successfully")

        return True

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_execution_results() -> None:
        """Log execution results for monitoring and auditing."""
        logging.info(f"DBT transformation pipeline completed successfully")

    # Get parameters from context
    model_name = "{{ params.model_name }}"

    # Build task flow
    run_result = run_dbt_models(model_name)
    test_result = test_dbt_models(model_name)
    execution_log = log_execution_results()

    # Set dependencies
    run_result >> test_result >> execution_log


# Instantiate the DAG
dbt_xkcd_transformation_dag = dbt_xkcd_transformation_dag()
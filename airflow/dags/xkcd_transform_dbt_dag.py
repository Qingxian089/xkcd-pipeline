from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import subprocess
import json
import logging

# Default configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


DBT_PROJECT_DIR = '/opt/airflow/dbt/xkcd_analytics'
DEFAULT_MODEL_NAME = 'dim_comics+'  # Default: Run dim_comics and all downstream models


@dag(
    dag_id='dbt_xkcd_transformation',
    default_args=default_args,
    description='Execute DBT transformations for XKCD data pipeline',
    schedule_interval=None,  # This DAG is only triggered by the ingestion DAG
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'xkcd'],
    params={
        'model_name': Param(
            default=DEFAULT_MODEL_NAME,
            type='string',
            description='DBT model(s) to run (use "+" for downstream models)'
        ),
        'full_refresh': Param(
            default=False,
            type='boolean',
            description='Whether to fully refresh incremental models'
        )
    }
)
def dbt_xkcd_transformation():
    """
    DAG for running DBT transformations on XKCD data.

    This DAG performs a sequence of operations:
    1. Validates that specified models exist
    2. Runs the DBT models
    3. Tests the models to ensure data quality
    4. Logs execution results

    It can be triggered manually or by the data ingestion process.
    """

    @task(task_id='validate_dbt_models')
    def validate_dbt_models(model_name: str) -> bool:
        """Validate that the specified DBT models exist in the project."""
        logging.info(f"Validating DBT models: {model_name}")

        result = subprocess.run(
            f"cd {DBT_PROJECT_DIR} && dbt ls --models {model_name} --output json",
            shell=True, capture_output=True, text=True
        )
        # Check if command executed successfully
        if result.returncode != 0:
            logging.error(f"Error listing models: {result.stderr}")
            raise ValueError(f"Failed to list models: {result.stderr}")

        return True

    @task(task_id='run_dbt_models')
    def run_dbt_models(model_name: str, full_refresh: bool) -> dict:
        """
        Run the specified DBT models.

        Args:
            model_name: Name of the DBT model(s) to run
            full_refresh: Whether to fully refresh incremental models
        Returns:
            dict: Execution information including command and output
        Raises:
            Exception: If DBT run fails
        """
        full_refresh_flag = "--full-refresh" if full_refresh else ""

        # Build dbt run command
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --models {model_name} {full_refresh_flag}"
        logging.info(f"Executing DBT run command: {cmd}")

        # Execute command and capture output
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        # Check for success
        if result.returncode != 0:
            logging.error(f"DBT run failed: {result.stderr}")
            raise Exception(f"DBT run failed: {result.stderr}")

        logging.info("DBT run completed successfully")

        # Return result information
        return {
            "command": cmd,
            "stdout": result.stdout,
            "success": True
        }

    @task(task_id='test_dbt_models')
    def test_dbt_models(model_name: str) -> dict:
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
        if result.returncode != 0:
            logging.error(f"DBT tests failed: {result.stderr}")
            raise Exception(f"DBT tests failed: {result.stderr}")

        logging.info("DBT tests completed successfully")

        return {
            "success": True,
            "stdout": result.stdout
        }

    @task(task_id='log_execution_results')
    def log_execution_results(run_result: dict, test_result: dict) -> None:
        """
        Log execution results for monitoring and auditing.

        Args:
            run_result: Results from the DBT run step
            test_result: Results from the DBT test step
        """
        logging.info(f"DBT transformation pipeline completed successfully")
        logging.info(f"Run command: {run_result['command']}")
        logging.info(f"Tests completed: {test_result['success']}")

    # Get parameters from context
    model_name = "{{ params.model_name }}"
    full_refresh = "{{ params.full_refresh }}"

    # Build task flow
    validation = validate_dbt_models(model_name)
    run_result = run_dbt_models(model_name, full_refresh)
    test_result = test_dbt_models(model_name)
    execution_log = log_execution_results(run_result, test_result)

    # Set dependencies
    validation >> run_result >> test_result >> execution_log


# Instantiate the DAG
dbt_xkcd_transformation_dag = dbt_xkcd_transformation()
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from xkcd.hooks.xkcd_hook import XKCDHook
from xkcd.utils.comic_parser import ComicParser

"""
DAG to sync latest XKCD comic daily
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_latest_comic(**context):
    """
    Fetch the latest XKCD comic
    Returns the parsed comic data
    """
    hook = XKCDHook()

    # Get latest comic number
    comic_num = hook.get_latest_comic_num()
    context['task_instance'].xcom_push(key='comic_num', value=comic_num)

    # Fetch comic data
    raw_data = hook.get_comic_by_num(comic_num)
    if not raw_data:
        raise Exception(f"Failed to fetch comic #{comic_num}")

    # Parse comic data
    parsed_data = ComicParser.parse_comic(raw_data)
    if not parsed_data:
        raise Exception(f"Failed to parse comic #{comic_num}")

    return parsed_data


def process_comic_data(**context):
    """
    Process the fetched comic data
    Here we can add database storage logic later
    """
    ti = context['task_instance']
    comic_data = context['task_instance'].xcom_pull(task_ids='fetch_latest')
    comic_num = ti.xcom_pull(key='comic_num')

    print(f"Processing comic #{comic_num}: {comic_data['title']}")
    # TODO: Add database storage logic

    return comic_data


with DAG(
        'sync_latest_xkcd',
        default_args=default_args,
        description='Sync latest XKCD comic daily',
        schedule_interval='@daily',
        start_date=datetime(2024, 4, 15),
        catchup=False,
        tags=['xkcd'],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_latest',
        python_callable=fetch_latest_comic,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id='process_comic',
        python_callable=process_comic_data,
        provide_context=True,
    )

    fetch_task >> process_task
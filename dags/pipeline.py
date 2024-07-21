from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.S3_hook import S3Hook # type: ignore

import csv

default_args = {
    'owner': 'john',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}


def clean_data(**context) -> list[dict[str, str]]:
    task_instance = context['task_instance']
    response: dict[str, any] = task_instance.xcom_pull(task_ids="fetch_trending")
    if not response or 'results' not in response:
        raise ValueError("No valid data fetched from the API")
    
    fetched_data: list[dict[str, str]] = []
    for movie in response['results'][:10]:  # Limit to top 10 movies
        title = movie.get('title', 'N/A')
        overview = movie.get('overview', 'N/A')
        release_date = movie.get('release_date', 'N/A')
        fetched_data.append({
            'Title': title,
            'Overview': overview,
            'Release_Date': release_date
        })
    
    return fetched_data


def write_to_csv(**context) -> None:
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    clean_data: list[dict[str, str]] = task_instance.xcom_pull(task_ids="clean_data_tsk")
    if not clean_data:
        raise ValueError("No data retrieved from XCom")
    
    date_str = execution_date.strftime('%Y%m%d')
    csv_file_path = f"/opt/airflow/Data/popular_movies_{date_str}.csv"

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['Title', 'Overview', 'Release_Date'])
        writer.writeheader()
        writer.writerows(clean_data)


def upload_to_s3(**context) -> None:
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y%m%d')
    filename = f"/opt/airflow/Data/popular_movies_{date_str}.csv"
    key = f"popular_movies_{date_str}.csv"
    
    hook = S3Hook('aws_conn')
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name="popularmoviesetl",
        replace=True
    )

    
with DAG(dag_id="fetch_trending_movies", start_date=datetime(2024, 7, 21), 
         schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    fetch_movies_task = SimpleHttpOperator(
        task_id="fetch_trending",
        method="GET",
        http_conn_id="tmdb_api_conn", 
        endpoint="/trending/movie/day?language=en-US",
        headers={"accept": "application/json"},
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    clean_data_fetched = PythonOperator(
        task_id="clean_data_tsk",
        python_callable=clean_data,
        provide_context=True
    )

    save_csv_file = PythonOperator(
        task_id="save_data",
        python_callable=write_to_csv,
        provide_context=True
    )

    load_to_aws = PythonOperator(
        task_id="load_csv_file",
        python_callable=upload_to_s3,
        provide_context=True
    )

    fetch_movies_task >> clean_data_fetched >> save_csv_file >> load_to_aws

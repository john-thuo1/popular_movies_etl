from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from airflow.models import DagBag # type: ignore
from unittest import mock, patch
from airflow.providers.http.operators.http import SimpleHttpOperator # type: ignore
from unittest.mock import Mock, patch
import os
import csv
from dags.pipeline import clean_data, upload_to_s3, write_to_csv



# def test_s3_connection():
#     hook = S3Hook(aws_conn_id='minio_conn')
#     bucket_name = os.getenv("MINIO_BUCKET_NAME")
#     if not bucket_name:
#         raise ValueError("MINIO_BUCKET_NAME environment variable is not set or is empty")
    
#     # Check if bucket exists
#     if bucket_name in hook.list_buckets():
#         print(f"Bucket {bucket_name} exists.")
#     else:
#         print(f"Bucket {bucket_name} does not exist.")

# test_s3_connection()


def test_dag_structure():
    dagbag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
    dag = dagbag.get_dag(dag_id='fetch_trending_movies')
    
    assert dag is not None
    assert len(dag.tasks) == 4

    fetch_movies_task = dag.get_task('fetch_trending')
    clean_data_task = dag.get_task('clean_data_tsk')
    save_data_task = dag.get_task('save_data')
    load_csv_task = dag.get_task('load_csv_file')
    
    assert fetch_movies_task.downstream_task_ids == {'clean_data_tsk'}
    assert clean_data_task.downstream_task_ids == {'save_data'}
    assert save_data_task.downstream_task_ids == {'load_csv_file'}

def test_clean_data():
    task_instance = Mock()
    task_instance.xcom_pull.return_value = {
        'results': [
            {'title': 'Movie 1', 'overview': 'Overview 1', 'release_date': '2024-07-01'},
            {'title': 'Movie 2', 'overview': 'Overview 2', 'release_date': '2024-07-02'}
        ]
    }
    cleaned_data = clean_data(task_instance)
    assert cleaned_data == [
        {'Title': 'Movie 1', 'Overview': 'Overview 1', 'Release_Date': '2024-07-01'},
        {'Title': 'Movie 2', 'Overview': 'Overview 2', 'Release_Date': '2024-07-02'}
    ]

def test_write_to_csv(tmpdir):
    task_instance = Mock()
    task_instance.xcom_pull.return_value = [
        {'Title': 'Movie 1', 'Overview': 'Overview 1', 'Release_Date': '2024-07-01'},
        {'Title': 'Movie 2', 'Overview': 'Overview 2', 'Release_Date': '2024-07-02'}
    ]
    
    csv_file_path = tmpdir.join("popular_movies.csv")
    
    with patch("builtins.open", new_callable=lambda: csv_file_path.open("w")):
        write_to_csv(task_instance)
    
    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        assert rows == [
            {'Title': 'Movie 1', 'Overview': 'Overview 1', 'Release_Date': '2024-07-01'},
            {'Title': 'Movie 2', 'Overview': 'Overview 2', 'Release_Date': '2024-07-02'}
        ]

@patch('airflow.hooks.S3_hook.S3Hook.load_file')
def test_upload_to_s3(mock_load_file):
    upload_to_s3()
    mock_load_file.assert_called_once_with(
        filename="/opt/airflow/Data/popular_movies.csv",
        key="popular_movies.csv",
        bucket_name="popularmoviesetl",
        replace=True
    )


# @patch("airflow.providers.http.operators.http.requests.Session.send")
# def test_fetch_movies_task(mock_send):
#     mock_response = Mock()
#     mock_response.json.return_value = {
#         'results': [
#             {'title': 'Movie 1', 'overview': 'Overview 1', 'release_date': '2024-07-01'}
#         ]
#     }
#     mock_send.return_value = mock_response
    
#     task_instance = Mock()
#     fetch_movies_task.execute(task_instance)
#     task_instance.xcom_push.assert_called_with(
#         key='return_value',
#         value={'results': [{'title': 'Movie 1', 'overview': 'Overview 1', 'release_date': '2024-07-01'}]}
#     )
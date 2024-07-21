from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from airflow.models import DagBag # type: ignore
from unittest import mock, patch
from airflow.providers.http.operators.http import SimpleHttpOperator # type: ignore
from unittest.mock import Mock, patch
import os
import csv
from dags.pipeline import clean_data, upload_to_s3, write_to_csv



def test_s3_connection():
    ...



def test_dag_structure():
    ...

def test_clean_data():
    ...

def test_write_to_csv(tmpdir):
    ...

@patch('airflow.hooks.S3_hook.S3Hook.load_file')
def test_upload_to_s3(mock_load_file):
    ...

@patch("airflow.providers.http.operators.http.requests.Session.send")
def test_fetch_movies_task(mock_send):
    ...
import glob
import os
import zipfile
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def file_name(url):
    return os.path.basename(urlparse(url).path).lower()


target_folder = "/var/data"
last_update_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
last_update_file_path = "{0}/{1}".format(target_folder, file_name(last_update_url))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'gdelt',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=30),
)


def download_last_update_csvs(last_update_file_path, target_folder):
    opened_file = open(last_update_file_path, 'r')
    lines = opened_file.readlines()

    for line in lines:
        _, _, csv_url = line.rstrip('\n').split(' ')
        download_file(csv_url, target_folder)


def download_file(url, target_folder):
    downloaded_file = requests.get(url)
    target_file = "{0}/{1}".format(target_folder, file_name(url))
    open(target_file, 'wb').write(downloaded_file.content)


def unzip_file(zip_file_path, target_folder):
    with zipfile.ZipFile(zip_file_path) as zip_ref:
        zip_ref.extractall(target_folder)


def unzip_all_csv(source_folder, target_folder):
    zipped_csv_files = glob.glob("{0}/*.csv.zip".format(source_folder))
    for file_path in zipped_csv_files:
        unzip_file(file_path, target_folder)
        os.remove(file_path)


clean_up_task = BashOperator(
    task_id='clean_up_task',
    bash_command="rm -rf /var/data/*",
    dag=dag,
)

last_update_index_task = PythonOperator(
    task_id='last_update_index_task',
    python_callable=download_file,
    op_kwargs={'url': last_update_url, 'target_folder': target_folder},
    dag=dag,
)

download_last_update_csvs_task = PythonOperator(
    task_id='download_last_update_csvs_task',
    python_callable=download_last_update_csvs,
    op_kwargs={'last_update_file_path': last_update_file_path, 'target_folder': target_folder},
    dag=dag,
)

unzip_csv_task = PythonOperator(
    task_id='unzip_csv_task',
    python_callable=unzip_all_csv,
    op_kwargs={'source_folder': target_folder, 'target_folder': target_folder},
    dag=dag,
)

clean_up_task >> last_update_index_task >> download_last_update_csvs_task >> unzip_csv_task

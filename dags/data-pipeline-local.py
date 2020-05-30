import sys

from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.timezone import datetime

sys.path.append("/usr/local/airflow")

from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG

from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from storage.send_message_to_kafka import send_message_to_gdelk_topics
from ingest.downloader import downloader

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


default_args = {
    # 'start_date': days_ago(2),
    # 'start_date': datetime(2015, 2, 18, 23),
    'start_date': datetime(2020, 5, 29, 2, 30),
    # 'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=20),
    'provide_context': True,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    dag_id='gdelt-data-pipeline',
    default_args=default_args,
    description='data micro-project',
    schedule_interval=timedelta(minutes=15),
)


def pre_set_subdag(parent_dag_name, child_dag_name):
    subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args,
        description='initialize some configuration',
        schedule_interval='@once',
    )

    # 增加一些连接配置
    set_up_env = BashOperator(
        task_id="create_spark_ssh_conn",
        bash_command='bash /usr/local/airflow/scripts/set_up_spark_ssh_connection.zsh',
        dag=subdag
    )

    return subdag


pre_set_tasks = SubDagOperator(
    task_id='pre_set_tasks_1',
    subdag=pre_set_subdag(dag.dag_id, 'pre_set_tasks_1'),
    dag=dag,
)


def downloader_push(**kwargs):
    """Pushes an XCom without a specific target"""
    csv_files = downloader()
    print(csv_files)
    kwargs['ti'].xcom_push(key='csv_files', value=csv_files)


download_csv = PythonOperator(
    task_id='download_csv_file',
    python_callable=downloader_push,
    dag=dag
)


def send_csv_files_to_kafka(**kwargs):
    ti = kwargs['ti']
    csv_files = ti.xcom_pull(key='csv_files', task_ids='download_csv_file')
    assert isinstance(csv_files, list)
    send_message_to_gdelk_topics(csv_files, kafka_host='kafka:9092')


send_message_kafka = PythonOperator(
    task_id='send_csv_files_to_kafka',
    python_callable=send_csv_files_to_kafka,
    dag=dag
)


def pre_set_subdag(parent_dag_name, child_dag_name):
    subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args,
        description='initialize some configuration',
        schedule_interval='@once',
    )

    # 增加一些连接配置
    remote_spark_submit = SSHOperator(
        ssh_conn_id='spark.dc.ssh',
        task_id='remote_spark_submit',
        command="""/root/.sdkman/candidates/spark/current/bin/spark-submit \
                    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.elasticsearch:elasticsearch-spark-20_2.11:7.6.0 \
                    --master spark://spark.dc:7077 \
                    file:///spark/processing/process_message_from_kafka_to_es.py kafka es
                """,
        dag=subdag)

    return subdag


streaming_processing_tasks = SubDagOperator(
    task_id='streaming_processing_task_1',
    subdag=pre_set_subdag(dag.dag_id, 'streaming_processing_task_1'),
    dag=dag,
)

pre_set_tasks >> download_csv >> send_message_kafka >> streaming_processing_tasks

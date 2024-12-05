from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import random
import requests
import shutil
import string
import time


with DAG(
    dag_id='copy-learning-data-to-nas',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        # 'retry_delay': timedelta(minutes=5),        
    },
    description="Mildang Sample Data Collector",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=['aidt', 'mildang', 'copy'],
) as dag:

    def validate_student_learning_data(ti):
        nas_tmp_directory = "/nas_root/aidt-tmp/{}".format(dag.dag_id)
        os.makedirs(nas_tmp_directory, exist_ok=True)
        
        files = [f for f in os.listdir(nas_tmp_directory) if os.path.isdir(f)]
        ti.xcom_push(key='results_student_learning_data', value=files)


    validate_data = PythonOperator(
        task_id='collect_data',
        python_callable=validate_student_learning_data,
        dag=dag
    )

    def copy_student_learning_data(ti):
        nas_directory = "/nas_root/aidt/{}".format(dag.dag_id)
        os.makedirs(nas_directory, exist_ok=True)

        collect_result = ti.xcom_pull(task_ids='collect_data', key='results_student_learning_data')
        for f in collect_result:
            head, tail = os.path.split(f)
            shutil.copyfile(f, f'{nas_directory}/{tail}')
            os.remove(f)

    copy_result = PythonOperator(
        task_id="process_result",
        python_callable=copy_student_learning_data
    )

    validate_data >> copy_result

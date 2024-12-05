from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging
import os
import requests
import string
import time


with DAG(
    dag_id='collect-student-learning-data',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        # 'retry_delay': timedelta(minutes=5),        
    },
    description="Mildang Sample Data Collector",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=['aidt', 'mildang', 'collect'],
) as dag:
    logger = logging.getLogger(__name__)
    # Add Connection
    # Conn Id: api_mildang_conn
    # Conn Type: HTTP
    # Host: https://api.mildang.kr/
    token = "bWlsZGFuZ3N0YWNrZXI=.e10e4c67875d4025a043b52a347906ab.61d8d51b8acd3223267c0e487fd95c917a1c0598bf001dcf590bd00e5ed97861"
    chars = string.ascii_lowercase + string.digits
    # trx_id = ''.join(random.choice(chars) for _ in range(8)) + str(datetime.fromtimestamp(time.time()))
    trx_id = str(datetime.fromtimestamp(time.time()))
    def collect_student_learning_data(ti):
        base_url = "https://api.mildang.kr/v1/student-learning-data"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;UTF-8",
            "Content-Type": "application/json"
        }
        params = {
            "trx": trx_id
        }
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code == 200:
            total_results = []
            data = response.json()
            logger.debug("{}".format(data))
            for item in data:
                total_results.extend(item)
            ti.xcom_push(key='transaction_id', value=trx_id)
            ti.xcom_push(key='results_student_learning_data', value=total_results)
            logger.info("Mildang API Call Success - [{}] {}".format(response.status_code, len(total_results)))
        else:
            logger.info("Mildang API Call Failed - [{}] {}".format(response.status_code, response.reason))

    collect_data = PythonOperator(
        task_id='collect_data',
        python_callable=collect_student_learning_data,
        dag=dag
    )

    __all__ = ['processing']
    def processing(ti):
        collect_result = ti.xcom_pull(task_ids='collect_data', key='results_student_learning_data')
        print(f"search_result: {collect_result}")
        # nas_directory = "/nas_root/aidt/{}".format(dag.dag_id)
        nas_tmp_directory = "/nas_root/aidt-tmp/{}".format(dag.dag_id)
        os.makedirs(nas_tmp_directory, exist_ok=True)
        for item in collect_result:
            id = item["id"]
            with open(f'{nas_tmp_directory}/{trx_id}_{id}.json', 'w') as f:
                json.dump(item, f)

        # postgres_hook = PostgresHook(postgres_conn_id='db_sql')
        # for index, item in collect_result.iterrows():
        #     sql = """
        #     INSERT INTO student_learning_data (id, created_at, updated_at, title, learning_state, type, difficulty,	correct_type, user_id)
        #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        #     """
        #     params = (item['id'], item['created_at'], item['updated_at'], item['title'], item['learning_state'], item['type'], item['difficulty'], item['correct_type'], item['user_id'])
        #     postgres_hook.run(sql, parameters=params)

    process_result = PythonOperator(
        task_id="process_result",
        python_callable=processing
    )

    collect_data >> process_result

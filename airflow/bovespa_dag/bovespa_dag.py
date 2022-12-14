from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bovespa_main import etl_function, etl_folders, etl_bucket

default_args = {
    'owner': 'tony',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 9),
    'email': ['almneto@estudante.ufla.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bovespa_dag',
    default_args=default_args,
    description = 'First bovespa dag'
)

create_etl_buckets = PythonOperator(
    task_id='create_bucket',
    python_callable=etl_bucket,
    dag=dag
)

create_etl_folders = PythonOperator(
    task_id='create_folders',
    python_callable=etl_folders,
    dag=dag
)

run_etl = PythonOperator(
    task_id='bovespa_etl',
    python_callable=etl_function, 
    dag=dag
)

create_etl_buckets >> create_etl_folders >> run_etl

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bovespa_main import etl_function

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

run_etl = PythonOperator(
    task_id='bovespa_etl',
    python_callable=etl_function, 
    dag=dag
)

run_etl

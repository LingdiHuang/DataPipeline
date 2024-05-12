from airflow import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import main

default_args = {
    'owner': 'lhuang',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid_dag',
    default_args=default_args,
    description='A simple Airflow DAG for pulling and loading covid data',
    schedule_interval=timedelta(days=1),
)

pull_data_task = PythonOperator(
    task_id='pull_data_task',
    python_callable=main.fetch_raw_data,
    provide_context=True,
    op_kwargs={'start_date': '{{ ds }}', 'end_date': '{{ ds }}'},  # Specify start_date and end_date
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=main.load_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
pull_data_task >> load_data_task
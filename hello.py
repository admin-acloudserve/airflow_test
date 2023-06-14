from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval='0 0 * * *'  # Executes daily at midnight
)

def extract_data():
    # Your data extraction logic here
    print("Extracting data...")

def transform_data():
    # Your data transformation logic here
    print("Transforming data...")

def load_data():
    # Your data loading logic here
    print("Loading data...")

with dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task

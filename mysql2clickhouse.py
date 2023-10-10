from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mysql.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 14),
    'retries': 1
}

dag = DAG(
    'sample_dag2',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval=None,
    catchup=False
)

from sqlalchemy import create_engine

def get_connection():
    connection_string = "mysql+mysqlconnector://airflow_user:airflow_pass@my-db-pxc-db-haproxy.xtradb.svc.cluster.local:3306/airflow_db"
    engine = create_engine(connection_string, echo=True)

    with engine.connect() as connection:

        result = connection.execute("SELECT dag_id FROM dag;")

        print(result)
        print(type(result))
        for row in result:
            print(row)




with dag:
    extract_task = PythonOperator(
        task_id='get_connection',
        python_callable=get_connection
    )
    extract_task 

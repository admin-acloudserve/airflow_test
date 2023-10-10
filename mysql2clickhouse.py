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

def get_mysql_connection():
    connection_string = "mysql+mysqlconnector://airflow_user:airflow_pass@my-db-pxc-db-haproxy.xtradb.svc.cluster.local:3306/airflow_db"
    engine = create_engine(connection_string, echo=True)

    with engine.connect() as connection:

        result = connection.execute("SELECT dag_id FROM dag")

        print(result)
        print(type(result))
        for row in result:
            print(row)

import clickhouse_connect
def get_clickhouse_connection():
    client = clickhouse_connect.get_client(host='172.20.172.230', port=9004, username='default', password='vkl5Qb0ybh')
    client.command('CREATE TABLE new_table (dag_id String) ENGINE MergeTree')
    data = [['new_dag1'], ['new_dag2']]
    client.insert('new_table', data, column_names=['dag_id'])
    result = client.query('SELECT * FROM new_table')

    print(result.result_rows)




with dag:
    get_mysql_connection = PythonOperator(
        task_id='get_mysql_connection',
        python_callable=get_mysql_connection
    )

    get_clickhouse_connection = PythonOperator(
        task_id='get_clickhouse_connection',
        python_callable=get_clickhouse_connection
    )


    get_mysql_connection > get_clickhouse_connection

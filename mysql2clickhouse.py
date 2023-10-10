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
# caonma
from clickhouse_driver import Client
def get_clickhouse_connection():
    host = 'a4053cf1292824fcdbc1af7886ad0420-1236332051.us-east-1.elb.amazonaws.com'
    port = 9000  # Default ClickHouse port
    database = 'system'
    user = 'default'
    password = 'vkl5Qb0ybh'

    # Create a ClickHouse client
    client = Client(host=host, port=port, database=database, user=user, password=password)

    # Define your SQL query
    query = 'select * from INFORMATION_SCHEMA.tables;;'

    try:
        # Execute the query
        result = client.execute(query)

        # Process the result
        for row in result:
            print(row)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the ClickHouse connection when done
        client.disconnect()
    




with dag:
    get_mysql_connection = PythonOperator(
        task_id='get_mysql_connection',
        python_callable=get_mysql_connection
    )

    get_clickhouse_connection = PythonOperator(
        task_id='get_clickhouse_connection',
        python_callable=get_clickhouse_connection
    )


    get_mysql_connection >> get_clickhouse_connection

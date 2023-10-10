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

def get_connection():
    mydb = mysql.connector.connect(
        host="http://172.20.51.218",
        user="airflow_user",
        password="airflow_pass",
        database="airflow_db"
    )

    mycursor = mydb.cursor()

    mycursor.execute("SELECT dag_id FROM dag")

    myresult = mycursor.fetchall()

    print(type(myresult))
    for x in myresult:
        print(x)

with dag:
    extract_task = PythonOperator(
        task_id='get_connection',
        python_callable=get_connection
    )
    extract_task 

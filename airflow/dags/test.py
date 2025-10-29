from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

def test():
    print("Hello World")


with DAG(
    dag_id='test_dag',
    start_date=datetime.datetime(2025, 10, 29),
    schedule_interval="@daily"
) as dag:
    test_dag = PythonOperator(
        task_id="test",
        python_callable=test
    )
    
    test_dag

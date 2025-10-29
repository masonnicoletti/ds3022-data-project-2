# airflow DAG goes here
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime
import time
import requests
import boto3
import pandas as pd

# Declare global variables
api_endpoint = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/"
computing_id = "cxx6sw"
submission_queue = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
submission_message = "Data Project 2 Submission"
platform = "airflow"

@dag(
    dag_id='dp2_quote_assembler',
    start_date=datetime.datetime(2025, 10, 29),
    schedule="@hourly",
    catchup=False
)

def quote_assembler():

    # Define tasks

    @task
    def create_api_url(api_endpoint, computing_id):
        # Create API Endpoint URL
        api_url = api_endpoint + computing_id
        return api_url
    
    @task
    def get_queue_url(api_url):
        #logger = get_run_logger()
        try:
            # Request Queue URL
            payload = requests.post(api_url).json()
            queue_url = payload['sqs_url']
            #logger.info("Retrieved SQS queue URL")
        
        except Exception as e:
            #logger.error(f"Error retrieving SQS queue URL: {e}")
            raise e
        
        return queue_url
    
    # Define dependencies
    api_url = create_api_url(api_endpoint, computing_id)
    queue_url = get_queue_url(api_url)


quote_assembler()
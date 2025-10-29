from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import datetime
import requests
import boto3

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
    
    @task
    def get_queue_attributes(queue_url):
        #logger = get_run_logger()
        try:
            # Get queue attributes
            sqs = boto3.client('sqs', region_name='us-east-1')
            attributes = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
                )
        
        except Exception as e:
            #logger.error(f"Error obtaining queue attributes: {e}")
            raise e
        
        return attributes
    
    @task
    def parse_queue_attributes(attributes):
        #logger = get_run_logger()
        try:
            # Parse response
            num_messages = int(attributes["Attributes"]['ApproximateNumberOfMessages'])
            delayed_messages = int(attributes["Attributes"]["ApproximateNumberOfMessagesDelayed"])
            messages_not_visible = int(attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
            total_messages = num_messages + delayed_messages + messages_not_visible
        
        except Exception as e:
            #logger.error(f"Error parsing queue attributes: {e}")
            raise e
        
        return total_messages, num_messages

    @task
    def receive_message(queue_url):
        #logger = get_run_logger()
        try:
            # Get message and attributes from SQS queue
            sqs = boto3.client('sqs', region_name='us-east-1')
            message = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MaxNumberOfMessages=1,
                VisibilityTimeout=60,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=10
                )
            
            # Error handling in case of null reply to receive_message
            if message is None:
                #logger.error("No messages available in queue")
                return None

        except Exception as e:
            #logger.error(f"Error retrieving SQS message: {e}")
            raise e
        
        return message


    api_url = create_api_url(api_endpoint, computing_id)
    queue_url = get_queue_url(api_url)
    attributes = get_queue_attributes(queue_url)
    total_messages, num_messages = parse_queue_attributes(attributes)
    message = receive_message(queue_url)


quote_assembler()

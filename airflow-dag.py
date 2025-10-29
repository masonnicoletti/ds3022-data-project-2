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
    
    '''
    @task
    def parse_queue_attributes(attributes):
        #logger = get_run_logger()
        try:
            # Parse response
            num_messages = int(attributes["Attributes"]['ApproximateNumberOfMessages'])
            delayed_messages = int(attributes["Attributes"]["ApproximateNumberOfMessagesDelayed"])
            messages_not_visible = int(attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
            total_messages = num_messages + delayed_messages + messages_not_visible
            parsed_attributes = {"total_messages": total_messages, "num_messages": num_messages}
        
        except Exception as e:
            #logger.error(f"Error parsing queue attributes: {e}")
            raise e
        
        return parsed_attributes

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
    
    @task
    def parse_message(message):
        #logger = get_run_logger()
        try:
            # Extract message information and append to list
            order_no = int(message['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
            word = message['Messages'][0]['MessageAttributes']['word']['StringValue']
            message_entry = {"order_no": order_no, "word": word}

            # Extract receipt handle
            receipt_handle = message['Messages'][0]['ReceiptHandle']
        
        except Exception as e:
            #logger.error(f"Error parsing SQS message: {e}")
            raise e
        
        parsed_message = {"message_entry": message_entry, "receipt_handle": receipt_handle} 
        return parsed_message
    
    @task
    def delete_message(queue_url, receipt_handle):
        #logger = get_run_logger()
        try:
            # Delete message
            sqs = boto3.client('sqs', region_name='us-east-1')
            deletion = sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        
        except Exception as e:
            #logger.error(f"Error deleting SQS message: {e}")
            raise e
    
    @task
    def save_message(messages_list):
        #logger = get_run_logger()
        try:
            # Convert messages to pandas dataframe
            messages_df = pd.DataFrame(messages_list)

            # Sort entries by order number
            messages_df = messages_df.sort_values(by="order_no", ascending=True)

            # Save as  csv file
            messages_df.to_csv('messages.csv', index=False)
            #logger.info("Message saved as csv file locally")
        
        except Exception as e:
            #logger.error(f"Error saving SQS messages as file: {e}")
            raise e

        return messages_df
    
    @task
    def assemble_quote(messages_df):
        #logger = get_run_logger()
        try:
            # Loop through words and concatenate
            quote = ""
            for word in messages_df['word']:
                quote += word + " "
            print((f'Quote: "{quote}"'))
            #logger.info(f'Quote: "{quote}"')
        
        except Exception as e:
            #logger.error(f"Error assembling SQS messages into quote: {e}")
            raise e 

        return quote
    
    @task
    def send_solution(submission_queue, submission_message, quote, computing_id, platform):
        #logger = get_run_logger()
        try:
            # Send message to submission queue
            sqs = boto3.client('sqs', region_name='us-east-1')
            send_response = sqs.send_message(
                QueueUrl=submission_queue,
                MessageBody=submission_message,
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': computing_id
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': quote
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': platform
                    }
                }
            )
            #logger.info("SQS message sent to submission queue")
        
        except Exception as e:
            #logger.error(f"Error sending SQS message: {e}")
            raise e
        
        return send_response
    
    @task 
    def parse_send_response(send_response):
        #logger = get_run_logger()
        try:
            # Parse metadata of sent message
            http_status_code = send_response['ResponseMetadata']['HTTPStatusCode']
            time_sent = send_response['ResponseMetadata']['HTTPHeaders']['date']
            
            # Confirm successful send
            if http_status_code == 200:
                print("Message received by submission queue")
            
            # Log time sent
            #logger.info(f"Submission Time: {time_sent}")
        
        except Exception as e:
            #logger.error(f"Error parsing SQS sent message response: {e}")
            raise e
    '''
    

    # Define dependencies
    api_url = create_api_url(api_endpoint, computing_id)
    queue_url = get_queue_url(api_url)
    attributes = get_queue_attributes(queue_url)

    '''
    while True:
        start_time = time.time()
        attributes = get_queue_attributes(queue_url)
        parsed_attributes = parse_queue_attributes(attributes)
        if parsed_attributes['num_messages'] == parsed_attributes['total_messages']:
            break
        elif time.time() - start_time > 900:
            break

    messages_list = []
    while len(messages_list) < parsed_attributes['total_messages']:
        message = receive_message(queue_url)
        message_attributes = parse_message(message)
        messages_list.append(message_attributes['message_entry'])
        delete_message(queue_url, message_attributes['receipt_handle'])
    
    message_df = save_message(messages_list)
    quote = assemble_quote(message_df)
    send_response = send_solution(submission_queue, submission_message, quote, computing_id, platform)
    parse_send_response(send_response)
    '''


quote_assembler()
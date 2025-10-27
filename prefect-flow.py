# prefect flow goes here
from prefect import task, flow
from prefect.logging import get_run_logger
import os
from prefect.types import TaskRetryDelaySeconds
import requests
import boto3
import time
import pandas as pd


# Declare global variables
api_endpoint = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/"
computing_id = "cxx6sw"
submission_queue = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
submission_message = "Data Project 2 Submission"
platform = "prefect"


@task
def create_api_url(api_endpoint, computing_id):
    # Create API Endpoint URL
    api_url = api_endpoint + computing_id
    return api_url


@task
def get_queue_url(api_url):
    # Request Queue URL
    try:
        payload = requests.post(api_url).json()
        queue_url = payload['sqs_url']
    except Exception as e:
        print(f"Error retrieving queue URL: {e}")
    
    return queue_url


@task
def get_queue_attributes(queue_url):
    # Create SQS Connection
    sqs = boto3.client('sqs')
    
    # Get queue attributes
    try:
        attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
            )
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return attributes


@task
def parse_queue_attributes(attributes):

    # Parse response
    num_messages = int(attributes["Attributes"]['ApproximateNumberOfMessages'])
    delayed_messages = int(attributes["Attributes"]["ApproximateNumberOfMessagesDelayed"])
    messages_not_visible = int(attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
    total_messages = num_messages + delayed_messages + messages_not_visible
    
    return total_messages, num_messages


@task
def receive_message(queue_url):
    
    # Create SQS Connection
    sqs = boto3.client('sqs') 
    
    # Get message and attributes from SQS queue
    message = sqs.receive_message(
        QueueUrl=queue_url,
        MessageSystemAttributeNames=['All'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=60,
        MessageAttributeNames=['All'],
        WaitTimeSeconds=10
        )
    
    return message


@task
def parse_message(message):
    
    # Extract message information and append to list
    order_no = int(message['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
    word = message['Messages'][0]['MessageAttributes']['word']['StringValue']
    message_entry = {"order_no": order_no, "word": word} 

    # Extract receipt handle
    receipt_handle = message['Messages'][0]['ReceiptHandle']

    return message_entry, receipt_handle


@task
def delete_message(queue_url, receipt_handle):
    
    # Create SQS Connection
    sqs = boto3.client('sqs')

    # Delete message
    deletion = sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


@task
def save_message(messages_list):
    
    # Convert messages to pandas dataframe
    messages_df = pd.DataFrame(messages_list)

    # Sort entries by order number
    messages_df = messages_df.sort_values(by="order_no", ascending=True)

    # Save as  csv file
    messages_df.to_csv('messages.csv', index=False)

    return messages_df


@task
def assemble_quote(messages_df):
    # Loop through words and concatenate
    quote = ""
    for word in messages_df['word']:
        quote += word + " "
    print(quote)

    return quote


@task
def send_solution(submission_queue, submission_message, quote, computing_id, platform):
    # Create SQS Connection
    sqs = boto3.client('sqs')

    # Send message to submission queue
    try:
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
    except Exception as e:
        print(f"Error sending message: {e}")
    
    return send_response


@task 
def parse_send_response(send_response):
    # Parse metadata of sent message
    http_status_code = send_response['ResponseMetadata']['HTTPStatusCode']
    time_sent = send_response['ResponseMetadata']['HTTPHeaders']['date']
    
    # Confirm successful send
    if http_status_code == 200:
        print("Message Received")
    # Log time sent
    print(f"Submission Time: {time_sent}")



@flow
def api_request(api_endpoint, computing_id):
    api_url = create_api_url(api_endpoint, computing_id)
    queue_url = get_queue_url(api_url)

    return queue_url



@flow 
def monitor_queue(queue_url):
    start_time = time.time()
    
    while True:
        attributes = get_queue_attributes(queue_url)
        total_messages, num_messages = parse_queue_attributes(attributes)
        print(f"Number of Messages Received: {num_messages}/{total_messages}")
        total_time = time.time() - start_time

        if num_messages == total_messages:
            break
        elif total_time > 900:
            break
        
        time.sleep(60) 
    
    return total_messages



@flow
def collect_messages(queue_url, total_messages):
    messages_list = []
    while len(messages_list) < total_messages:
        message = receive_message(queue_url)
        message_entry, receipt_handle = parse_message(message)
        messages_list.append(message_entry)
        delete_message(queue_url, receipt_handle)

    messages_df = save_message(messages_list)

    return messages_df



@flow
def submission(messages_df, submission_queue, submission_message, computing_id, platform):
    quote = assemble_quote(messages_df)
    send_response = send_solution(submission_queue, submission_message, quote, computing_id, platform)
    parse_send_response(send_response)



@flow
def quote_assembler(api_endpoint, computing_id, submission_queue, submission_message, platform):
    queue_url = api_request(api_endpoint, computing_id)
    total_messages = monitor_queue(queue_url)
    messages_df = collect_messages(queue_url, total_messages)
    submission(messages_df, submission_queue, submission_message, computing_id, platform)




if __name__ == "__main__":
    quote_assembler(api_endpoint, computing_id, submission_queue, submission_message, platform)

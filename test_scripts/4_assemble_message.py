import os
import requests
import boto3
import time
import pandas as pd

computing_id = "cxx6sw"
submission_queue = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
platform = "prefect"

messages_df = pd.read_csv("messages.csv")

quote = ""

for word in messages_df['word']:
    quote += word + " "

print(quote)

# Create SQS Connection
sqs = boto3.client('sqs')

submission_message = "Data Project 2 Submission"

try:
    response = sqs.send_message(
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


print(response)

http_status_code = response['ResponseMetadata']['HTTPStatusCode']
time_sent = response['ResponseMetadata']['HTTPHeaders']['date']

print(http_status_code)
print(time_sent)
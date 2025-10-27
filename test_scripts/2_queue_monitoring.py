import os
import requests
import boto3
import time

queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/cxx6sw"

sqs = boto3.client('sqs')


response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=['All']
)

print(response)

num_messages = int(response["Attributes"]['ApproximateNumberOfMessages'])
delayed_messages = int(response["Attributes"]["ApproximateNumberOfMessagesDelayed"])
messages_not_visible = int(response["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
total_messages = num_messages + delayed_messages + messages_not_visible
print(f"Total Number of Messages: {total_messages}")



if delayed_messages > 0:
    print(f"Number of Messages Received: {num_messages}")
    time.sleep(120)

print("All Messages Delivered")
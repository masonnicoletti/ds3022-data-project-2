import os
import requests
import boto3
import time

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/cxx6sw"

payload = requests.post(url).json()

print(payload)

queue_url = payload['sqs_url']

print(queue_url)

sqs = boto3.client('sqs')

response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=['All'],
    MessageSystemAttributeNames=['All'],
    MaxNumberOfMessages=10,
    MessageAttributeNames=['All'],
    WaitTimeSeconds=10)

print(response)

#handle = response['Messages'][0]
#print(handle)



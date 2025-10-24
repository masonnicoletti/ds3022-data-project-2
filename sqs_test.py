import os
import requests
import boto3
import time

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/cxx6sw"

payload = requests.post(url).json

sqs = boto3.client('sqs')

print(payload)

queue_url = payload['sqs_url']

print(queue_url)
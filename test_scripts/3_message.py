import os
import requests
import boto3
import time
import pandas as pd
total_messages = 21


queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/cxx6sw"

sqs = boto3.client('sqs')

messages_list = []

while len(messages_list) < total_messages:

    # Get message and attributes from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MessageSystemAttributeNames=['All'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=60,
        MessageAttributeNames=['All'],
        WaitTimeSeconds=10
        )

    # Extract information and append to list
    order_no = int(response['Messages'][0]['MessageAttributes']['order_no']['StringValue'])
    word = response['Messages'][0]['MessageAttributes']['word']['StringValue']
    messages_list.append({"order_no": order_no, "word": word})

    # Extract receipt handle
    receipt_handle = response['Messages'][0]['ReceiptHandle']

    # Delete message
    deletion = sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


# Convert messages to pandas dataframe
messages_df = pd.DataFrame(messages_list).sort_values(by="order_no", ascending=True)

# Sort entries by order number
messages_df = messages_df.sort_values(by="order_no", ascending=True)
print(messages_df)

# Save as  csv file
messages_df.to_csv('messages.csv', index=False)
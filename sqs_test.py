# sqs_test.py
import boto3
import json
import sys
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def send_message(queue_url: str, message_body: dict):
    sqs = boto3.client("sqs")
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        print(f"Message sent. MessageId: {response['MessageId']}")
    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials not found. Configure with `aws configure`.")

def receive_messages(queue_url: str, max_messages: int = 1, wait_time: int = 5):
    sqs = boto3.client("sqs")
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
        messages = response.get("Messages", [])
        if not messages:
            print("No messages available.")
            return
        for msg in messages:
            print(f"Received message: {msg['Body']}")
            # Delete after processing
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"]
            )
            print("Message deleted from queue.")
    except (NoCredentialsError, PartialCredentialsError):
        print("AWS credentials not found. Configure with `aws configure`.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python sqs_test.py <queue_url> <send|receive>")
        sys.exit(1)

    queue_url = sys.argv[1]
    action = sys.argv[2].lower()

    if action == "send":
        test_message = {"id": "123", "event": "line_crossing", "status": "test"}
        send_message(queue_url, test_message)
    elif action == "receive":
        receive_messages(queue_url)
    else:
        print("Action must be 'send' or 'receive'")

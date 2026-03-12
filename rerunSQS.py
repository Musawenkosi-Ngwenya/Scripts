#!/usr/bin/env python3
"""
Script to submit a message to the testDiscountsBatch SQS queue.
Queue ARN: arn:aws:sqs:us-east-1:389633136494:testDiscountsBatch
"""

import boto3
import json
from datetime import datetime

# Queue configuration
QUEUE_ARN = "arn:aws:sqs:us-east-1:389633136494:testDiscountsBatch"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/389633136494/testDiscountsBatch"
AWS_REGION = "us-east-1"

def build_message_body() -> str:
    """Build the message body to send to SQS."""
    timestamp = int(datetime.utcnow().timestamp() * 1000)
    return f"TESTSOMTATEST_DiscountValues_{timestamp}"


def send_sqs_message() -> dict:
    """Send a message to the SQS queue."""
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    message_body = 'TESTSOMTATEST_DiscountValues_1773249267402'

    print(f"Sending message to queue: {QUEUE_URL}")
    print(f"Message body: {message_body}")

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=message_body,
        MessageAttributes={},  # No message attributes (matches md5OfMessageAttributes: null)
    )

    return response


def main():
    response = send_sqs_message()

    print("\n✅ Message sent successfully!")
    print(f"  MessageId:      {response.get('MessageId')}")
    print(f"  MD5OfBody:      {response.get('MD5OfMessageBody')}")
    print(f"  SequenceNumber: {response.get('SequenceNumber', 'N/A')}")
    print(f"  HTTPStatus:     {response['ResponseMetadata']['HTTPStatusCode']}")

    # Print the full SQS event structure that the Lambda/consumer will receive
    print("\n📦 Expected SQS event received by consumer:")
    event = {
        "Records": [
            {
                "attributes": {
                    "ApproximateFirstReceiveTimestamp": str(int(datetime.utcnow().timestamp() * 1000)),
                    "ApproximateReceiveCount": "1",
                    "SenderId": "AIDAIL3CAZHF52UFCHULQ",
                    "SentTimestamp": str(int(datetime.utcnow().timestamp() * 1000)),
                },
                "awsRegion": AWS_REGION,
                "body": response.get("MessageId"),  # Will be actual body on consume
                "eventSource": "aws:sqs",
                "eventSourceARN": QUEUE_ARN,
                "md5OfBody": response.get("MD5OfMessageBody"),
                "md5OfMessageAttributes": None,
                "messageAttributes": {},
                "messageId": response.get("MessageId"),
                "receiptHandle": "<assigned-on-receive>",
            }
        ]
    }
    print(json.dumps(event, indent=4))


if __name__ == "__main__":
    main()
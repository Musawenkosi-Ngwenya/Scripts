#!/usr/bin/env python3
"""
📨 Generic SQS Message Sender

Prompts for everything needed to send a message to any SQS (incl. FIFO)
queue, on any AWS profile/account, then prints the send response plus the
expected Records[] event shape a Lambda/consumer would receive.

Usage:
    python send_sqs_message.py
"""

import json
import sys
import boto3
from datetime import datetime
from botocore.exceptions import ClientError, ProfileNotFound, NoCredentialsError

DEFAULT_REGION = "us-east-1"


# ── Small interactive helpers ───────────────────────────────────────────────

def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"➡️  {prompt}{suffix}: ").strip()
    return val or default


def say(msg: str) -> None:
    print(msg)


# ── Core logic ───────────────────────────────────────────────────────────────

def send_sqs_message(profile: str, region: str, queue_url: str, message_body: str, message_group_id: str = None) -> dict:
    """Send a message to the SQS queue, handling FIFO dedup automatically."""
    session = boto3.Session(profile_name=profile or None, region_name=region)
    sqs = session.client("sqs")

    say(f"📤 Sending message to queue: {queue_url}")
    say(f"📝 Message body: {message_body}")

    send_args = {
        "QueueUrl": queue_url,
        "MessageBody": message_body,
    }

    is_fifo = queue_url.endswith(".fifo")

    if is_fifo:
        if not message_group_id:
            say("⚠️  FIFO queue detected but no MessageGroupId provided — this will likely fail.")
        send_args["MessageGroupId"] = message_group_id

        say("🔍 Checking ContentBasedDeduplication setting …")
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ContentBasedDeduplication"],
        )
        content_based_dedup = attrs.get("Attributes", {}).get("ContentBasedDeduplication") == "true"

        if not content_based_dedup:
            send_args["MessageDeduplicationId"] = f"{message_body}-{int(datetime.utcnow().timestamp() * 1000)}"
            say("🆔 Content-based dedup is OFF — generated a MessageDeduplicationId.")
        else:
            say("🆔 Content-based dedup is ON — no MessageDeduplicationId needed.")

    say("🚀 Calling sqs.send_message …")
    response = sqs.send_message(**send_args)
    return response


def print_result(response: dict, queue_arn: str, region: str) -> None:
    say("\n✅ Message sent successfully!")
    say(f"  MessageId:      {response.get('MessageId')}")
    say(f"  MD5OfBody:      {response.get('MD5OfMessageBody')}")
    say(f"  SequenceNumber: {response.get('SequenceNumber', 'N/A')}")
    say(f"  HTTPStatus:     {response['ResponseMetadata']['HTTPStatusCode']}")

    say("\n📦 Expected SQS event received by consumer:")
    event = {
        "Records": [
            {
                "attributes": {
                    "ApproximateFirstReceiveTimestamp": str(int(datetime.utcnow().timestamp() * 1000)),
                    "ApproximateReceiveCount": "1",
                    "SenderId": "AIDAIL3CAZHF52UFCHULQ",
                    "SentTimestamp": str(int(datetime.utcnow().timestamp() * 1000)),
                },
                "awsRegion": region,
                "body": response.get("MessageId"),
                "eventSource": "aws:sqs",
                "eventSourceARN": queue_arn,
                "md5OfBody": response.get("MD5OfMessageBody"),
                "md5OfMessageAttributes": None,
                "messageAttributes": {},
                "messageId": response.get("MessageId"),
                "receiptHandle": "<assigned-on-receive>",
            }
        ]
    }
    print(json.dumps(event, indent=4))


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("📨  Generic SQS Message Sender")
    print("=" * 70)

    profile = ask("AWS profile (e.g. musa, musafc, musarisaas — blank = default)", "default")
    if profile.lower() == "default":
        profile = ""

    region = ask("AWS region", DEFAULT_REGION)
    queue_arn = ask("QUEUE_ARN")
    queue_url = ask("QUEUE_URL")
    message_body = ask("message_body")

    message_group_id = ""
    if queue_url.endswith(".fifo"):
        message_group_id = ask("MessageGroupId (required for FIFO queues)")
    else:
        message_group_id = ask("MessageGroupId (optional — only used for FIFO queues)", "")

    print()
    try:
        response = send_sqs_message(profile, region, queue_url, message_body, message_group_id)
    except ProfileNotFound:
        say(f"❌ AWS profile '{profile}' not found — check ~/.aws/config")
        sys.exit(1)
    except NoCredentialsError:
        say(f"❌ No AWS credentials found for profile '{profile or 'default'}'")
        sys.exit(1)
    except ClientError as e:
        say(f"❌ AWS error: {e.response['Error']['Message']}")
        sys.exit(1)

    print_result(response, queue_arn, region)
    print("\n🏁 Done.")


if __name__ == "__main__":
    main()
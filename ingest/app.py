import json
import os
import urllib.parse

import boto3

sqs = boto3.client("sqs")

ALLOWED_EXTS = (".jpg", ".jpeg", ".png")

def lambda_handler(event, context):
    queue_url = os.environ["QUEUE_URL"]

    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        etag = s3_info.get("object", {}).get("eTag") or s3_info.get("object", {}).get("etag")

        if not bucket or not key:
            continue

        key = urllib.parse.unquote_plus(key)

        if not key.startswith("incoming/"):
            continue

        lower = key.lower()
        if not lower.endswith(ALLOWED_EXTS):
            continue

        msg = {"bucket": bucket, "key": key, "etag": etag}
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(msg),
        )

    return {"ok": True}
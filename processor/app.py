import io
import json
import os

import boto3
from PIL import Image, ExifTags

s3 = boto3.client("s3")

def _metadata_key_for_image(image_key: str, metadata_prefix: str) -> str:
    rel = image_key[len("incoming/"):] if image_key.startswith("incoming/") else image_key
    return f"{metadata_prefix}{rel}.json"

def _s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def _extract_exif(img: Image.Image) -> dict:
    out = {}
    try:
        exif = img.getexif()
        if not exif:
            return out
        for tag_id, value in exif.items():
            tag = ExifTags.TAGS.get(tag_id, str(tag_id))
            # Keep it JSON-serializable
            try:
                json.dumps(value)
                out[tag] = value
            except TypeError:
                out[tag] = str(value)
    except Exception:
        return {}
    return out

def lambda_handler(event, context):
    metadata_prefix = os.environ.get("METADATA_PREFIX", "metadata/")

    for record in event.get("Records", []):
        body = record.get("body", "{}")
        msg = json.loads(body)

        bucket = msg["bucket"]
        key = msg["key"]
        etag = msg.get("etag")

        if not key.startswith("incoming/"):
            continue

        meta_key = _metadata_key_for_image(key, metadata_prefix)

        if _s3_object_exists(bucket, meta_key):
            continue

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        size_bytes = len(data)

        img = Image.open(io.BytesIO(data))
        width, height = img.size
        fmt = (img.format or "").lower()

        meta = {
            "source_bucket": bucket,
            "source_key": key,
            "etag": etag,
            "format": fmt,
            "width": width,
            "height": height,
            "file_size_bytes": size_bytes,
            "exif": _extract_exif(img),
        }

        s3.put_object(
            Bucket=bucket,
            Key=meta_key,
            Body=json.dumps(meta, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    return {"ok": True}
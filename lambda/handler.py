import os
import json
import csv
import logging
from io import StringIO
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from utils import normalize_indicator, put_metric_processed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

DDB_TABLE = os.environ.get("DDB_TABLE")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

if not DDB_TABLE:
    raise RuntimeError("DDB_TABLE env var not set")
if not SNS_TOPIC_ARN:
    raise RuntimeError("SNS_TOPIC_ARN env var not set")

table = dynamodb.Table(DDB_TABLE)

def parse_json(body: str):
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.exception("JSON decode error")
        raise
    # Expect either top-level list or {"indicators": [...]}
    if isinstance(payload, list):
        return payload
    return payload.get("indicators", [])

def parse_csv(body: str):
    reader = csv.DictReader(StringIO(body))
    return list(reader)

def write_item(indicator_value: str, indicator_type: str, source_file: str):
    try:
        table.put_item(
            Item={
                "indicator_value": indicator_value,
                "indicator_type": indicator_type,
                "source_file": source_file,
                "ingested_at": datetime.utcnow().isoformat()
            }
        )
    except ClientError:
        logger.exception("Failed to write to DynamoDB")
        raise

def publish_summary(count: int, source_file: str):
    try:
        message = {
            "source_file": source_file,
            "processed_indicators": count,
            "timestamp": datetime.utcnow().isoformat()
        }
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message),
            Subject="Threat Intel Ingest Summary"
        )
    except ClientError:
        logger.exception("Failed to publish SNS summary")
        raise

def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except Exception:
        logger.exception("Malformed event")
        raise

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8")

    indicators = []
    if key.lower().endswith(".json"):
        indicators = parse_json(raw)
    elif key.lower().endswith(".csv"):
        indicators = parse_csv(raw)
    else:
        logger.warning("Unsupported file type: %s", key)
        indicators = []

    processed = 0
    for item in indicators:
        # support different field names
        raw_value = item.get("value") or item.get("indicator") or item.get("indicator_value")
        itype = item.get("type") or item.get("indicator_type") or "unknown"

        if not raw_value:
            logger.warning("Skipping item without value: %s", item)
            continue

        value = normalize_indicator(raw_value)
        write_item(value, itype.lower(), key)
        processed += 1

    put_metric_processed(processed)
    publish_summary(processed, key)

    logger.info("Processed %d indicators from %s", processed, key)
    return {"statusCode": 200, "body": json.dumps({"processed": processed})}

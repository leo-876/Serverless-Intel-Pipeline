import re
import logging
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch = boto3.client("cloudwatch")

def normalize_indicator(value: str) -> str:
    if value is None:
        return ""
    v = str(value).strip().lower()
    # simple normalization: collapse whitespace
    v = re.sub(r"\s+", " ", v)
    return v

def put_metric_processed(count: int):
    try:
        cloudwatch.put_metric_data(
            Namespace="ThreatIntelPipeline",
            MetricData=[{
                "MetricName": "IndicatorsProcessed",
                "Timestamp": datetime.utcnow(),
                "Value": count,
                "Unit": "Count"
            }]
        )
    except Exception as e:
        logger.warning("Could not put metric data: %s", e)

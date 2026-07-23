"""
CloudWatch Metrics → New Relic Metrics forwarder
- Discovers S3 buckets and DynamoDB tables
- Fetches key CloudWatch metrics (S3: storage + requests, DynamoDB: capacity + throttling)
- Pushes to New Relic Metrics API
- Tracks last-fetch time in metrics-state.json
"""

import json
import os
import sys
import time
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ── Env vars ─────────────────────────────────────────────────────────────────
NEW_RELIC_LICENSE_KEY = os.environ["NEW_RELIC_LICENSE_KEY"]   # New Relic ingest license key
NEW_RELIC_REGION      = os.environ.get("NEW_RELIC_REGION", "eu")  # "us" or "eu"
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
STATE_FILE            = os.environ.get("METRICS_STATE_FILE") or "metrics-state.json"
LOOKBACK_MINUTES      = int(os.environ.get("LOOKBACK_MINUTES") or "10")
BATCH_SIZE            = int(os.environ.get("BATCH_SIZE") or "500")
METRICS_PERIOD        = int(os.environ.get("METRICS_PERIOD") or "300")

# ── New Relic endpoints ──────────────────────────────────────────────────────
NR_METRIC_API_URL = (
    "https://metric-api.eu.newrelic.com/metric/v1"
    if NEW_RELIC_REGION == "eu"
    else "https://metric-api.newrelic.com/metric/v1"
)

# ── AWS clients ──────────────────────────────────────────────────────────────
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
s3_client  = boto3.client("s3", region_name=AWS_REGION)
dynamodb   = boto3.client("dynamodb", region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────────────────────
# State file helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        logger.info("State file %s not found, starting fresh", STATE_FILE)
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read state file %s: %s. Starting fresh.", STATE_FILE, e)
        return {}


def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# Resource discovery
# ─────────────────────────────────────────────────────────────────────────────

def list_s3_buckets() -> list[str]:
    try:
        resp = s3_client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]
    except ClientError as e:
        logger.error("Failed to list S3 buckets: %s", e)
        return []


def list_dynamodb_tables() -> list[str]:
    tables = []
    try:
        paginator = dynamodb.get_paginator("list_tables")
        for page in paginator.paginate():
            tables.extend(page.get("TableNames", []))
    except ClientError as e:
        logger.error("Failed to list DynamoDB tables: %s", e)
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch metric helpers
# ─────────────────────────────────────────────────────────────────────────────

S3_STORAGE_METRICS = [
    {"name": "BucketSizeBytes", "stat": "Average", "unit": "Bytes"},
    {"name": "NumberOfObjects", "stat": "Average", "unit": "Count"},
]

S3_REQUEST_METRICS = [
    {"name": "GetObject",      "stat": "Sum", "unit": "Count"},
    {"name": "PutObject",      "stat": "Sum", "unit": "Count"},
    {"name": "HeadObject",     "stat": "Sum", "unit": "Count"},
    {"name": "ListBucket",     "stat": "Sum", "unit": "Count"},
    {"name": "GetObjectAcl",   "stat": "Sum", "unit": "Count"},
    {"name": "PutObjectAcl",   "stat": "Sum", "unit": "Count"},
    {"name": "DeleteObject",   "stat": "Sum", "unit": "Count"},
    {"name": "PostObject",     "stat": "Sum", "unit": "Count"},
    {"name": "CopyObject",     "stat": "Sum", "unit": "Count"},
    {"name": "HeadBucket",     "stat": "Sum", "unit": "Count"},
    {"name": "4xxErrors",      "stat": "Sum", "unit": "Count"},
    {"name": "5xxErrors",      "stat": "Sum", "unit": "Count"},
]

DYNAMODB_METRICS = [
    {"name": "ConsumedReadCapacityUnits",  "stat": "Sum", "unit": "Count"},
    {"name": "ConsumedWriteCapacityUnits", "stat": "Sum", "unit": "Count"},
    {"name": "ProvisionedReadCapacityUnits",  "stat": "Average", "unit": "Count"},
    {"name": "ProvisionedWriteCapacityUnits", "stat": "Average", "unit": "Count"},
    {"name": "ThrottledRequests",         "stat": "Sum", "unit": "Count"},
    {"name": "ReadThrottleEvents",        "stat": "Sum", "unit": "Count"},
    {"name": "WriteThrottleEvents",       "stat": "Sum", "unit": "Count"},
    {"name": "SystemErrors",             "stat": "Sum", "unit": "Count"},
    {"name": "UserErrors",               "stat": "Sum", "unit": "Count"},
    {"name": "ReturnedItemCount",        "stat": "Sum", "unit": "Count"},
    {"name": "SuccessfulRequestLatency", "stat": "Average", "unit": "Milliseconds"},
]


def fetch_metric(
    namespace: str,
    metric_name: str,
    dimensions: list[dict],
    stat: str,
    unit: str,
    start_time: datetime,
    end_time: datetime,
    period: int = 300,
) -> list[dict]:
    """Fetch metric datapoints from CloudWatch and return as list of value dicts."""
    try:
        resp = cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=[stat],
            Unit=unit,
        )
        datapoints = resp.get("Datapoints", [])
        datapoints.sort(key=lambda x: x["Timestamp"])
        return datapoints
    except ClientError as e:
        logger.warning("Failed to fetch %s/%s: %s", namespace, metric_name, e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# New Relic Metrics push
# ─────────────────────────────────────────────────────────────────────────────

def build_nr_metric_payload(metrics: list[dict]) -> dict:
    """
    New Relic Metrics API payload format:
    {
      "metrics": [
        {
          "name": "custom.cloudwatch.s3.BucketSizeBytes",
          "type": "gauge",
          "value": 123.45,
          "timestamp": <epoch_ms>,
          "attributes": {
            "bucket": "my-bucket",
            "region": "us-east-1",
            "source": "s3"
          }
        },
        ...
      ]
    }
    """
    return {"metrics": metrics}


def push_to_new_relic(payload: dict) -> None:
    """Push metrics to New Relic Metrics API."""
    if not payload.get("metrics"):
        return

    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        NR_METRIC_API_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Api-Key": NEW_RELIC_LICENSE_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body_text = resp.read().decode("utf-8", errors="replace")
            if resp.status not in (200, 202):
                raise RuntimeError(f"New Relic Metrics API returned HTTP {resp.status}: {body_text}")
            logger.debug("New Relic Metrics API accepted %d metrics", len(payload["metrics"]))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"New Relic Metrics API HTTP error {e.code}: {body_text}") from e


# ─────────────────────────────────────────────────────────────────────────────
# S3 metrics collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_s3_metrics(
    bucket: str,
    start_time: datetime,
    end_time: datetime,
    state: dict,
) -> list[dict]:
    """Collect S3 metrics for a bucket and return New Relic metric objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"s3:{bucket}"

    # Storage metrics
    for m in S3_STORAGE_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        if m["name"] == "BucketSizeBytes":
            dims.append({"Name": "StorageType", "Value": "StandardStorage"})

        dps = fetch_metric(
            namespace="AWS/S3",
            metric_name=m["name"],
            dimensions=dims,
            stat=m["stat"],
            unit=m["unit"],
            start_time=start_time,
            end_time=end_time,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append({
                "name": f"custom.cloudwatch.s3.{m['name']}",
                "type": "gauge",
                "value": dp[m["stat"]],
                "timestamp": ts,
                "attributes": {
                    "bucket": bucket,
                    "region": AWS_REGION,
                    "source": "s3",
                    "resource": bucket,
                    "unit": m["unit"],
                },
            })

    # Request metrics
    for m in S3_REQUEST_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        dps = fetch_metric(
            namespace="AWS/S3",
            metric_name=m["name"],
            dimensions=dims,
            stat=m["stat"],
            unit=m["unit"],
            start_time=start_time,
            end_time=end_time,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append({
                "name": f"custom.cloudwatch.s3.{m['name']}",
                "type": "gauge",
                "value": dp[m["stat"]],
                "timestamp": ts,
                "attributes": {
                    "bucket": bucket,
                    "region": AWS_REGION,
                    "source": "s3",
                    "resource": bucket,
                    "unit": m["unit"],
                },
            })

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# DynamoDB metrics collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_dynamodb_metrics(
    table: str,
    start_time: datetime,
    end_time: datetime,
    state: dict,
) -> list[dict]:
    """Collect DynamoDB metrics for a table and return New Relic metric objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"dynamodb:{table}"

    for m in DYNAMODB_METRICS:
        dims = [{"Name": "TableName", "Value": table}]
        dps = fetch_metric(
            namespace="AWS/DynamoDB",
            metric_name=m["name"],
            dimensions=dims,
            stat=m["stat"],
            unit=m["unit"],
            start_time=start_time,
            end_time=end_time,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append({
                "name": f"custom.cloudwatch.dynamodb.{m['name']}",
                "type": "gauge",
                "value": dp[m["stat"]],
                "timestamp": ts,
                "attributes": {
                    "table": table,
                    "region": AWS_REGION,
                    "source": "dynamodb",
                    "resource": table,
                    "unit": m["unit"],
                },
            })

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(seconds=METRICS_PERIOD)
    run_start_ms = int(time.time() * 1000)

    state = load_state()
    state_modified = False

    stats = {"s3_buckets": 0, "dynamodb_tables": 0, "total_metrics": 0}
    all_metrics = []

    # ── S3 metrics ────────────────────────────────────────────────────────
    buckets = list_s3_buckets()
    logger.info("Discovered %d S3 buckets", len(buckets))
    for bucket in buckets:
        try:
            metrics = collect_s3_metrics(bucket, start_time, end_time, state)
            if metrics:
                all_metrics.extend(metrics)
                stats["total_metrics"] += len(metrics)
                stats["s3_buckets"] += 1
                logger.info("S3 %s: %d datapoints", bucket, len(metrics))
                state_modified = True
        except Exception as exc:
            logger.error("S3 %s: failed – %s", bucket, exc, exc_info=True)

    # ── DynamoDB metrics ───────────────────────────────────────────────────
    tables = list_dynamodb_tables()
    logger.info("Discovered %d DynamoDB tables", len(tables))
    for table in tables:
        try:
            metrics = collect_dynamodb_metrics(table, start_time, end_time, state)
            if metrics:
                all_metrics.extend(metrics)
                stats["total_metrics"] += len(metrics)
                stats["dynamodb_tables"] += 1
                logger.info("DynamoDB %s: %d datapoints", table, len(metrics))
                state_modified = True
        except Exception as exc:
            logger.error("DynamoDB %s: failed – %s", table, exc, exc_info=True)

    # ── Push to New Relic Metrics API (in batches) ────────────────────────
    if all_metrics:
        for i in range(0, len(all_metrics), BATCH_SIZE):
            batch = all_metrics[i : i + BATCH_SIZE]
            payload = build_nr_metric_payload(batch)
            try:
                push_to_new_relic(payload)
            except Exception as exc:
                logger.error("Failed to push metrics batch: %s", exc)

    if state_modified:
        save_state(state)

    logger.info("Run complete: %s", stats)
    print(json.dumps({"statusCode": 0, "body": stats}))


if __name__ == "__main__":
    main()
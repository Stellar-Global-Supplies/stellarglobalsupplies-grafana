"""
CloudWatch Metrics → Splunk SignalFx forwarder
- Discovers S3 buckets, DynamoDB tables, Lambda functions, API Gateway APIs
- Fetches key CloudWatch metrics
- Pushes to Splunk SignalFx ingest API
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
SPLUNK_INGEST_TOKEN = os.environ["SPLUNK_INGEST_TOKEN"]   # SignalFx ingest token
SPLUNK_REALM        = os.environ.get("SPLUNK_REALM", "us0")   # e.g. us0, eu0
AWS_REGION          = os.environ.get("AWS_REGION", "us-east-1")
STATE_FILE          = os.environ.get("METRICS_STATE_FILE") or "metrics-state.json"
LOOKBACK_MINUTES    = int(os.environ.get("LOOKBACK_MINUTES") or "10")
BATCH_SIZE          = int(os.environ.get("BATCH_SIZE") or "500")
METRICS_PERIOD      = int(os.environ.get("METRICS_PERIOD") or "300")

# ── Splunk SignalFx endpoint ─────────────────────────────────────────────────
SPLUNK_METRIC_URL = f"https://ingest.{SPLUNK_REALM}.signalfx.com/v2/datapoint"

# ── AWS clients ──────────────────────────────────────────────────────────────
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
s3_client  = boto3.client("s3", region_name=AWS_REGION)
dynamodb   = boto3.client("dynamodb", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
apigw_client  = boto3.client("apigateway", region_name=AWS_REGION)


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


def list_lambda_functions() -> list[str]:
    fns = []
    try:
        paginator = lambda_client.get_paginator("list_functions")
        for page in paginator.paginate():
            fns.extend(f["FunctionName"] for f in page.get("Functions", []))
        logger.info("Discovered %d Lambda functions", len(fns))
    except ClientError as e:
        logger.error("Failed to list Lambda functions: %s", e)
    return fns


def list_apigw_apis() -> list[str]:
    apis = []
    try:
        paginator = apigw_client.get_paginator("get_rest_apis")
        for page in paginator.paginate():
            apis.extend(a["id"] for a in page.get("items", []))
        logger.info("Discovered %d REST APIs", len(apis))
    except ClientError as e:
        logger.error("Failed to list API Gateway APIs: %s", e)
    return apis


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

LAMBDA_METRICS = [
    {"name": "Invocations",          "stat": "Sum",     "unit": "Count"},
    {"name": "Errors",               "stat": "Sum",     "unit": "Count"},
    {"name": "Throttles",            "stat": "Sum",     "unit": "Count"},
    {"name": "Duration",             "stat": "Average", "unit": "Milliseconds"},
    {"name": "ConcurrentExecutions", "stat": "Maximum", "unit": "Count"},
]

APIGW_METRICS = [
    {"name": "Count",              "stat": "Sum",     "unit": "Count"},
    {"name": "4XXError",           "stat": "Sum",     "unit": "Count"},
    {"name": "5XXError",           "stat": "Sum",     "unit": "Count"},
    {"name": "Latency",            "stat": "Average", "unit": "Milliseconds"},
    {"name": "IntegrationLatency", "stat": "Average", "unit": "Milliseconds"},
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


def build_metric_object(
    metric_name: str,
    value: float,
    timestamp: int,
    dimensions: dict,
) -> dict:
    """
    Build a Splunk SignalFx gauge datapoint.
    Renames custom.cloudwatch.* prefix to aws.* for cleaner naming in Splunk.
    """
    sfx_name = metric_name.replace("custom.cloudwatch.", "aws.")
    return {
        "metric": sfx_name,
        "value": value,
        "timestamp": timestamp,
        "dimensions": {k: str(v) for k, v in dimensions.items()},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Splunk SignalFx push
# ─────────────────────────────────────────────────────────────────────────────

def build_splunk_metric_payload(metrics: list[dict]) -> dict:
    """
    Splunk SignalFx gauge format:
    { "gauge": [ { "metric": "aws.s3.BucketSizeBytes", "value": 123,
                   "timestamp": <epoch_ms>, "dimensions": {...} } ] }
    """
    return {"gauge": metrics}


def push_to_splunk_metrics(payload: dict) -> None:
    """Push metrics to Splunk SignalFx ingest API."""
    if not payload.get("gauge"):
        return

    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        SPLUNK_METRIC_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-SF-Token": SPLUNK_INGEST_TOKEN,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body_text = resp.read().decode("utf-8", errors="replace")
            logger.info("Splunk SignalFx response HTTP %d: %s", resp.status, body_text)
            if resp.status not in (200, 204):
                raise RuntimeError(f"Splunk metrics API returned HTTP {resp.status}: {body_text}")
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Splunk metrics API error {e.code}: {body_text}") from e


# ─────────────────────────────────────────────────────────────────────────────
# S3 metrics collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_s3_metrics(
    bucket: str,
    start_time: datetime,
    end_time: datetime,
    state: dict,
) -> list[dict]:
    """Collect S3 metrics for a bucket and return Splunk datapoint objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"s3:{bucket}"

    for m in S3_STORAGE_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        if m["name"] == "BucketSizeBytes":
            dims.append({"Name": "StorageType", "Value": "StandardStorage"})
        dps = fetch_metric("AWS/S3", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"custom.cloudwatch.s3.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"bucket": bucket, "region": AWS_REGION, "source": "s3", "resource": bucket},
            ))

    for m in S3_REQUEST_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        dps = fetch_metric("AWS/S3", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"custom.cloudwatch.s3.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"bucket": bucket, "region": AWS_REGION, "source": "s3", "resource": bucket},
            ))

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
    """Collect DynamoDB metrics for a table and return Splunk datapoint objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"dynamodb:{table}"

    for m in DYNAMODB_METRICS:
        dims = [{"Name": "TableName", "Value": table}]
        dps = fetch_metric("AWS/DynamoDB", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"custom.cloudwatch.dynamodb.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"table": table, "region": AWS_REGION, "source": "dynamodb", "resource": table},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Lambda metrics collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_lambda_metrics(
    fn: str,
    start_time: datetime,
    end_time: datetime,
    state: dict,
) -> list[dict]:
    """Collect Lambda metrics for a function and return Splunk datapoint objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"lambda:{fn}"

    for m in LAMBDA_METRICS:
        dims = [{"Name": "FunctionName", "Value": fn}]
        dps = fetch_metric("AWS/Lambda", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            val = dp.get(m["stat"], dp.get("Average", dp.get("Sum", dp.get("Maximum", 0))))
            metrics.append(build_metric_object(
                f"custom.cloudwatch.lambda.{m['name']}.{m['stat']}",
                val, ts,
                {"function_name": fn, "region": AWS_REGION, "source": "lambda", "resource": fn},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# API Gateway metrics collector
# ─────────────────────────────────────────────────────────────────────────────

def collect_apigw_metrics(
    api_id: str,
    start_time: datetime,
    end_time: datetime,
    state: dict,
) -> list[dict]:
    """Collect API Gateway metrics for an API and return Splunk datapoint objects."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"apigw:{api_id}"

    for m in APIGW_METRICS:
        dims = [{"Name": "ApiId", "Value": api_id}]
        dps = fetch_metric("AWS/ApiGateway", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            val = dp.get(m["stat"], dp.get("Average", dp.get("Sum", 0)))
            metrics.append(build_metric_object(
                f"custom.cloudwatch.apigw.{m['name']}.{m['stat']}",
                val, ts,
                {"api_id": api_id, "region": AWS_REGION, "source": "apigateway", "resource": api_id},
            ))

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

    stats = {"s3_buckets": 0, "dynamodb_tables": 0, "lambda_functions": 0, "apigw_apis": 0, "total_metrics": 0}
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

    # ── Lambda metrics ─────────────────────────────────────────────────────
    for fn in list_lambda_functions():
        try:
            metrics = collect_lambda_metrics(fn, start_time, end_time, state)
            if metrics:
                all_metrics.extend(metrics)
                stats["total_metrics"] += len(metrics)
                stats["lambda_functions"] += 1
                logger.info("Lambda %s: %d datapoints", fn, len(metrics))
                state_modified = True
        except Exception as exc:
            logger.error("Lambda %s: failed – %s", fn, exc, exc_info=True)

    # ── API Gateway metrics ────────────────────────────────────────────────
    for api_id in list_apigw_apis():
        try:
            metrics = collect_apigw_metrics(api_id, start_time, end_time, state)
            if metrics:
                all_metrics.extend(metrics)
                stats["total_metrics"] += len(metrics)
                stats["apigw_apis"] += 1
                logger.info("APIGW %s: %d datapoints", api_id, len(metrics))
                state_modified = True
        except Exception as exc:
            logger.error("APIGW %s: failed – %s", api_id, exc, exc_info=True)

    # ── Push to Splunk SignalFx (in batches) ──────────────────────────────
    if all_metrics:
        for i in range(0, len(all_metrics), BATCH_SIZE):
            batch = all_metrics[i : i + BATCH_SIZE]
            payload = build_splunk_metric_payload(batch)
            try:
                push_to_splunk_metrics(payload)
            except Exception as exc:
                logger.error("Failed to push metrics batch: %s", exc)

    if state_modified:
        save_state(state)

    logger.info("Run complete: %s", stats)
    print(json.dumps({"statusCode": 0, "body": stats}))


if __name__ == "__main__":
    main()
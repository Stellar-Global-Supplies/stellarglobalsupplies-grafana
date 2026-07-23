"""
CloudWatch Metrics → New Relic Metric API forwarder (GitHub Actions version)
- Discovers S3 buckets, DynamoDB tables, Lambda functions, API Gateway APIs
- Fetches key CloudWatch metrics
- Pushes to New Relic Metric API (HTTP POST, JSON)
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
NR_LICENSE_KEY  = os.environ["NEW_RELIC_LICENSE_KEY"]          # New Relic ingest license key
NR_METRICS_URL  = os.environ.get(                              # override for EU: metric-api.eu.newrelic.com
    "NEW_RELIC_METRICS_URL",
    "https://metric-api.newrelic.com/metric/v1",
)
AWS_REGION      = os.environ.get("AWS_REGION", "us-east-1")
STATE_FILE      = os.environ.get("METRICS_STATE_FILE") or "metrics-state.json"
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES") or "10")
BATCH_SIZE      = int(os.environ.get("BATCH_SIZE") or "500")
METRICS_PERIOD  = int(os.environ.get("METRICS_PERIOD") or "300")

# ── AWS clients ──────────────────────────────────────────────────────────────
cloudwatch    = boto3.client("cloudwatch",  region_name=AWS_REGION)
s3_client     = boto3.client("s3",          region_name=AWS_REGION)
dynamodb      = boto3.client("dynamodb",    region_name=AWS_REGION)
lambda_client = boto3.client("lambda",      region_name=AWS_REGION)
apigw_client  = boto3.client("apigateway",  region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────────────────────
# State file helpers  (unchanged from original)
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
# Resource discovery  (unchanged from original)
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
# CloudWatch metric definitions  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

S3_STORAGE_METRICS = [
    {"name": "BucketSizeBytes",  "stat": "Average", "unit": "Bytes"},
    {"name": "NumberOfObjects",  "stat": "Average", "unit": "Count"},
]

S3_REQUEST_METRICS = [
    {"name": "GetObject",     "stat": "Sum", "unit": "Count"},
    {"name": "PutObject",     "stat": "Sum", "unit": "Count"},
    {"name": "HeadObject",    "stat": "Sum", "unit": "Count"},
    {"name": "ListBucket",    "stat": "Sum", "unit": "Count"},
    {"name": "GetObjectAcl",  "stat": "Sum", "unit": "Count"},
    {"name": "PutObjectAcl",  "stat": "Sum", "unit": "Count"},
    {"name": "DeleteObject",  "stat": "Sum", "unit": "Count"},
    {"name": "PostObject",    "stat": "Sum", "unit": "Count"},
    {"name": "CopyObject",    "stat": "Sum", "unit": "Count"},
    {"name": "HeadBucket",    "stat": "Sum", "unit": "Count"},
    {"name": "4xxErrors",     "stat": "Sum", "unit": "Count"},
    {"name": "5xxErrors",     "stat": "Sum", "unit": "Count"},
]

DYNAMODB_METRICS = [
    {"name": "ConsumedReadCapacityUnits",     "stat": "Sum",     "unit": "Count"},
    {"name": "ConsumedWriteCapacityUnits",    "stat": "Sum",     "unit": "Count"},
    {"name": "ProvisionedReadCapacityUnits",  "stat": "Average", "unit": "Count"},
    {"name": "ProvisionedWriteCapacityUnits", "stat": "Average", "unit": "Count"},
    {"name": "ThrottledRequests",             "stat": "Sum",     "unit": "Count"},
    {"name": "ReadThrottleEvents",            "stat": "Sum",     "unit": "Count"},
    {"name": "WriteThrottleEvents",           "stat": "Sum",     "unit": "Count"},
    {"name": "SystemErrors",                  "stat": "Sum",     "unit": "Count"},
    {"name": "UserErrors",                    "stat": "Sum",     "unit": "Count"},
    {"name": "ReturnedItemCount",             "stat": "Sum",     "unit": "Count"},
    {"name": "SuccessfulRequestLatency",      "stat": "Average", "unit": "Milliseconds"},
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


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch fetch helper  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

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
# New Relic Metric API helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_metric_object(
    metric_name: str,
    value: float,
    timestamp_ms: int,
    attributes: dict,
    interval_ms: int = 300_000,
) -> dict:
    """
    Build a New Relic Metric API datapoint.

    New Relic supports three metric types:
      - gauge   : a value at a point in time (e.g. BucketSizeBytes, Duration)
      - count   : a delta count over an interval (e.g. Invocations, Errors)
      - summary : min/max/sum/count rollup — not used here, we keep it simple

    We map CloudWatch "Sum" stats → count, everything else → gauge.
    The interval_ms must match METRICS_PERIOD so NR can compute rates correctly.

    Payload shape (one item in the outer array's "metrics" list):
    {
      "name":       "aws.s3.BucketSizeBytes.Average",
      "type":       "gauge",
      "value":      12345.0,
      "timestamp":  1234567890000,   # epoch ms
      "interval.ms": 300000,         # required for count type
      "attributes": { "bucket": "my-bucket", ... }
    }
    """
    # "Sum" stats represent accumulated counts over the period → NR count type
    nr_type = "count" if metric_name.endswith(".Sum") else "gauge"

    return {
        "name":          metric_name,
        "type":          nr_type,
        "value":         float(value),
        "timestamp":     timestamp_ms,
        "interval.ms":   interval_ms,
        "attributes":    {k: str(v) for k, v in attributes.items()},
    }


def build_nr_metric_payload(metrics: list[dict]) -> list[dict]:
    """
    New Relic Metric API envelope:
    [
      {
        "common": {
          "attributes": { "forwarder": "github-actions", "aws.region": "us-east-1" },
          "interval.ms": 300000
        },
        "metrics": [ <metric objects> ]
      }
    ]
    """
    return [
        {
            "common": {
                "attributes": {
                    "forwarder":  "github-actions",
                    "aws.region": AWS_REGION,
                },
                "interval.ms": METRICS_PERIOD * 1000,
            },
            "metrics": metrics,
        }
    ]


def push_to_nr_metrics(payload: list[dict]) -> None:
    """Push metrics to New Relic Metric API."""
    if not payload or not payload[0].get("metrics"):
        return

    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        NR_METRICS_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Api-Key":      NR_LICENSE_KEY,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body_text = resp.read().decode("utf-8", errors="replace")
            logger.info("New Relic Metrics response HTTP %d: %s", resp.status, body_text)
            if resp.status not in (200, 202):
                raise RuntimeError(f"New Relic Metric API returned HTTP {resp.status}: {body_text}")
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"New Relic Metric API error {e.code}: {body_text}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Per-service metric collectors  (logic unchanged, NR metric names used)
# ─────────────────────────────────────────────────────────────────────────────

def collect_s3_metrics(bucket: str, start_time: datetime, end_time: datetime, state: dict) -> list[dict]:
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
                f"aws.s3.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"bucket": bucket, "region": AWS_REGION, "source": "s3", "resource": bucket},
            ))

    for m in S3_REQUEST_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        dps = fetch_metric("AWS/S3", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"aws.s3.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"bucket": bucket, "region": AWS_REGION, "source": "s3", "resource": bucket},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_dynamodb_metrics(table: str, start_time: datetime, end_time: datetime, state: dict) -> list[dict]:
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"dynamodb:{table}"

    for m in DYNAMODB_METRICS:
        dims = [{"Name": "TableName", "Value": table}]
        dps = fetch_metric("AWS/DynamoDB", m["name"], dims, m["stat"], m["unit"], start_time, end_time)
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"aws.dynamodb.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts,
                {"table": table, "region": AWS_REGION, "source": "dynamodb", "resource": table},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_lambda_metrics(fn: str, start_time: datetime, end_time: datetime, state: dict) -> list[dict]:
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
                f"aws.lambda.{m['name']}.{m['stat']}",
                val, ts,
                {"function_name": fn, "region": AWS_REGION, "source": "lambda", "resource": fn},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_apigw_metrics(api_id: str, start_time: datetime, end_time: datetime, state: dict) -> list[dict]:
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
                f"aws.apigateway.{m['name']}.{m['stat']}",
                val, ts,
                {"api_id": api_id, "region": AWS_REGION, "source": "apigateway", "resource": api_id},
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def main():
    end_time     = datetime.now(timezone.utc)
    start_time   = end_time - timedelta(seconds=METRICS_PERIOD)

    state = load_state()
    state_modified = False

    stats = {"s3_buckets": 0, "dynamodb_tables": 0, "lambda_functions": 0, "apigw_apis": 0, "total_metrics": 0}
    all_metrics = []

    # ── S3 ────────────────────────────────────────────────────────────────
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

    # ── DynamoDB ──────────────────────────────────────────────────────────
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

    # ── Lambda ────────────────────────────────────────────────────────────
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

    # ── API Gateway ───────────────────────────────────────────────────────
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

    # ── Push to New Relic Metric API (in batches) ─────────────────────────
    if all_metrics:
        for i in range(0, len(all_metrics), BATCH_SIZE):
            batch = all_metrics[i : i + BATCH_SIZE]
            payload = build_nr_metric_payload(batch)
            try:
                push_to_nr_metrics(payload)
            except Exception as exc:
                logger.error("Failed to push metrics batch: %s", exc)

    if state_modified:
        save_state(state)

    logger.info("Run complete: %s", stats)
    print(json.dumps({"statusCode": 0, "body": stats}))


if __name__ == "__main__":
    main()
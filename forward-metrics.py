"""
CloudWatch Metrics → New Relic Metric API forwarder (GitHub Actions version)

Fixes applied vs original:
  1. Lambda / API Gateway / StepFunctions / DynamoDB → 24-hour lookback window
     (was METRICS_PERIOD=300 s which returned 0 datapoints for low-traffic resources).
  2. S3 BucketSizeBytes & NumberOfObjects → lifetime window (90 days back) with
     86 400-second period because AWS only publishes these once per day.
  3. DynamoDB ItemCount (total items) → fetched via describe_table(), not CW,
     because AWS stopped publishing ItemCount to CloudWatch for on-demand tables
     and it shows 0 in CW for many table configurations.
  4. StepFunctions (AWS Step Functions) added as a new service with 24h window.
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
NR_LICENSE_KEY   = os.environ["NEW_RELIC_LICENSE_KEY"]
_NR_REGION       = os.environ.get("NEW_RELIC_REGION", "eu").strip().lower()
_NR_METRIC_HOST  = "metric-api.eu.newrelic.com" if _NR_REGION == "eu" else "metric-api.newrelic.com"
NR_METRICS_URL   = os.environ.get("NEW_RELIC_METRICS_URL") or f"https://{_NR_METRIC_HOST}/metric/v1"
logger.info("New Relic region=%s  metrics endpoint=%s", _NR_REGION, NR_METRICS_URL)
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
STATE_FILE       = os.environ.get("METRICS_STATE_FILE") or "metrics-state.json"
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES") or "10")
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE") or "500")
METRICS_PERIOD   = int(os.environ.get("METRICS_PERIOD") or "300")

# ── Window constants ──────────────────────────────────────────────────────────
# Lambda / APIGW / StepFunctions / DynamoDB CW metrics — 24-hour lookback
# so each GitHub Actions run captures all datapoints since the last run.
WINDOW_24H_SECONDS = 24 * 3600          # 86 400 s

# S3 storage metrics are published by AWS once per day; we use a 90-day
# lookback with a 1-day period to guarantee we always get the latest value
# regardless of when exactly AWS publishes it.
S3_STORAGE_LOOKBACK_DAYS = 90
S3_STORAGE_PERIOD        = 86_400       # 1 day in seconds

# ── AWS clients ──────────────────────────────────────────────────────────────
cloudwatch    = boto3.client("cloudwatch",  region_name=AWS_REGION)
s3_client     = boto3.client("s3",          region_name=AWS_REGION)
dynamodb      = boto3.client("dynamodb",    region_name=AWS_REGION)
lambda_client = boto3.client("lambda",      region_name=AWS_REGION)
apigw_client  = boto3.client("apigateway",  region_name=AWS_REGION)
sfn_client    = boto3.client("stepfunctions", region_name=AWS_REGION)


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
        logger.info("Discovered %d DynamoDB tables", len(tables))
    except Exception as e:
        logger.error("Failed to list DynamoDB tables: %s", e, exc_info=True)
    return tables


def list_lambda_functions() -> list[str]:
    fns = []
    try:
        paginator = lambda_client.get_paginator("list_functions")
        for page in paginator.paginate():
            fns.extend(f["FunctionName"] for f in page.get("Functions", []))
        logger.info("Discovered %d Lambda functions", len(fns))
    except Exception as e:
        logger.error("Failed to list Lambda functions: %s", e, exc_info=True)
    return fns


def list_apigw_apis() -> list[str]:
    apis = []
    try:
        # REST APIs (v1)
        paginator = apigw_client.get_paginator("get_rest_apis")
        for page in paginator.paginate():
            apis.extend(a["id"] for a in page.get("items", []))
        logger.info("Discovered %d REST APIs", len(apis))
    except Exception as e:
        logger.error("Failed to list API Gateway APIs: %s", e, exc_info=True)
    return apis


def list_step_functions() -> list[dict]:
    """Return list of {name, arn} dicts for all state machines."""
    machines = []
    try:
        paginator = sfn_client.get_paginator("list_state_machines")
        for page in paginator.paginate():
            for sm in page.get("stateMachines", []):
                machines.append({"name": sm["name"], "arn": sm["stateMachineArn"]})
        logger.info("Discovered %d Step Function state machines", len(machines))
    except Exception as e:
        logger.error("Failed to list Step Functions: %s", e, exc_info=True)
    return machines


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch metric definitions
# ─────────────────────────────────────────────────────────────────────────────

# S3 storage: published daily — fetched with S3_STORAGE_PERIOD (86400s)
S3_STORAGE_METRICS = [
    {"name": "BucketSizeBytes",  "stat": "Average", "unit": "Bytes"},
    {"name": "NumberOfObjects",  "stat": "Average", "unit": "Count"},
]

# S3 request metrics: real-time, use standard METRICS_PERIOD
S3_REQUEST_METRICS = [
    {"name": "GetRequests",      "stat": "Sum", "unit": "Count"},
    {"name": "PutRequests",      "stat": "Sum", "unit": "Count"},
    {"name": "HeadRequests",     "stat": "Sum", "unit": "Count"},
    {"name": "ListRequests",     "stat": "Sum", "unit": "Count"},
    {"name": "DeleteRequests",   "stat": "Sum", "unit": "Count"},
    {"name": "4xxErrors",        "stat": "Sum", "unit": "Count"},
    {"name": "5xxErrors",        "stat": "Sum", "unit": "Count"},
]

# DynamoDB: use 24h window
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

# Lambda: use 24h window
LAMBDA_METRICS = [
    {"name": "Invocations",          "stat": "Sum",     "unit": "Count"},
    {"name": "Errors",               "stat": "Sum",     "unit": "Count"},
    {"name": "Throttles",            "stat": "Sum",     "unit": "Count"},
    {"name": "Duration",             "stat": "Average", "unit": "Milliseconds"},
    {"name": "ConcurrentExecutions", "stat": "Maximum", "unit": "Count"},
]

# API Gateway: use 24h window
APIGW_METRICS = [
    {"name": "Count",              "stat": "Sum",     "unit": "Count"},
    {"name": "4XXError",           "stat": "Sum",     "unit": "Count"},
    {"name": "5XXError",           "stat": "Sum",     "unit": "Count"},
    {"name": "Latency",            "stat": "Average", "unit": "Milliseconds"},
    {"name": "IntegrationLatency", "stat": "Average", "unit": "Milliseconds"},
]

# Step Functions: use 24h window
# Namespace: AWS/States; dimension: StateMachineArn
SFN_METRICS = [
    {"name": "ExecutionsStarted",   "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionsSucceeded", "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionsFailed",    "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionsAborted",   "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionsTimedOut",  "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionThrottled",  "stat": "Sum",     "unit": "Count"},
    {"name": "ExecutionTime",       "stat": "Average", "unit": "Milliseconds"},
]


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch fetch helper
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
    nr_type = "count" if metric_name.endswith(".Sum") else "gauge"
    return {
        "name":        metric_name,
        "type":        nr_type,
        "value":       float(value),
        "timestamp":   timestamp_ms,
        "interval.ms": interval_ms,
        "attributes":  {k: str(v) for k, v in attributes.items()},
    }


def build_nr_metric_payload(metrics: list[dict]) -> list[dict]:
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
# Per-service metric collectors
# ─────────────────────────────────────────────────────────────────────────────

def collect_s3_metrics(bucket: str, now: datetime, state: dict) -> list[dict]:
    """
    S3 storage metrics (BucketSizeBytes, NumberOfObjects):
      - AWS publishes these ONCE PER DAY to CloudWatch.
      - We use a 90-day lookback + 86400-second period so we always capture
        the latest daily value regardless of when the action runs.

    S3 request metrics:
      - Real-time; use standard 24h window.
    """
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"s3:{bucket}"
    attrs = {"bucket": bucket, "region": AWS_REGION, "source": "s3", "resource": bucket}

    # ── Storage metrics: lifetime / 90-day window, daily period ──────────────
    storage_start = now - timedelta(days=S3_STORAGE_LOOKBACK_DAYS)
    for m in S3_STORAGE_METRICS:
        dims = [{"Name": "BucketName", "Value": bucket}]
        if m["name"] == "BucketSizeBytes":
            dims.append({"Name": "StorageType", "Value": "StandardStorage"})
        else:
            dims.append({"Name": "StorageType", "Value": "AllStorageTypes"})

        dps = fetch_metric(
            "AWS/S3", m["name"], dims, m["stat"], m["unit"],
            storage_start, now,
            period=S3_STORAGE_PERIOD,
        )
        if dps:
            # Take the most recent datapoint so Grafana always shows current value
            latest = dps[-1]
            ts = int(latest["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"aws.s3.{m['name']}.{m['stat']}",
                latest[m["stat"]], ts, attrs,
                interval_ms=S3_STORAGE_PERIOD * 1000,
            ))
            logger.info("S3 %s %s: latest value=%.0f at %s",
                        bucket, m["name"], latest[m["stat"]], latest["Timestamp"])
        else:
            logger.warning(
                "S3 %s %s: no CW datapoints in 90-day window — "
                "enable S3 Storage Lens or request metrics if needed.",
                bucket, m["name"],
            )

    # ── Request metrics: 24h window, standard period ──────────────────────────
    req_start = now - timedelta(seconds=WINDOW_24H_SECONDS)
    for m in S3_REQUEST_METRICS:
        dims = [
            {"Name": "BucketName", "Value": bucket},
            {"Name": "FilterId",   "Value": "EntireBucket"},
        ]
        dps = fetch_metric(
            "AWS/S3", m["name"], dims, m["stat"], m["unit"],
            req_start, now,
            period=METRICS_PERIOD,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"aws.s3.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts, attrs,
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_dynamodb_metrics(table: str, now: datetime, state: dict) -> list[dict]:
    """
    DynamoDB metrics — 24-hour lookback so even low-traffic tables surface data.

    ItemCount (total objects) is fetched directly from describe_table() because:
      - AWS stopped publishing ItemCount to CloudWatch for on-demand tables.
      - Even for provisioned tables, CW ItemCount can lag by ~6 hours.
      - describe_table() returns the value as of the last table update.
    """
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"dynamodb:{table}"
    attrs = {"table": table, "region": AWS_REGION, "source": "dynamodb", "resource": table}

    # ── ItemCount via describe_table (fixes "0 metrics" issue) ───────────────
    try:
        resp = dynamodb.describe_table(TableName=table)
        item_count = resp["Table"].get("ItemCount", 0)
        table_size_bytes = resp["Table"].get("TableSizeBytes", 0)
        metrics.append(build_metric_object(
            "aws.dynamodb.ItemCount.describe",
            float(item_count), now_ms, attrs,
        ))
        metrics.append(build_metric_object(
            "aws.dynamodb.TableSizeBytes.describe",
            float(table_size_bytes), now_ms, attrs,
        ))
        logger.info("DynamoDB %s: ItemCount=%d SizeBytes=%d (via describe_table)",
                    table, item_count, table_size_bytes)
    except ClientError as e:
        logger.warning("DynamoDB %s: describe_table failed – %s", table, e)

    # ── Standard CW metrics: 24h window ──────────────────────────────────────
    start_time = now - timedelta(seconds=WINDOW_24H_SECONDS)
    for m in DYNAMODB_METRICS:
        dims = [{"Name": "TableName", "Value": table}]
        dps = fetch_metric(
            "AWS/DynamoDB", m["name"], dims, m["stat"], m["unit"],
            start_time, now,
            period=METRICS_PERIOD,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            metrics.append(build_metric_object(
                f"aws.dynamodb.{m['name']}.{m['stat']}",
                dp[m["stat"]], ts, attrs,
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_lambda_metrics(fn: str, now: datetime, state: dict) -> list[dict]:
    """Lambda metrics — 24-hour lookback."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"lambda:{fn}"
    attrs = {"function_name": fn, "region": AWS_REGION, "source": "lambda", "resource": fn}

    start_time = now - timedelta(seconds=WINDOW_24H_SECONDS)
    for m in LAMBDA_METRICS:
        dims = [{"Name": "FunctionName", "Value": fn}]
        dps = fetch_metric(
            "AWS/Lambda", m["name"], dims, m["stat"], m["unit"],
            start_time, now,
            period=METRICS_PERIOD,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            val = dp.get(m["stat"], dp.get("Average", dp.get("Sum", dp.get("Maximum", 0))))
            metrics.append(build_metric_object(
                f"aws.lambda.{m['name']}.{m['stat']}",
                val, ts, attrs,
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_apigw_metrics(api_id: str, now: datetime, state: dict) -> list[dict]:
    """API Gateway metrics — 24-hour lookback."""
    metrics = []
    now_ms = int(time.time() * 1000)
    state_key = f"apigw:{api_id}"
    attrs = {"api_id": api_id, "region": AWS_REGION, "source": "apigateway", "resource": api_id}

    start_time = now - timedelta(seconds=WINDOW_24H_SECONDS)
    for m in APIGW_METRICS:
        dims = [{"Name": "ApiId", "Value": api_id}]
        dps = fetch_metric(
            "AWS/ApiGateway", m["name"], dims, m["stat"], m["unit"],
            start_time, now,
            period=METRICS_PERIOD,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            val = dp.get(m["stat"], dp.get("Average", dp.get("Sum", 0)))
            metrics.append(build_metric_object(
                f"aws.apigateway.{m['name']}.{m['stat']}",
                val, ts, attrs,
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


def collect_sfn_metrics(sm: dict, now: datetime, state: dict) -> list[dict]:
    """
    Step Functions metrics — 24-hour lookback.
    Namespace: AWS/States; dimension: StateMachineArn.
    """
    metrics = []
    now_ms = int(time.time() * 1000)
    name = sm["name"]
    arn  = sm["arn"]
    state_key = f"sfn:{name}"
    attrs = {
        "state_machine_name": name,
        "state_machine_arn":  arn,
        "region": AWS_REGION,
        "source": "stepfunctions",
        "resource": name,
    }

    start_time = now - timedelta(seconds=WINDOW_24H_SECONDS)
    for m in SFN_METRICS:
        dims = [{"Name": "StateMachineArn", "Value": arn}]
        dps = fetch_metric(
            "AWS/States", m["name"], dims, m["stat"], m["unit"],
            start_time, now,
            period=METRICS_PERIOD,
        )
        for dp in dps:
            ts = int(dp["Timestamp"].timestamp() * 1000)
            val = dp.get(m["stat"], dp.get("Average", dp.get("Sum", 0)))
            metrics.append(build_metric_object(
                f"aws.stepfunctions.{m['name']}.{m['stat']}",
                val, ts, attrs,
            ))

    state[state_key] = {"lastFetchMs": now_ms, "updatedAt": datetime.now(timezone.utc).isoformat()}
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)

    # ── Startup diagnostics ───────────────────────────────────────────────────
    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        identity = sts.get_caller_identity()
        logger.info("AWS identity: account=%s arn=%s region=%s",
                    identity["Account"], identity["Arn"], AWS_REGION)
    except Exception as e:
        logger.error("Failed to get AWS identity — credentials may be missing: %s", e, exc_info=True)

    state = load_state()
    state_modified = False

    stats = {
        "s3_buckets": 0, "dynamodb_tables": 0, "lambda_functions": 0,
        "apigw_apis": 0, "sfn_state_machines": 0, "total_metrics": 0,
    }
    all_metrics = []

    # ── S3 ────────────────────────────────────────────────────────────────────
    buckets = list_s3_buckets()
    logger.info("Discovered %d S3 buckets", len(buckets))
    for bucket in buckets:
        try:
            m = collect_s3_metrics(bucket, now, state)
            if m:
                all_metrics.extend(m)
                stats["total_metrics"] += len(m)
                stats["s3_buckets"] += 1
                logger.info("S3 %s: %d datapoints", bucket, len(m))
                state_modified = True
        except Exception as exc:
            logger.error("S3 %s: failed – %s", bucket, exc, exc_info=True)

    # ── DynamoDB ──────────────────────────────────────────────────────────────
    tables = list_dynamodb_tables()
    logger.info("Discovered %d DynamoDB tables", len(tables))
    for table in tables:
        try:
            m = collect_dynamodb_metrics(table, now, state)
            if m:
                all_metrics.extend(m)
                stats["total_metrics"] += len(m)
                stats["dynamodb_tables"] += 1
                logger.info("DynamoDB %s: %d datapoints", table, len(m))
                state_modified = True
        except Exception as exc:
            logger.error("DynamoDB %s: failed – %s", table, exc, exc_info=True)

    # ── Lambda ────────────────────────────────────────────────────────────────
    for fn in list_lambda_functions():
        try:
            m = collect_lambda_metrics(fn, now, state)
            if m:
                all_metrics.extend(m)
                stats["total_metrics"] += len(m)
                stats["lambda_functions"] += 1
                logger.info("Lambda %s: %d datapoints", fn, len(m))
                state_modified = True
        except Exception as exc:
            logger.error("Lambda %s: failed – %s", fn, exc, exc_info=True)

    # ── API Gateway ───────────────────────────────────────────────────────────
    for api_id in list_apigw_apis():
        try:
            m = collect_apigw_metrics(api_id, now, state)
            if m:
                all_metrics.extend(m)
                stats["total_metrics"] += len(m)
                stats["apigw_apis"] += 1
                logger.info("APIGW %s: %d datapoints", api_id, len(m))
                state_modified = True
        except Exception as exc:
            logger.error("APIGW %s: failed – %s", api_id, exc, exc_info=True)

    # ── Step Functions ────────────────────────────────────────────────────────
    for sm in list_step_functions():
        try:
            m = collect_sfn_metrics(sm, now, state)
            if m:
                all_metrics.extend(m)
                stats["total_metrics"] += len(m)
                stats["sfn_state_machines"] += 1
                logger.info("StepFunctions %s: %d datapoints", sm["name"], len(m))
                state_modified = True
        except Exception as exc:
            logger.error("StepFunctions %s: failed – %s", sm["name"], exc, exc_info=True)

    # ── Push to New Relic ─────────────────────────────────────────────────────
    if all_metrics:
        for i in range(0, len(all_metrics), BATCH_SIZE):
            batch = all_metrics[i : i + BATCH_SIZE]
            payload = build_nr_metric_payload(batch)
            try:
                push_to_nr_metrics(payload)
            except Exception as exc:
                logger.error("Failed to push metrics batch: %s", exc)
    else:
        logger.warning("No metrics collected — check AWS permissions and resource existence.")

    if state_modified:
        save_state(state)

    logger.info("Run complete: %s", stats)
    print(json.dumps({"statusCode": 0, "body": stats}))


if __name__ == "__main__":
    main()
"""
CUR JSON → New Relic Metric API forwarder (GitHub Actions version)

Reads 5 pre-transformed CUR JSON files produced by your existing automation:
  - costs.json                : daily cost+usage per service per region (granular)
  - daily-costs.json          : daily total + per-service rollup
  - summary.json              : monthly total per service
  - costs-by-tag.json         : cost breakdown by application tag + uncategorized
  - costs-by-usage-group.json : daily cost per service + normalized resource
                                group (usageGroup) — powers the resource
                                naming-convention / grouping panels

Pushes everything as gauges to New Relic Metric API under the namespace
  aws.cur.*
so they can be queried in New Relic and displayed in a Grafana-style dashboard.

Note on timestamps: costs.json, daily-costs.json, and costs-by-usage-group.json
are stamped per-calendar-day from the row's own date. summary.json and
costs-by-tag.json represent month-to-date rollups that are regenerated every
run, so they are stamped with the current run time rather than the 1st of the
billing month — the New Relic Metric API silently drops any point more than
48 hours old, which would otherwise make those two files' metrics vanish
after the first couple of days of each month.

Environment variables:
  NEW_RELIC_LICENSE_KEY   — required
  NEW_RELIC_REGION        — "eu" (default) or "us"
  CUR_COSTS_FILE          — path to costs.json                (default: costs.json)
  CUR_DAILY_FILE          — path to daily-costs.json          (default: daily-costs.json)
  CUR_SUMMARY_FILE        — path to summary.json              (default: summary.json)
  CUR_TAGS_FILE           — path to costs-by-tag.json         (default: costs-by-tag.json)
  CUR_USAGE_GROUP_FILE    — path to costs-by-usage-group.json (default: costs-by-usage-group.json)
  BILLING_PERIOD          — override e.g. "2026-07" (default: auto from files)
  BATCH_SIZE              — NR metric batch size        (default: 500)
"""

import json
import os
import re
import sys
import time
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ── Env vars ──────────────────────────────────────────────────────────────────
NR_LICENSE_KEY   = os.environ["NEW_RELIC_LICENSE_KEY"]
_NR_REGION       = os.environ.get("NEW_RELIC_REGION", "eu").strip().lower()
_NR_METRIC_HOST  = "metric-api.eu.newrelic.com" if _NR_REGION == "eu" else "metric-api.newrelic.com"
NR_METRICS_URL   = os.environ.get("NEW_RELIC_METRICS_URL") or f"https://{_NR_METRIC_HOST}/metric/v1"
logger.info("New Relic region=%s  metrics endpoint=%s", _NR_REGION, NR_METRICS_URL)

CUR_COSTS_FILE       = os.environ.get("CUR_COSTS_FILE",       "costs.json")
CUR_DAILY_FILE       = os.environ.get("CUR_DAILY_FILE",       "daily-costs.json")
CUR_SUMMARY_FILE     = os.environ.get("CUR_SUMMARY_FILE",     "summary.json")
CUR_TAGS_FILE        = os.environ.get("CUR_TAGS_FILE",        "costs-by-tag.json")
CUR_USAGE_GROUP_FILE = os.environ.get("CUR_USAGE_GROUP_FILE", "costs-by-usage-group.json")
BATCH_SIZE           = int(os.environ.get("BATCH_SIZE") or "500")

# Run timestamp used for any metric that represents a "month-to-date, updated
# daily" aggregate (summary + tag rollups). These must NOT be stamped on the
# 1st of the billing month: the New Relic Metric API silently drops any point
# with a timestamp more than 48 hours in the past (or 24h in the future) from
# when it's received — https://docs.newrelic.com/docs/data-apis/ingest-apis/metric-api/report-metrics-metric-api/
# Since this job runs daily, a 1st-of-month timestamp is only valid for the
# first ~2 days of the month; after that every summary/tag metric is
# silently accepted (HTTP 202) and then dropped. Stamping "now" instead makes
# these true daily snapshots of the month-to-date total, which is what the
# dashboard actually wants, and keeps them inside the accepted window forever.
RUN_TS_MS = int(time.time() * 1000)


# ─────────────────────────────────────────────────────────────────────────────
# File loader
# ─────────────────────────────────────────────────────────────────────────────

def load_json(path: str) -> dict | list | None:
    if not os.path.exists(path):
        logger.warning("File not found, skipping: %s", path)
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        logger.info("Loaded %s", path)
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load %s: %s", path, e)
        return None


def date_to_ts_ms(date_str: str) -> int:
    """Convert 'YYYY-MM-DD' to Unix milliseconds (noon UTC to avoid DST edge cases)."""
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=12, tzinfo=timezone.utc
    )
    return int(d.timestamp() * 1000)


def month_to_ts_ms(month_str: str) -> int:
    """Convert 'YYYY-MM' to Unix ms at noon on the 1st of that month."""
    return date_to_ts_ms(f"{month_str}-01")


# ─────────────────────────────────────────────────────────────────────────────
# Metric builders
# ─────────────────────────────────────────────────────────────────────────────

def make_gauge(name: str, value: float, timestamp_ms: int, attrs: dict) -> dict:
    return {
        "name":        name,
        "type":        "gauge",
        "value":       round(float(value), 8),
        "timestamp":   timestamp_ms,
        "interval.ms": 86_400_000,   # 1 day — all CUR metrics are daily
        "attributes":  {k: str(v) for k, v in attrs.items()},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Collectors — one per JSON file
# ─────────────────────────────────────────────────────────────────────────────

def collect_costs(data: list) -> list[dict]:
    """
    costs.json — granular daily cost per service per region.
    Emits:
      aws.cur.service.unblended_cost   (USD)
      aws.cur.service.blended_cost     (USD)
      aws.cur.service.usage_quantity
      aws.cur.service.record_count
    """
    metrics = []
    for row in data:
        ts = date_to_ts_ms(row["date"])
        attrs = {
            "source":       "cur",
            "file":         "costs",
            "service":      row["service"],
            "service_name": row["serviceName"],
            "region":       row.get("region", "us-east-1"),
            "date":         row["date"],
        }
        metrics.append(make_gauge("aws.cur.service.unblended_cost",  row["totalCost"],        ts, attrs))
        metrics.append(make_gauge("aws.cur.service.blended_cost",    row["totalBlendedCost"], ts, attrs))
        metrics.append(make_gauge("aws.cur.service.usage_quantity",  row["totalUsage"],       ts, attrs))
        metrics.append(make_gauge("aws.cur.service.record_count",    row["recordCount"],      ts, attrs))
    logger.info("costs.json → %d metrics from %d rows", len(metrics), len(data))
    return metrics


def collect_daily(data: dict) -> list[dict]:
    """
    daily-costs.json — daily total cost + per-service breakdown.
    Emits:
      aws.cur.daily.total_cost          (total for the day)
      aws.cur.daily.service_cost        (per-service for the day)
    """
    metrics = []
    # normalise both "20260701" and "2026-07-01" → "2026-07"
    bp_raw = re.sub(r'[^0-9]', '', data.get("billingPeriod", {}).get("start", ""))[:8]
    billing_period = f"{bp_raw[:4]}-{bp_raw[4:6]}" if len(bp_raw) >= 6 else "unknown"

    for day in data.get("dailyCosts", []):
        ts      = date_to_ts_ms(day["date"])
        day_attrs = {
            "source":         "cur",
            "file":           "daily-costs",
            "date":           day["date"],
            "billing_period": billing_period,
        }
        metrics.append(make_gauge("aws.cur.daily.total_cost", day["totalCost"], ts, day_attrs))

        for svc in day.get("services", []):
            svc_attrs = {**day_attrs,
                         "service":      svc["service"],
                         "service_name": svc["serviceName"]}
            metrics.append(make_gauge("aws.cur.daily.service_cost", svc["cost"], ts, svc_attrs))

    # Monthly-to-date total as a single gauge stamped "now" (see RUN_TS_MS
    # note above) so it isn't dropped by New Relic once we're past day 2 of
    # the month. This is a running total, re-sent daily as the month
    # progresses — not a one-time end-of-month value.
    monthly_total = data.get("monthlyTotal", 0)
    if monthly_total:
        metrics.append(make_gauge("aws.cur.monthly.total_cost", monthly_total, RUN_TS_MS, {
            "source":         "cur",
            "file":           "daily-costs",
            "billing_period": billing_period,
        }))

    logger.info("daily-costs.json → %d metrics", len(metrics))
    return metrics


def collect_summary(data: list) -> list[dict]:
    """
    summary.json — monthly cost per service.
    Emits:
      aws.cur.summary.monthly_total     (total for the month)
      aws.cur.summary.service_cost      (per-service for the month)
    """
    metrics = []
    for month_row in data:
        month = month_row["month"]               # "2026-07"
        # Stamped "now", not on the 1st of the month — see RUN_TS_MS note.
        # summary.json is regenerated daily from month-to-date data, so this
        # is correctly a fresh snapshot each run, not a stale historical value.
        ts    = RUN_TS_MS
        month_attrs = {
            "source":         "cur",
            "file":           "summary",
            "billing_period": month,
        }
        metrics.append(make_gauge("aws.cur.summary.monthly_total", month_row["totalCost"], ts, month_attrs))

        for svc in month_row.get("services", []):
            svc_attrs = {**month_attrs,
                         "service":      svc["service"],
                         "service_name": svc["serviceName"]}
            metrics.append(make_gauge("aws.cur.summary.service_cost", svc["cost"], ts, svc_attrs))

    logger.info("summary.json → %d metrics from %d months", len(metrics), len(data))
    return metrics


def collect_tags(data: dict) -> list[dict]:
    """
    costs-by-tag.json — cost breakdown by application tag + uncategorized.
    Emits:
      aws.cur.tag.app_total_cost        (total for an application tag)
      aws.cur.tag.app_service_cost      (per-service within an application)
      aws.cur.tag.uncategorized_total   (total untagged cost)
      aws.cur.tag.uncategorized_service (per-service untagged cost)
    """
    metrics = []
    bp_raw = data.get("billingPeriod", {}).get("start", "")
    billing_period = f"{bp_raw[:4]}-{bp_raw[4:6]}" if len(bp_raw) >= 6 else "unknown"
    # Stamped "now", not on the 1st of the month — see RUN_TS_MS note above.
    ts = RUN_TS_MS

    base_attrs = {
        "source":         "cur",
        "file":           "costs-by-tag",
        "billing_period": billing_period,
    }

    # Tagged applications
    for app in data.get("byApplication", []):
        app_name  = app.get("application", "unknown")
        app_attrs = {**base_attrs, "application": app_name, "tagged": "true"}
        metrics.append(make_gauge("aws.cur.tag.app_total_cost", app.get("totalCost", 0), ts, app_attrs))
        for svc in app.get("services", []):
            svc_attrs = {**app_attrs, "service": svc["service"], "service_name": svc["serviceName"]}
            metrics.append(make_gauge("aws.cur.tag.app_service_cost", svc["cost"], ts, svc_attrs))

    # Uncategorized
    uncat = data.get("uncategorized", {})
    if uncat:
        uncat_attrs = {**base_attrs, "application": "uncategorized", "tagged": "false"}
        metrics.append(make_gauge("aws.cur.tag.uncategorized_total", uncat.get("totalCost", 0), ts, uncat_attrs))
        for svc in uncat.get("services", []):
            svc_attrs = {**uncat_attrs, "service": svc["service"], "service_name": svc["serviceName"]}
            metrics.append(make_gauge("aws.cur.tag.uncategorized_service", svc["cost"], ts, svc_attrs))

    logger.info("costs-by-tag.json → %d metrics (%d apps + uncategorized)",
                len(metrics), len(data.get("byApplication", [])))
    return metrics


def collect_usage_group(data: list) -> list[dict]:
    """
    costs-by-usage-group.json — granular daily cost per service + usageGroup
    (the normalized resource-naming group, e.g. "s3-storage", "bedrock-nova-lite",
    "lambda-compute" — see USAGE_GROUP_PATTERNS in cur_processor.py). This is
    what lets the dashboard group resources under one common label instead of
    dozens of raw AWS usageType strings.

    Emits:
      aws.cur.usage_group.cost            (USD)
      aws.cur.usage_group.usage_quantity
      aws.cur.usage_group.record_count
    """
    metrics = []
    for row in data:
        ts = date_to_ts_ms(row["date"])
        attrs = {
            "source":       "cur",
            "file":         "costs-by-usage-group",
            "service":      row["service"],
            "service_name": row["serviceName"],
            "usage_group":  row["usageGroup"],
            "region":       row.get("region", "global"),
            "date":         row["date"],
        }
        metrics.append(make_gauge("aws.cur.usage_group.cost",           row["totalCost"],   ts, attrs))
        metrics.append(make_gauge("aws.cur.usage_group.usage_quantity", row["usageAmount"], ts, attrs))
        metrics.append(make_gauge("aws.cur.usage_group.record_count",   row["recordCount"], ts, attrs))
    logger.info("costs-by-usage-group.json → %d metrics from %d rows", len(metrics), len(data))
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# New Relic push
# ─────────────────────────────────────────────────────────────────────────────

def build_payload(metrics: list[dict]) -> list[dict]:
    return [{
        "common": {
            "attributes": {
                "forwarder":  "github-actions-cur",
                "data_type":  "cur",
            },
            "interval.ms": 86_400_000,
        },
        "metrics": metrics,
    }]


def push_metrics(metrics: list[dict]) -> None:
    if not metrics:
        logger.warning("No metrics to push")
        return

    total_pushed = 0
    for i in range(0, len(metrics), BATCH_SIZE):
        batch = metrics[i: i + BATCH_SIZE]
        payload = build_payload(batch)
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
                logger.info("NR Metrics HTTP %d batch %d-%d: %s",
                            resp.status, i, i + len(batch), body_text)
                if resp.status not in (200, 202):
                    raise RuntimeError(f"NR Metric API returned HTTP {resp.status}: {body_text}")
            total_pushed += len(batch)
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"NR Metric API error {e.code}: {body_text}") from e

    logger.info("Total metrics pushed: %d", total_pushed)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    all_metrics: list[dict] = []
    stats = {
        "costs_metrics":       0,
        "daily_metrics":       0,
        "summary_metrics":     0,
        "tags_metrics":        0,
        "usage_group_metrics": 0,
        "total_metrics":       0,
    }

    # costs.json
    costs_data = load_json(CUR_COSTS_FILE)
    if isinstance(costs_data, list) and costs_data:
        m = collect_costs(costs_data)
        all_metrics.extend(m)
        stats["costs_metrics"] = len(m)

    # daily-costs.json
    daily_data = load_json(CUR_DAILY_FILE)
    if isinstance(daily_data, dict):
        m = collect_daily(daily_data)
        all_metrics.extend(m)
        stats["daily_metrics"] = len(m)

    # summary.json
    summary_data = load_json(CUR_SUMMARY_FILE)
    if isinstance(summary_data, list) and summary_data:
        m = collect_summary(summary_data)
        all_metrics.extend(m)
        stats["summary_metrics"] = len(m)

    # costs-by-tag.json
    tags_data = load_json(CUR_TAGS_FILE)
    if isinstance(tags_data, dict):
        m = collect_tags(tags_data)
        all_metrics.extend(m)
        stats["tags_metrics"] = len(m)

    # costs-by-usage-group.json (resource-naming/grouping)
    usage_group_data = load_json(CUR_USAGE_GROUP_FILE)
    if isinstance(usage_group_data, list) and usage_group_data:
        m = collect_usage_group(usage_group_data)
        all_metrics.extend(m)
        stats["usage_group_metrics"] = len(m)

    stats["total_metrics"] = len(all_metrics)
    logger.info("Collected metrics: %s", stats)

    push_metrics(all_metrics)

    result = {"statusCode": 0, "body": stats}
    logger.info("Run complete: %s", stats)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
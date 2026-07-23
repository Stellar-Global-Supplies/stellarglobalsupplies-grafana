"""
Web Traffic → New Relic Metric API forwarder

Reads web-traffic-report.json and pushes metrics to New Relic.
Uses aws.webtraffic.* metric namespace for all web traffic data.

Environment variables:
  NEW_RELIC_LICENSE_KEY  - New Relic ingest license key (required)
  NEW_RELIC_REGION       - "us" or "eu" (default: eu)
  WEB_TRAFFIC_REPORT     - Path to web-traffic-report.json (default: web-traffic-report.json)
"""

import json
import os
import sys
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

# ── Configuration ─────────────────────────────────────────────────────────────
NR_LICENSE_KEY = os.environ["NEW_RELIC_LICENSE_KEY"]
_NR_REGION     = os.environ.get("NEW_RELIC_REGION", "eu").strip().lower()
_NR_HOST       = "metric-api.eu.newrelic.com" if _NR_REGION == "eu" else "metric-api.newrelic.com"
NR_METRICS_URL = os.environ.get("NEW_RELIC_METRICS_URL") or f"https://{_NR_HOST}/metric/v1"
logger.info("New Relic region=%s  metrics endpoint=%s", _NR_REGION, NR_METRICS_URL)

REPORT_FILE = os.environ.get("WEB_TRAFFIC_REPORT", "web-traffic-report.json")
WEB_TRAFFIC_STATE_FILE = os.environ.get("WEB_TRAFFIC_STATE_FILE", "web-traffic-state.json")

# ─────────────────────────────────────────────────────────────────────────────
# Persistent deduplication state
# ─────────────────────────────────────────────────────────────────────────────
import hashlib
from pathlib import Path

def _load_state(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read state file %s: %s; starting fresh", path, exc)
        return {}

def _save_state_atomic(path: str, state: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, p)

def _stable_hash(value) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

BATCH_SIZE  = int(os.environ.get("BATCH_SIZE") or "500")


# ── Load web traffic report ───────────────────────────────────────────────────
def load_report() -> dict[str, Any] | None:
    """Load web-traffic-report.json from disk."""
    if not os.path.exists(REPORT_FILE):
        logger.error(f"Report file not found: {REPORT_FILE}")
        return None
    
    try:
        with open(REPORT_FILE, 'r') as f:
            report = json.load(f)
        logger.info(f"Loaded report: {report.get('period')} — {report.get('summary', {}).get('total_requests')} requests")
        return report
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to read report file: {e}")
        return None


# ── New Relic Metric API helpers ─────────────────────────────────────────────
def build_metric_object(
    metric_name: str,
    value: float,
    timestamp_ms: int,
    attributes: dict[str, str],
    interval_ms: int = 86_400_000,  # 24 hours for daily metrics
) -> dict[str, Any]:
    """Build a New Relic metric object."""
    return {
        "name":        f"aws.webtraffic.{metric_name}",
        "type":        "gauge",
        "value":       float(value),
        "timestamp":   timestamp_ms,
        "interval.ms": interval_ms,
        "attributes":  {k: str(v) for k, v in attributes.items()},
    }


def build_nr_metric_payload(metrics: list[dict]) -> list[dict]:
    """Build New Relic Metric API payload."""
    return [
        {
            "common": {
                "attributes": {
                    "forwarder": "github-actions",
                    "source":    "cloudfront-logs",
                },
                "interval.ms": 86_400_000,  # 24 hours
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


# ── Convert report to New Relic metrics ──────────────────────────────────────
def report_to_metrics(report: dict[str, Any]) -> list[dict]:
    """Convert web traffic report to New Relic metric objects."""
    metrics = []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    summary = report.get("summary", {})
    generated_at = report.get("generated_at", datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
    
    # Parse timestamp from report
    try:
        ts = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
        timestamp_ms = int(ts.timestamp() * 1000)
    except Exception:
        timestamp_ms = now_ms

    base_attrs = {
        "period": report.get("period", "7-day"),
        "generated_at": generated_at,
    }

    # ── Summary metrics ───────────────────────────────────────────────────────
    metrics.append(build_metric_object(
        "summary.total_requests",
        summary.get("total_requests", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.unique_ips",
        summary.get("unique_ips", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.avg_daily",
        summary.get("avg_daily", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.mobile_pct",
        summary.get("mobile_pct", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.desktop_pct",
        summary.get("desktop_pct", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.peak_hour",
        int(summary.get("peak_hour", "00:00").split(":")[0]),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.error_rate",
        summary.get("error_rate", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.cache_hit_pct",
        summary.get("cache_hit_pct", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "summary.https_pct",
        summary.get("https_pct", 0),
        timestamp_ms,
        base_attrs,
    ))

    # ── Traffic over time (daily) ─────────────────────────────────────────────
    for entry in report.get("traffic_over_time", []):
        date_str = entry.get("date", "")
        if not date_str:
            continue
        # Use the date as timestamp for time-series data
        try:
            day_ts = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            day_timestamp_ms = int(day_ts.timestamp() * 1000)
        except Exception:
            day_timestamp_ms = timestamp_ms
        
        day_attrs = {**base_attrs, "date": date_str}
        metrics.append(build_metric_object(
            "traffic.daily.requests",
            entry.get("requests", 0),
            day_timestamp_ms,
            day_attrs,
        ))

    # ── Top pages ─────────────────────────────────────────────────────────────
    for idx, page in enumerate(report.get("top_pages", []), 1):
        page_attrs = {
            **base_attrs,
            "page": page.get("page", ""),
            "rank": idx,
        }
        metrics.append(build_metric_object(
            "pages.visits",
            page.get("visits", 0),
            timestamp_ms,
            page_attrs,
        ))

    # ── Geo distribution ──────────────────────────────────────────────────────
    for geo in report.get("geo_distribution", []):
        geo_attrs = {
            **base_attrs,
            "country": geo.get("country", ""),
        }
        metrics.append(build_metric_object(
            "geo.requests",
            geo.get("requests", 0),
            timestamp_ms,
            geo_attrs,
        ))
        metrics.append(build_metric_object(
            "geo.pct",
            geo.get("pct", 0),
            timestamp_ms,
            geo_attrs,
        ))

    # ── Device split ──────────────────────────────────────────────────────────
    for device in report.get("device_split", []):
        device_attrs = {
            **base_attrs,
            "device": device.get("device", ""),
        }
        metrics.append(build_metric_object(
            "devices.pct",
            device.get("pct", 0),
            timestamp_ms,
            device_attrs,
        ))

    # ── Peak hours ────────────────────────────────────────────────────────────
    for hour_entry in report.get("peak_hours", []):
        hour_attrs = {
            **base_attrs,
            "hour": hour_entry.get("hour", 0),
        }
        metrics.append(build_metric_object(
            "traffic.hourly.requests",
            hour_entry.get("requests", 0),
            timestamp_ms,
            hour_attrs,
        ))

    # ── HTTP Status distribution ──────────────────────────────────────────────
    for status in report.get("status_distribution", []):
        status_attrs = {
            **base_attrs,
            "status_code": status.get("status", ""),
        }
        metrics.append(build_metric_object(
            "http.status.count",
            status.get("count", 0),
            timestamp_ms,
            status_attrs,
        ))
        metrics.append(build_metric_object(
            "http.status.pct",
            status.get("pct", 0),
            timestamp_ms,
            status_attrs,
        ))

    # ── Browser distribution ──────────────────────────────────────────────────
    for browser in report.get("browser_distribution", []):
        browser_attrs = {
            **base_attrs,
            "browser": browser.get("browser", ""),
        }
        metrics.append(build_metric_object(
            "browsers.pct",
            browser.get("pct", 0),
            timestamp_ms,
            browser_attrs,
        ))

    # ── OS distribution ───────────────────────────────────────────────────────
    for os_entry in report.get("os_distribution", []):
        os_attrs = {
            **base_attrs,
            "os": os_entry.get("os", ""),
        }
        metrics.append(build_metric_object(
            "os.pct",
            os_entry.get("pct", 0),
            timestamp_ms,
            os_attrs,
        ))

    # ── HTTP methods ──────────────────────────────────────────────────────────
    for method in report.get("method_distribution", []):
        method_attrs = {
            **base_attrs,
            "method": method.get("method", ""),
        }
        metrics.append(build_metric_object(
            "http.methods.pct",
            method.get("pct", 0),
            timestamp_ms,
            method_attrs,
        ))

    # ── Top referrers ─────────────────────────────────────────────────────────
    for idx, ref in enumerate(report.get("top_referrers", []), 1):
        ref_attrs = {
            **base_attrs,
            "referer": ref.get("referer", ""),
            "rank": idx,
        }
        metrics.append(build_metric_object(
            "referrers.visits",
            ref.get("visits", 0),
            timestamp_ms,
            ref_attrs,
        ))

    # ── Cache distribution ────────────────────────────────────────────────────
    cache_dist = report.get("cache_distribution", {})
    metrics.append(build_metric_object(
        "cache.hit_pct",
        cache_dist.get("hit_pct", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "cache.miss_pct",
        cache_dist.get("miss_pct", 0),
        timestamp_ms,
        base_attrs,
    ))

    # ── Response metrics ──────────────────────────────────────────────────────
    resp_metrics = report.get("response_metrics", {})
    metrics.append(build_metric_object(
        "response.avg_size_bytes",
        resp_metrics.get("avg_response_size_bytes", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "request.avg_size_bytes",
        resp_metrics.get("avg_request_size_bytes", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "response.avg_time_seconds",
        resp_metrics.get("avg_response_time_seconds", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "response.total_bytes",
        resp_metrics.get("total_bytes_transferred", 0),
        timestamp_ms,
        base_attrs,
    ))

    # ── Meta insights ─────────────────────────────────────────────────────────
    meta = report.get("meta_insights", {})
    metrics.append(build_metric_object(
        "insights.warm_audience_size",
        meta.get("warm_audience_size", 0),
        timestamp_ms,
        base_attrs,
    ))
    metrics.append(build_metric_object(
        "insights.high_intent_visits",
        meta.get("high_intent_visits", 0),
        timestamp_ms,
        base_attrs,
    ))

    logger.info(f"Generated {len(metrics)} metric objects from report")
    return metrics


# ── Main entry point ─────────────────────────────────────────────────────────
def main():
    """Load report, deduplicate its semantic content, then forward to New Relic."""
    logger.info("Starting web traffic forwarder...")

    report = load_report()
    if not report:
        logger.error("No report to forward")
        return 1

    # generated_at changes on every processor run even when the underlying
    # report is identical. Exclude it from the fingerprint.
    fingerprint_report = dict(report)
    fingerprint_report.pop("generated_at", None)
    report_hash = _stable_hash(fingerprint_report)

    state = _load_state(WEB_TRAFFIC_STATE_FILE)
    if state.get("sha256") == report_hash:
        logger.info("Web traffic report unchanged (sha256=%s) — skipping New Relic ingest",
                    report_hash[:12])
        print(json.dumps({"statusCode": 0, "sent": 0, "failed": 0, "skipped": True}))
        return 0

    metrics = report_to_metrics(report)
    if not metrics:
        logger.warning("No metrics generated from report")
        return 0

    total_sent = 0
    for i in range(0, len(metrics), BATCH_SIZE):
        batch = metrics[i:i + BATCH_SIZE]
        payload = build_nr_metric_payload(batch)
        try:
            push_to_nr_metrics(payload)
            total_sent += len(batch)
        except Exception as exc:
            # Do not advance state on a partial/failed run. The next execution
            # retries the report rather than silently losing telemetry.
            logger.error("Failed to push batch %d: %s", i, exc)
            print(json.dumps({
                "statusCode": 1, "sent": total_sent,
                "failed": len(metrics) - total_sent, "skipped": False,
            }))
            return 1

    now = datetime.now(timezone.utc).isoformat()
    _save_state_atomic(WEB_TRAFFIC_STATE_FILE, {
        "sha256": report_hash,
        "last_successful_push": now,
        "metric_count": total_sent,
        "period": report.get("period"),
    })

    logger.info("Forwarding complete: sent=%d; state=%s", total_sent, WEB_TRAFFIC_STATE_FILE)
    print(json.dumps({
        "statusCode": 0, "sent": total_sent, "failed": 0, "skipped": False,
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""
CloudWatch Logs → New Relic Logs forwarder (GitHub Actions version)
- Discovers all CW log groups
- Fetches events since last run (stored in state.json)
- Pushes to New Relic Logs API
- Updates last-fetch timestamps in state.json
"""

import json
import os
import sys
import time
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone

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
LOG_GROUP_PREFIX      = os.environ.get("LOG_GROUP_PREFIX", "")    # optional filter prefix
BATCH_SIZE            = int(os.environ.get("BATCH_SIZE") or "500")
LOOKBACK_HOURS        = int(os.environ.get("LOOKBACK_HOURS") or "5")
STATE_FILE            = os.environ.get("STATE_FILE") or "state.json"

# ── New Relic endpoints ──────────────────────────────────────────────────────
NR_LOG_API_URL = (
    "https://log-api.eu.newrelic.com/log/v1"
    if NEW_RELIC_REGION == "eu"
    else "https://log-api.newrelic.com/log/v1"
)

# ── AWS clients ──────────────────────────────────────────────────────────────
cwlogs = boto3.client("logs", region_name=AWS_REGION)


# ─────────────────────────────────────────────────────────────────────────────
# State file helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    """Load state from state.json. Returns {logGroup: {lastFetchMs, updatedAt}, ...}"""
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
    """Write state to state.json atomically."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)
    logger.debug("State saved to %s", STATE_FILE)


def get_last_fetch_ms(log_group: str, state: dict) -> int:
    """Return last successful fetch timestamp in epoch-milliseconds, or 0."""
    entry = state.get(log_group)
    if entry and "lastFetchMs" in entry:
        return int(entry["lastFetchMs"])
    # First run: look back LOOKBACK_HOURS
    return int((time.time() - LOOKBACK_HOURS * 3600) * 1000)


def set_last_fetch_ms(log_group: str, ts_ms: int, state: dict) -> None:
    state[log_group] = {
        "lastFetchMs": ts_ms,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch helpers
# ─────────────────────────────────────────────────────────────────────────────

def list_log_groups() -> list[str]:
    groups = []
    kwargs = {}
    if LOG_GROUP_PREFIX:
        kwargs["logGroupNamePrefix"] = LOG_GROUP_PREFIX
    paginator = cwlogs.get_paginator("describe_log_groups")
    for page in paginator.paginate(**kwargs):
        for g in page.get("logGroups", []):
            groups.append(g["logGroupName"])
    logger.info("Discovered %d log groups", len(groups))
    return groups


def fetch_log_events(log_group: str, start_ms: int, end_ms: int) -> list[dict]:
    """Fetch all log events across all streams since start_ms."""
    events = []
    paginator = cwlogs.get_paginator("filter_log_events")
    try:
        for page in paginator.paginate(
            logGroupName=log_group,
            startTime=start_ms,
            endTime=end_ms,
        ):
            for ev in page.get("events", []):
                events.append(ev)
            if len(events) >= 10_000:
                logger.warning("%s: capping at 10 000 events", log_group)
                break
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            logger.warning("Log group not found (may have been deleted): %s", log_group)
        else:
            raise
    return events


# ─────────────────────────────────────────────────────────────────────────────
# New Relic Logs push
# ─────────────────────────────────────────────────────────────────────────────

def build_nr_log_payload(log_group: str, events: list[dict]) -> list[dict]:
    """
    New Relic Logs API payload format (with common block for shared attributes):
    [
      {
        "common": {
          "attributes": {
            "log_group": "...",
            "region": "...",
            "job": "cloudwatch-forwarder"
          }
        },
        "logs": [
          { "timestamp": <epoch_ms>, "message": "<log line>" },
          ...
        ]
      }
    ]
    """
    log_entries = []
    for ev in events:
        ts_ms = ev["timestamp"]  # already in milliseconds
        message = ev.get("message", "").rstrip("\n")
        log_entries.append({
            "timestamp": ts_ms,
            "message": message,
        })

    return [
        {
            "common": {
                "attributes": {
                    "log_group": log_group,
                    "region": AWS_REGION,
                    "job": "cloudwatch-forwarder",
                }
            },
            "logs": log_entries,
        }
    ]


def push_to_new_relic(payload: list[dict]) -> None:
    """Push log entries to New Relic Logs API."""
    if not payload:
        return

    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        NR_LOG_API_URL,
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
            logger.info("New Relic Logs API response HTTP %d: %s", resp.status, body_text)
            if resp.status not in (200, 202):
                raise RuntimeError(f"New Relic Logs API returned HTTP {resp.status}: {body_text}")
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        logger.error("New Relic Logs API HTTP error %d: %s", e.code, body_text)
        raise RuntimeError(f"New Relic Logs API HTTP error {e.code}: {body_text}") from e


def push_in_batches(log_group: str, events: list[dict]) -> None:
    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i : i + BATCH_SIZE]
        payload = build_nr_log_payload(log_group, batch)
        push_to_new_relic(payload)
        logger.debug("%s: pushed batch %d-%d", log_group, i, i + len(batch))


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    run_start_ms = int(time.time() * 1000)
    state = load_state()
    log_groups = list_log_groups()

    stats = {"groups_processed": 0, "groups_failed": 0, "total_events": 0}
    state_modified = False

    for lg in log_groups:
        start_ms = get_last_fetch_ms(lg, state)
        end_ms = run_start_ms

        if start_ms >= end_ms:
            logger.debug("%s: nothing new (start >= end)", lg)
            continue

        try:
            events = fetch_log_events(lg, start_ms, end_ms)
            if events:
                push_in_batches(lg, events)
                stats["total_events"] += len(events)
                logger.info("%s: pushed %d events", lg, len(events))

            set_last_fetch_ms(lg, end_ms, state)
            state_modified = True
            stats["groups_processed"] += 1

        except Exception as exc:
            logger.error("%s: failed – %s", lg, exc, exc_info=True)
            stats["groups_failed"] += 1

    if state_modified:
        save_state(state)

    logger.info("Run complete: %s", stats)
    print(json.dumps({"statusCode": 0, "body": stats}))


if __name__ == "__main__":
    main()
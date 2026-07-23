"""
main.py — Supabase → New Relic monitor entrypoint.

Usage (from repo root's supabase/ folder):
    python main.py

Required secrets (GitHub Actions → Settings → Secrets):
    SUPABASE_DB_URL        Postgres connection string — this is all that's needed
                           to authenticate with Supabase. No anon key, no service
                           role key, no REST API. Direct Postgres connection only.
                           Format: postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres

    NEW_RELIC_LICENSE_KEY  New Relic Ingest License Key (EU account).
                           Found in: NR One → API Keys → INGEST - LICENSE

State file:
    supabase-state.json — SHA-256 fingerprint of the last successfully shipped
    metric set. Stored on the 'state' branch by the GitHub Actions workflow
    so that unchanged metrics are not re-sent on every hourly run.
"""

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from collectors.supabase_collector import SupabaseCollector
from shippers.newrelic_shipper import NewRelicShipper

# ---------------------------------------------------------------------------
# Logging setup — structured, timestamps, level from env
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
logger = logging.getLogger("supabase_monitor")

# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------
SUPABASE_STATE_FILE = os.environ.get("SUPABASE_STATE_FILE", "supabase-state.json")


# ---------------------------------------------------------------------------
# State file helpers  (mirrors the pattern used by forward_cur.py /
# forward-web-traffic.py — atomic writes, SHA-256 fingerprints)
# ---------------------------------------------------------------------------

def _load_state(path: str) -> dict:
    """Load state from *path*. Returns {} on any error."""
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
    """Write *state* to *path* atomically (temp file + os.replace)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, p)


def _stable_hash(value) -> str:
    """Deterministic SHA-256 of any JSON-serialisable value."""
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _metrics_fingerprint(metrics: list[dict]) -> str:
    """
    Build a SHA-256 fingerprint of the collected metrics.

    Timestamps are excluded because they are stamped at collection time
    (``_now_ms()``) and therefore change on every run even when the
    underlying data is identical.  We hash name / type / value / attributes
    so that only genuine data changes trigger a New Relic ingest.
    """
    fingerprintable = [
        {
            "name":       m.get("name"),
            "type":       m.get("type"),
            "value":      m.get("value"),
            "interval.ms": m.get("interval.ms"),
            "attributes": m.get("attributes", {}),
        }
        for m in metrics
    ]
    return _stable_hash(fingerprintable)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    start = time.monotonic()
    run_ts = datetime.now(timezone.utc).isoformat()
    logger.info("=== Supabase → New Relic monitor  run_at=%s ===", run_ts)

    # ── Config ──────────────────────────────────────────────────────────
    db_url  = os.getenv("SUPABASE_DB_URL")
    nr_key  = os.getenv("NEW_RELIC_LICENSE_KEY")

    if not db_url:
        logger.critical("SUPABASE_DB_URL is not set. Aborting.")
        return 1
    if not nr_key:
        logger.critical("NEW_RELIC_LICENSE_KEY is not set. Aborting.")
        return 1

    # ── Collect ─────────────────────────────────────────────────────────
    collector = SupabaseCollector(dsn=db_url)
    metrics: list = []
    try:
        collector.connect()
        metrics = collector.collect_all()
    except Exception as exc:
        logger.error("Collection failed: %s", exc)
        return 1
    finally:
        collector.close()

    logger.info("Total metrics collected: %d", len(metrics))

    if not metrics:
        logger.warning("No metrics collected — nothing to ship.")
        return 0

    # ── Deduplicate ─────────────────────────────────────────────────────
    # Hash the metrics (excluding timestamps) and compare with the last
    # successful run.  If nothing changed, skip the New Relic ingest to
    # avoid burning API quota on identical data.
    fingerprint = _metrics_fingerprint(metrics)
    state = _load_state(SUPABASE_STATE_FILE)
    previous_hash = state.get("sha256")

    if previous_hash == fingerprint:
        logger.info("Metrics unchanged (sha256=%s) — skipping New Relic ingest",
                    fingerprint[:12])
        print(json.dumps({
            "statusCode": 0, "sent": 0, "failed": 0,
            "skipped": True, "sha256": fingerprint[:12],
        }))
        return 0

    # ── Ship ─────────────────────────────────────────────────────────────
    shipper  = NewRelicShipper(license_key=nr_key)
    summary  = shipper.ship_metrics(metrics)

    # State is advanced only after every batch was accepted (failed == 0).
    if summary["failed"] > 0:
        logger.error("Run completed with failures — %d metric(s) not delivered. "
                     "State not advanced; next run will retry.", summary["failed"])
        print(json.dumps({
            "statusCode": 1, "sent": summary["sent"],
            "failed": summary["failed"], "skipped": False,
        }))
        return 1

    elapsed  = round(time.monotonic() - start, 2)
    run_info = {
        **summary,
        "run_at":      run_ts,
        "duration_s":  elapsed,
        "total_collected": len(metrics),
        "sha256":      fingerprint[:12],
    }

    shipper.ship_run_log(run_info)

    # ── Persist state ────────────────────────────────────────────────────
    _save_state_atomic(SUPABASE_STATE_FILE, {
        "sha256":             fingerprint,
        "last_successful_push": run_ts,
        "metric_count":       len(metrics),
        "sent":               summary["sent"],
        "failed":             summary["failed"],
        "batches":            summary["batches"],
    })

    logger.info("=== Run complete in %.2fs  sent=%d  state=%s ===",
                elapsed, summary["sent"], SUPABASE_STATE_FILE)
    print(json.dumps({
        "statusCode": 0, "sent": summary["sent"],
        "failed": 0, "skipped": False,
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())

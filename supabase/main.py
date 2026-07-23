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
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

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

    # ── Ship ─────────────────────────────────────────────────────────────
    shipper  = NewRelicShipper(license_key=nr_key)
    summary  = shipper.ship_metrics(metrics)

    elapsed  = round(time.monotonic() - start, 2)
    run_info = {
        **summary,
        "run_at":      run_ts,
        "duration_s":  elapsed,
        "total_collected": len(metrics),
    }

    shipper.ship_run_log(run_info)

    if summary["failed"] > 0:
        logger.error("Run completed with failures — %d metric(s) not delivered.", summary["failed"])
        return 1

    logger.info("=== Run complete in %.2fs  sent=%d ===", elapsed, summary["sent"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
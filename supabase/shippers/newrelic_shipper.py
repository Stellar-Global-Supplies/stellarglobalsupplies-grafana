"""
New Relic EU shipper — Metric API + Log API.
Handles batching, retries, and structured run logs.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# EU endpoints
# ---------------------------------------------------------------------------
METRIC_API_URL = "https://metric-api.eu.newrelic.com/metric/v1"
LOG_API_URL    = "https://log-api.eu.newrelic.com/log/v1"

# New Relic Metric API hard limit per request
METRIC_BATCH_SIZE = 2_000


# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# Shipper class
# ---------------------------------------------------------------------------

class NewRelicShipper:
    def __init__(self, license_key: str, service_name: str = "supabase-monitor") -> None:
        self._key          = license_key
        self._service_name = service_name
        self._session      = _build_session()
        self._headers      = {
            "Api-Key":     license_key,
            "Content-Type":"application/json",
        }

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def ship_metrics(self, metrics: list[dict]) -> dict:
        """
        Batch-send metrics to New Relic Metric API.
        Returns a summary dict: {sent, failed, batches}.
        """
        if not metrics:
            logger.warning("No metrics to ship.")
            return {"sent": 0, "failed": 0, "batches": 0}

        # Inject common attributes into every metric
        common_attrs = {
            "service.name": self._service_name,
            "collector":    "supabase-newrelic",
        }
        for m in metrics:
            m["attributes"] = {**common_attrs, **m.get("attributes", {})}

        sent = failed = batches = 0

        for batch in _chunk(metrics, METRIC_BATCH_SIZE):
            batches += 1
            payload = [{"metrics": batch}]
            try:
                resp = self._session.post(
                    METRIC_API_URL,
                    headers=self._headers,
                    data=json.dumps(payload),
                    timeout=30,
                )
                resp.raise_for_status()
                sent += len(batch)
                logger.debug("Batch %d accepted (HTTP %d).", batches, resp.status_code)
            except requests.HTTPError as exc:
                failed += len(batch)
                logger.error("Metric batch %d HTTP error: %s — body: %s",
                             batches, exc, exc.response.text[:500])
            except requests.RequestException as exc:
                failed += len(batch)
                logger.error("Metric batch %d request error: %s", batches, exc)

        logger.info("Metrics — sent=%d  failed=%d  batches=%d", sent, failed, batches)
        return {"sent": sent, "failed": failed, "batches": batches}

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------

    def ship_run_log(self, run_summary: dict) -> bool:
        """
        Push a single structured log entry summarising the run.
        Returns True on success.
        """
        payload = [{
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "message":   f"supabase-monitor run completed — metrics_sent={run_summary.get('sent', 0)}",
            "attributes": {
                "service.name":    self._service_name,
                "logtype":         "supabase_monitor_run",
                **{k: v for k, v in run_summary.items()},
            },
        }]
        try:
            resp = self._session.post(
                LOG_API_URL,
                headers=self._headers,
                data=json.dumps(payload),
                timeout=15,
            )
            resp.raise_for_status()
            logger.info("Run log shipped (HTTP %d).", resp.status_code)
            return True
        except requests.RequestException as exc:
            logger.error("Failed to ship run log: %s", exc)
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i: i + size]
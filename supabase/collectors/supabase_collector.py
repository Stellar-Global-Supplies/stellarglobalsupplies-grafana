"""
Supabase Postgres metrics collector.
Queries pg_stat_* views and returns structured metric payloads.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

SQL_DB_SIZE = """
SELECT
    pg_database_size(current_database()) AS size_bytes,
    current_database()                   AS db_name;
"""

SQL_CONNECTIONS = """
SELECT
    state,
    COUNT(*) AS count
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state;
"""

SQL_TABLE_STATS = """
SELECT
    schemaname,
    relname                                  AS table_name,
    n_live_tup                               AS live_rows,
    n_dead_tup                               AS dead_rows,
    n_tup_ins                                AS rows_inserted,
    n_tup_upd                                AS rows_updated,
    n_tup_del                                AS rows_deleted,
    seq_scan,
    idx_scan,
    pg_total_relation_size(relid)            AS total_size_bytes
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
"""

SQL_BGWRITER = """
SELECT
    checkpoints_timed,
    checkpoints_req,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    buffers_alloc
FROM pg_stat_bgwriter;
"""

SQL_STATEMENTS = """
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat%'
ORDER BY mean_exec_time DESC
LIMIT 20;
"""


# ---------------------------------------------------------------------------
# Collector class
# ---------------------------------------------------------------------------

class SupabaseCollector:
    """Opens one connection, runs all collectors, closes cleanly."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg2.extensions.connection | None = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def connect(self) -> None:
        logger.info("Connecting to Supabase Postgres …")
        self._conn = psycopg2.connect(
            self._dsn,
            connect_timeout=10,
            options="-c statement_timeout=30000",   # 30 s hard limit
        )
        self._conn.set_session(readonly=True, autocommit=True)
        logger.info("Connected.")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Connection closed.")

    def _query(self, sql: str) -> list[dict]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Individual collectors  →  list[dict]  (New Relic metric dicts)
    # ------------------------------------------------------------------

    def collect_db_size(self) -> list[dict]:
        rows = self._query(SQL_DB_SIZE)
        if not rows:
            return []
        row = rows[0]
        return [_gauge("supabase.db.size_bytes", row["size_bytes"],
                        {"db_name": row["db_name"]})]

    def collect_connections(self) -> list[dict]:
        rows = self._query(SQL_CONNECTIONS)
        metrics: list[dict] = []
        state_map = {
            "active":                  "active",
            "idle":                    "idle",
            "idle in transaction":     "idle_in_transaction",
            "idle in transaction (aborted)": "idle_in_transaction_aborted",
            None:                      "unknown",
        }
        totals: dict[str, int] = {}
        for row in rows:
            label = state_map.get(row["state"], "other")
            totals[label] = totals.get(label, 0) + int(row["count"])

        total = sum(totals.values())
        metrics.append(_gauge("supabase.connections.total", total))
        for state, count in totals.items():
            metrics.append(_gauge("supabase.connections.by_state",
                                  count, {"state": state}))
        return metrics

    def collect_table_stats(self) -> list[dict]:
        rows = self._query(SQL_TABLE_STATS)
        metrics: list[dict] = []
        for row in rows:
            attrs = {
                "schema":     row["schemaname"],
                "table_name": row["table_name"],
            }
            metrics += [
                _gauge("supabase.table.live_rows",       row["live_rows"],        attrs),
                _gauge("supabase.table.dead_rows",       row["dead_rows"],        attrs),
                _count("supabase.table.rows_inserted",   row["rows_inserted"],    attrs),
                _count("supabase.table.rows_updated",    row["rows_updated"],     attrs),
                _count("supabase.table.rows_deleted",    row["rows_deleted"],     attrs),
                _count("supabase.table.seq_scans",       row["seq_scan"],         attrs),
                _count("supabase.table.idx_scans",       row["idx_scan"] or 0,    attrs),
                _gauge("supabase.table.total_size_bytes",row["total_size_bytes"], attrs),
            ]
        return metrics

    def collect_bgwriter(self) -> list[dict]:
        rows = self._query(SQL_BGWRITER)
        if not rows:
            return []
        row = rows[0]
        return [
            _count("supabase.bgwriter.checkpoints_timed",  row["checkpoints_timed"]),
            _count("supabase.bgwriter.checkpoints_req",    row["checkpoints_req"]),
            _count("supabase.bgwriter.buffers_checkpoint", row["buffers_checkpoint"]),
            _count("supabase.bgwriter.buffers_clean",      row["buffers_clean"]),
            _count("supabase.bgwriter.buffers_backend",    row["buffers_backend"]),
            _count("supabase.bgwriter.buffers_alloc",      row["buffers_alloc"]),
        ]

    def collect_statements(self) -> list[dict]:
        """Top-20 slowest queries by mean execution time."""
        try:
            rows = self._query(SQL_STATEMENTS)
        except Exception as exc:
            logger.warning("pg_stat_statements not available: %s", exc)
            return []

        metrics: list[dict] = []
        for row in rows:
            # Truncate query to 200 chars for attribute safety
            query_label = (row["query"] or "")[:200].replace("\n", " ")
            attrs = {"query": query_label}
            metrics += [
                _gauge("supabase.statements.mean_exec_time_ms", row["mean_exec_time"], attrs),
                _count("supabase.statements.calls",             row["calls"],           attrs),
                _count("supabase.statements.rows",              row["rows"],            attrs),
                _count("supabase.statements.blks_hit",          row["shared_blks_hit"], attrs),
                _count("supabase.statements.blks_read",         row["shared_blks_read"],attrs),
            ]
        return metrics

    # ------------------------------------------------------------------
    # Run all
    # ------------------------------------------------------------------

    def collect_all(self) -> list[dict]:
        collectors = [
            ("db_size",    self.collect_db_size),
            ("connections",self.collect_connections),
            ("table_stats",self.collect_table_stats),
            ("bgwriter",   self.collect_bgwriter),
            ("statements", self.collect_statements),
        ]
        all_metrics: list[dict] = []
        for name, fn in collectors:
            try:
                result = fn()
                logger.info("%-15s → %d metric(s)", name, len(result))
                all_metrics.extend(result)
            except Exception as exc:
                logger.error("Collector '%s' failed: %s", name, exc)

        return all_metrics


# ---------------------------------------------------------------------------
# Metric builder helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _gauge(name: str, value: Any, attributes: dict | None = None) -> dict:
    return {
        "name":       name,
        "type":       "gauge",
        "value":      float(value) if value is not None else 0.0,
        "timestamp":  _now_ms(),
        "attributes": attributes or {},
    }


def _count(name: str, value: Any, attributes: dict | None = None) -> dict:
    return {
        "name":       name,
        "type":       "count",
        "value":      float(value) if value is not None else 0.0,
        "interval.ms": 3_600_000,   # 1-hour window matches cron cadence
        "timestamp":  _now_ms(),
        "attributes": attributes or {},
    }
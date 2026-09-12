"""
monitor_health.py — Pipeline health and freshness report.

Usage:
    python monitor_health.py            # human-readable table
    python monitor_health.py --json     # machine-readable, for dashboards

Fixes in this revision:
  * It used sqlite3 and a hardcoded 'env_data.db' directly, so it reported
    nothing when the pipeline ran against Postgres or a different SQLITE_DB_PATH.
    It goes through db.get_db_connection() now.
  * The source list was missing the satellite and gold-layer jobs, so two of
    six scheduled jobs could fail indefinitely without appearing in the report.
  * Its exit code is now non-zero when any source is stale or has no
    successful run, so it can be used as a container healthcheck or in CI.
"""

import sys
import json
import logging
from datetime import datetime, timezone, timedelta

from db import get_db_connection, execute_query

logger = logging.getLogger('monitor_health')

# Expected cadence per source, and the age at which we call it stale.
# The thresholds mirror scheduler.JOBS at roughly 2x the interval.
SOURCES = {
    'waqi':    {'name': 'WAQI Air Quality',   'interval_min': 30,  'stale_after_min': 75},
    'firms':   {'name': 'FIRMS Active Fires', 'interval_min': 20,  'stale_after_min': 50},
    'weather': {'name': 'Open-Meteo Weather', 'interval_min': 60,  'stale_after_min': 150},
    'gfs':     {'name': 'GFS Forecast Grid',  'interval_min': 360, 'stale_after_min': 800},
    'cams':    {'name': 'CAMS Composition',   'interval_min': 1440, 'stale_after_min': 3000},
    'gold':    {'name': 'Medallion Gold Layer', 'interval_min': 60, 'stale_after_min': 150},
}


def parse_iso(ts_str):
    """Parses an ISO timestamp, assuming UTC when no offset is present."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_str))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except ValueError:
        return None


def _value(row, key, index):
    """Reads a column from either a sqlite3.Row or a plain psycopg2 tuple."""
    try:
        return row[key]
    except (TypeError, IndexError, KeyError):
        return row[index]


def collect_health(now_utc=None):
    """Gathers per-source health. Returns a list of dicts."""
    now_utc = now_utc or datetime.now(timezone.utc)
    cutoff_24h = (now_utc - timedelta(hours=24)).isoformat()

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        report = []

        for source_key, meta in SOURCES.items():
            execute_query(cur, """
                SELECT
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END),
                    AVG(CASE WHEN status IN ('success', 'partial') THEN rows_inserted END)
                FROM pipeline_run_log
                WHERE source = %s AND run_started_at >= %s
            """, (source_key, cutoff_24h))
            row = cur.fetchone() or (0, 0, 0, None)
            successes, partials, failures = (row[0] or 0), (row[1] or 0), (row[2] or 0)
            avg_rows = round(row[3], 1) if row[3] is not None else 0.0

            execute_query(cur, """
                SELECT run_finished_at FROM pipeline_run_log
                WHERE source = %s AND status IN ('success', 'partial')
                ORDER BY id DESC LIMIT 1
            """, (source_key,))
            last = cur.fetchone()
            last_success = _value(last, 'run_finished_at', 0) if last else None

            execute_query(cur, """
                SELECT run_finished_at, status, error_message FROM pipeline_run_log
                WHERE source = %s AND status IN ('failure', 'partial')
                ORDER BY id DESC LIMIT 1
            """, (source_key,))
            err_row = cur.fetchone()

            last_dt = parse_iso(last_success)
            if last_dt is None:
                state, age_min = 'NO SUCCESSFUL RUNS', None
            else:
                age_min = int((now_utc - last_dt).total_seconds() / 60)
                state = 'STALE' if age_min > meta['stale_after_min'] else 'HEALTHY'

            report.append({
                'source': source_key,
                'name': meta['name'],
                'state': state,
                'age_minutes': age_min,
                'stale_after_minutes': meta['stale_after_min'],
                'last_success': last_success,
                'successes_24h': successes,
                'partials_24h': partials,
                'failures_24h': failures,
                'avg_rows': avg_rows,
                'last_error': {
                    'at': _value(err_row, 'run_finished_at', 0),
                    'status': _value(err_row, 'status', 1),
                    'message': _value(err_row, 'error_message', 2) or 'unknown',
                } if err_row else None,
            })
        return report
    finally:
        conn.close()


def print_report(report, now_utc):
    width = 96
    print("=" * width)
    print(f" PIPELINE HEALTH REPORT — {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * width)
    print(f"{'Source':<24} | {'Last OK (UTC)':<20} | {'24h S/P/F':<12} | {'Avg rows':<9} | Status")
    print("-" * width)

    for entry in report:
        last = (entry['last_success'] or 'Never')[:19].replace('T', ' ')
        counts = f"{entry['successes_24h']}S / {entry['partials_24h']}P / {entry['failures_24h']}F"
        if entry['state'] == 'HEALTHY':
            status = f"[HEALTHY] {entry['age_minutes']}m ago"
        elif entry['state'] == 'STALE':
            status = (f"[STALE] {entry['age_minutes']}m ago "
                      f"(> {entry['stale_after_minutes']}m)")
        else:
            status = "[NO SUCCESSFUL RUNS]"
        print(f"{entry['name']:<24} | {last:<20} | {counts:<12} | {entry['avg_rows']:<9} | {status}")

    print("=" * width)

    errors = [e for e in report if e['last_error']]
    if errors:
        print("\nMOST RECENT FAILURE OR DEGRADED RUN PER SOURCE:")
        print("-" * width)
        for entry in errors:
            err = entry['last_error']
            when = (err['at'] or '')[:19].replace('T', ' ')
            print(f"* [{entry['name']}] {when} [{str(err['status']).upper()}]: {err['message']}")
        print("=" * width)
    else:
        print("\nNo pipeline errors recorded.")


def main():
    now_utc = datetime.now(timezone.utc)
    report = collect_health(now_utc)

    if '--json' in sys.argv:
        print(json.dumps({'generated_at': now_utc.isoformat(), 'sources': report}, indent=2))
    else:
        print_report(report, now_utc)

    # Non-zero exit when anything is unhealthy, so this works as a
    # container healthcheck or a CI gate.
    unhealthy = [e for e in report if e['state'] != 'HEALTHY']
    return 1 if unhealthy else 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    sys.exit(main())

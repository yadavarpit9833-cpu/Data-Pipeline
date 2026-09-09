"""
monitor_health.py — Real-Time Data Pipeline Health & Stability Reporter
Usage: python monitor_health.py
"""
import sqlite3
from datetime import datetime, timezone, timedelta

DB_PATH = 'env_data.db'

# Expected run intervals (minutes) and 2x freshness thresholds (minutes)
SOURCES = {
    'cpcb':  {'name': 'CPCB Air Quality', 'interval': 15,  'threshold': 30},
    'firms': {'name': 'FIRMS Active Fires', 'interval': 20,  'threshold': 40},
    'weather': {'name': 'Open-Meteo Weather', 'interval': 60,  'threshold': 120},
    'gfs':   {'name': 'GFS Grid Forecast', 'interval': 360, 'threshold': 720},
}

def parse_iso(ts_str):
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now_utc = datetime.now(timezone.utc)
    cutoff_24h = (now_utc - timedelta(hours=24)).isoformat()

    print("=" * 85)
    print(f" PIPELINE HEALTH & STABILITY SNAPSHOT REPORT — {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 85)

    health_summary = []
    error_logs = []

    for src_key, meta in SOURCES.items():
        # 1. Total successes, partials, failures in last 24h
        cur.execute("""
            SELECT 
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
                SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partials,
                SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) as failures,
                AVG(CASE WHEN status = 'success' THEN rows_inserted ELSE NULL END) as avg_rows
            FROM pipeline_run_log
            WHERE source = ? AND run_started_at >= ?
        """, (src_key, cutoff_24h))
        
        row = cur.fetchone()
        successes = row['successes'] or 0
        partials = row['partials'] or 0
        failures = row['failures'] or 0
        avg_rows = round(row['avg_rows'], 1) if row['avg_rows'] is not None else 0.0

        # 2. Last successful run
        cur.execute("""
            SELECT run_finished_at, rows_inserted
            FROM pipeline_run_log
            WHERE source = ? AND status = 'success'
            ORDER BY id DESC LIMIT 1
        """, (src_key,))
        last_succ_row = cur.fetchone()
        last_success_ts = last_succ_row['run_finished_at'] if last_succ_row else "Never"

        # 3. Last failure or partial run
        cur.execute("""
            SELECT run_finished_at, status, error_message
            FROM pipeline_run_log
            WHERE source = ? AND status IN ('failure', 'partial')
            ORDER BY id DESC LIMIT 1
        """, (src_key,))
        last_err_row = cur.fetchone()

        if last_err_row:
            error_logs.append({
                'source': meta['name'],
                'timestamp': last_err_row['run_finished_at'],
                'status': last_err_row['status'],
                'message': last_err_row['error_message'] or 'Unknown error'
            })

        # 4. Freshness check (2x expected interval)
        last_succ_dt = parse_iso(last_success_ts)
        if last_succ_dt:
            mins_ago = int((now_utc - last_succ_dt).total_seconds() / 60)
            if mins_ago > meta['threshold']:
                freshness_status = f"[STALE] ({mins_ago}m ago > {meta['threshold']}m max)"
            else:
                freshness_status = f"[HEALTHY] ({mins_ago}m ago)"
        else:
            freshness_status = "[NO SUCCESSFUL RUNS]"

        health_summary.append({
            'source': meta['name'],
            'last_success': last_success_ts[:19].replace('T', ' ') if last_success_ts != "Never" else "Never",
            '24h_s_p_f': f"{successes}S / {partials}P / {failures}F",
            'avg_rows': avg_rows,
            'status': freshness_status
        })

    # Print Health Summary Table
    print(f"{'Source':<22} | {'Last Success (UTC)':<19} | {'24h Runs (S/P/F)':<15} | {'Avg Rows':<10} | {'Freshness / Status'}")
    print("-" * 85)
    for h in health_summary:
        print(f"{h['source']:<22} | {h['last_success']:<19} | {h['24h_s_p_f']:<15} | {h['avg_rows']:<10} | {h['status']}")
    print("=" * 85)

    # Print Error / Warning Details
    if error_logs:
        print("\nRECENT ERRORS & DEGRADED RUN LOGS:")
        print("-" * 85)
        for err in error_logs:
            ts = err['timestamp'][:19].replace('T', ' ') if err['timestamp'] else ''
            print(f"* [{err['source']}] {ts} [{err['status'].upper()}]: {err['message']}")
        print("=" * 85)
    else:
        print("\nNo pipeline errors recorded.")

    conn.close()

if __name__ == '__main__':
    main()

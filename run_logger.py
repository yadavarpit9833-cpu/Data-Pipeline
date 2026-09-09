"""
run_logger.py
-------------
Shared helper to write a structured row to pipeline_run_log for any execution
context: scheduler, standalone direct run, or test.

Usage in any fetcher's main():
    from run_logger import log_run
    result = actual_fetch_function()
    log_run('cpcb', started_at, result)
"""

import logging
from datetime import datetime, timezone
from db import get_db_connection, execute_query

logger = logging.getLogger('run_logger')

def log_run(source: str, run_started_at: str, result):
    """
    Writes execution outcome to pipeline_run_log regardless of how the
    fetcher was launched (scheduler, standalone, or test harness).

    Args:
        source: Source key e.g. 'cpcb', 'gfs', 'weather', 'firms'
        run_started_at: ISO timestamp of when the run began
        result: Either a (status, rows_inserted, error_message) 3-tuple
                returned by main(), or None/int for simpler cases.
    """
    run_finished_at = datetime.now(timezone.utc).isoformat()

    if isinstance(result, tuple) and len(result) >= 3:
        status, rows_inserted, error_msg = result[0], result[1], result[2]
    elif isinstance(result, int):
        status, rows_inserted, error_msg = 'success', result, None
    elif result is None:
        status, rows_inserted, error_msg = 'failure', 0, 'main() returned None'
    else:
        status, rows_inserted, error_msg = 'success', 0, None

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            """
            INSERT INTO pipeline_run_log (
                source, run_started_at, run_finished_at, status, rows_inserted, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (source, run_started_at, run_finished_at, status, int(rows_inserted or 0), error_msg)
        )
        conn.commit()
        logger.info(f"[run_logger] Logged run: source={source}, status={status}, rows={rows_inserted}")
    except Exception as e:
        logger.error(f"[run_logger] Failed to write pipeline_run_log for {source}: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

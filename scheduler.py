import os
import time
import logging
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from db import get_db_connection, execute_query, init_db

# Import main functions from fetchers
from fetch_cpcb import main as fetch_cpcb
from fetch_weather import main as fetch_weather
from fetch_gfs import main as fetch_gfs
from fetch_firms import main as fetch_firms
from fetch_sentinel5p import main as fetch_sentinel5p
from gold_layer import build_all_gold

# Setup logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scheduler.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8-sig'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger('scheduler')

# Global set to track active running jobs (prevents overlapping execution for long-running jobs)
active_jobs = set()

def log_pipeline_run(source, run_started_at, run_finished_at, status, rows_inserted, error_message):
    """Inserts a structured execution log row into pipeline_run_log table"""
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
            (source, run_started_at, run_finished_at, status, int(rows_inserted), error_message)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to log pipeline run for {source}: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

def run_job(source_key, job_name, job_func):
    """
    Resilient wrapper for scheduled jobs:
    1. Prevents overlapping executions if a previous trigger is still running.
    2. Measures exact execution start/end timestamps.
    3. Catches all exceptions so scheduler process never crashes.
    4. Records structured run status ('success', 'partial', 'failure') into pipeline_run_log.
    """
    if source_key in active_jobs:
        logger.warning(f"--- Job {job_name} ({source_key}) is already running! Skipping this scheduled trigger. ---")
        return
        
    active_jobs.add(source_key)
    logger.info(f"--- Starting Scheduled Job: {job_name} [{source_key}] ---")
    run_started_at = datetime.now(timezone.utc).isoformat()
    
    try:
        result = job_func()
        run_finished_at = datetime.now(timezone.utc).isoformat()
        
        if isinstance(result, tuple) and len(result) >= 3:
            status, rows_inserted, error_msg = result[0], result[1], result[2]
        else:
            status, rows_inserted, error_msg = 'success', (result if isinstance(result, int) else 0), None
            
        logger.info(f"--- Finished Scheduled Job: {job_name} | Status: {status} | Rows: {rows_inserted} ---")
        log_pipeline_run(source_key, run_started_at, run_finished_at, status, rows_inserted, error_msg)
        
    except Exception as e:
        run_finished_at = datetime.now(timezone.utc).isoformat()
        err_msg = str(e)
        logger.error(f"--- Job {job_name} failed with unhandled exception: {err_msg} ---")
        log_pipeline_run(source_key, run_started_at, run_finished_at, 'failure', 0, err_msg)
        
    finally:
        active_jobs.remove(source_key)

if __name__ == '__main__':
    # Initialize database schema on startup
    init_db()
    
    scheduler = BlockingScheduler()
    now_utc = datetime.now(timezone.utc)

    # CPCB: every 15 min (triggers immediately on startup, then every 15 min)
    scheduler.add_job(
        run_job,
        'interval',
        minutes=15,
        args=['cpcb', 'CPCB Air Quality', fetch_cpcb],
        id='cpcb_job',
        next_run_time=now_utc
    )

    # IMD: every 60 min (best effort, triggers immediately on startup, then every 60 min)
    scheduler.add_job(
        run_job,
        'interval',
        minutes=60,
        args=['weather', 'Open-Meteo Weather', fetch_weather],
        id='weather_job',
        next_run_time=now_utc
    )

    # FIRMS: every 20 min (triggers immediately on startup, then every 20 min)
    scheduler.add_job(
        run_job,
        'interval',
        minutes=20,
        args=['firms', 'FIRMS Active Fires', fetch_firms],
        id='firms_job',
        next_run_time=now_utc
    )

    # GFS: every 6 hours (00, 06, 12, 18Z cycles, triggers immediately on startup, then on schedule)
    scheduler.add_job(
        run_job,
        CronTrigger(hour='0,6,12,18', minute=30, timezone='UTC'),
        args=['gfs', 'GFS Grid Weather', fetch_gfs],
        id='gfs_job',
        next_run_time=now_utc
    )

    # Sentinel-5P TROPOMI: once daily at 12:00 UTC (CAMS daily composite available by then)
    scheduler.add_job(
        run_job,
        CronTrigger(hour=12, minute=0, timezone='UTC'),
        args=['sentinel5p', 'Sentinel-5P TROPOMI Satellite', fetch_sentinel5p],
        id='sentinel5p_job',
        next_run_time=now_utc
    )

    # Gold layer: rebuild every 60 min so analysis-ready features stay fresh
    def _gold_job():
        n = build_all_gold()
        return ('success', n, None)
    scheduler.add_job(
        run_job,
        'interval',
        minutes=60,
        args=['gold', 'Medallion Gold Layer', _gold_job],
        id='gold_job',
        next_run_time=now_utc
    )

    logger.info("Scheduler started successfully for environmental pipeline. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

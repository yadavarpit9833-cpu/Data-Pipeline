"""
scheduler.py — Orchestrates the fetchers.

Changes in this revision:

  * Jobs no longer all start at the same instant. Six jobs with
    next_run_time=now fired simultaneously, each opening its own SQLite
    connection, which is how the "database is locked" errors documented in
    scripts/setup_background_task.ps1 happened. Starts are staggered and
    db.py now uses WAL with a busy timeout.
  * The hand-rolled `active_jobs` set is gone. APScheduler's max_instances=1
    already guarantees no overlapping execution of the same job and does it
    without a set mutated from several worker threads.
  * The log file rotates. It was a plain FileHandler, so scheduler.log grew
    without bound for as long as the pipeline ran.
  * log_pipeline_run duplicated run_logger.log_run almost exactly. There is
    one implementation now, in run_logger.
  * WAQI moved from every 15 minutes to every 30. WAQI's city feeds update
    roughly hourly, so three of every four requests returned a payload we
    already had and were dropped by the unique constraint.
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from db import init_db
from run_logger import log_run

from fetch_waqi import main as fetch_waqi
from fetch_weather import main as fetch_weather
from fetch_gfs import main as fetch_gfs
from fetch_firms import main as fetch_firms
from fetch_cams import main as fetch_cams
from gold_layer import build_all_gold

# Windows consoles default to cp1252/cp437 and cannot encode the degree signs
# and dashes in these log messages. logging swallows the UnicodeEncodeError and
# drops the line, so runs looked silently truncated.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scheduler.log')
LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES,
                            backupCount=LOG_BACKUP_COUNT, encoding='utf-8'),
        logging.StreamHandler(),
    ],
    force=True,
)
logger = logging.getLogger('scheduler')


def run_job(source_key, job_name, job_func):
    """
    Runs one fetcher, recording the outcome in pipeline_run_log.

    Every exception is caught so a failing source can never take the scheduler
    process down with it and stop the other four.
    """
    logger.info(f"--- Starting job: {job_name} [{source_key}] ---")
    run_started_at = datetime.now(timezone.utc).isoformat()

    try:
        result = job_func()
    except Exception as e:
        logger.exception(f"--- Job {job_name} raised an unhandled exception ---")
        log_run(source_key, run_started_at, ('failure', 0, str(e)))
        return

    if isinstance(result, tuple) and len(result) >= 3:
        status, rows, error = result[0], result[1], result[2]
    else:
        status, rows, error = 'success', (result if isinstance(result, int) else 0), None

    level = logging.INFO if status == 'success' else logging.WARNING
    logger.log(level, f"--- Finished job: {job_name} | status={status} | rows={rows}"
                      + (f" | {error}" if error else "") + " ---")
    log_run(source_key, run_started_at, result)


def gold_job():
    """Wraps the gold-layer rebuild in the (status, rows, error) contract."""
    return ('success', build_all_gold(), None)


# (source_key, display name, callable, trigger, stagger seconds)
#
# The stagger keeps six jobs from opening six SQLite write transactions in the
# same second at start-up.
JOBS = [
    ('waqi',    'WAQI Air Quality',   fetch_waqi,    IntervalTrigger(minutes=30),  0),
    ('firms',   'FIRMS Active Fires', fetch_firms,   IntervalTrigger(minutes=20),  20),
    ('weather', 'Open-Meteo Weather', fetch_weather, IntervalTrigger(minutes=60),  40),
    # GFS cycles land 3.5-5 hours after their nominal time, so these run well
    # after 00/06/12/18Z rather than on the hour. fetch_gfs picks the cycle.
    ('gfs',     'GFS Forecast Grid',  fetch_gfs,
     CronTrigger(hour='5,11,17,23', minute=15, timezone='UTC'), 60),
    ('cams',    'CAMS Composition',   fetch_cams,
     CronTrigger(hour=12, minute=30, timezone='UTC'), 90),
    ('gold',    'Medallion Gold Layer', gold_job,    IntervalTrigger(minutes=60), 120),
]


def build_scheduler(start_immediately=True):
    """Creates the configured scheduler. Separated out so tests can inspect it."""
    scheduler = BlockingScheduler(timezone='UTC')
    now = datetime.now(timezone.utc)

    for source_key, job_name, job_func, trigger, stagger_s in JOBS:
        scheduler.add_job(
            run_job,
            trigger,
            args=[source_key, job_name, job_func],
            id=f'{source_key}_job',
            name=job_name,
            # One instance at a time: a slow GFS run is skipped rather than
            # overlapped with the next trigger.
            max_instances=1,
            # If the process was asleep, run once on wake instead of firing
            # every missed trigger in a burst.
            coalesce=True,
            misfire_grace_time=300,
            next_run_time=(now + timedelta(seconds=stagger_s)) if start_immediately else None,
        )
    return scheduler


if __name__ == '__main__':
    init_db()
    scheduler = build_scheduler()
    logger.info(f"Scheduler started with {len(JOBS)} jobs. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

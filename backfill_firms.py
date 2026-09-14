"""
backfill_firms.py — Historical NASA FIRMS backfill.

The live fetcher (`fetch_firms.py`) only reaches NRT data, which spans roughly the
last two months. This pulls the Standard Processing (SP) archives instead, which
go back to 2000 (MODIS) and 2012 (VIIRS SNPP).

Defaults to January, October, November and December of 2020-2025 over the India
bounding box — the months that matter for stubble-burning and winter pollution.

Usage:
    python backfill_firms.py                      # the default window
    python backfill_firms.py --years 2020 2021    # specific years
    python backfill_firms.py --months 10 11       # specific months
    python backfill_firms.py --dry-run            # plan only, no requests

Safe to re-run: every insert is idempotent on the same keys the live fetcher uses,
and Parquet partitions are read-merge-written rather than overwritten.
"""

import os
import sys
import time
import json
import logging
import argparse
import calendar
import hashlib
from io import StringIO
from datetime import date, datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv

from db import get_db_connection, execute_query, init_db, DB_ENGINE
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill_firms.log')
_handlers = [logging.FileHandler(LOG_PATH, encoding='utf-8')]
_console = sys.stderr if sys.stderr is not None else sys.stdout
if _console is not None:
    _handlers.append(logging.StreamHandler(_console))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=_handlers, force=True)
logger = logging.getLogger('backfill_firms')

load_dotenv()
FIRMS_MAP_KEY = os.getenv('FIRMS_MAP_KEY')

AREA = '68,6,97,37'            # India bounding box, same as the live fetcher
MAX_DAY_RANGE = 5              # FIRMS area API hard limit: "Expects [1..5]"
BASE = 'https://firms.modaps.eosdis.nasa.gov/api'

# SP = Standard Processing, the reprocessed archive. The NRT equivalents used by
# fetch_firms.py only cover the last ~2 months and cannot serve historical dates.
SOURCES = ['VIIRS_SNPP_SP', 'MODIS_SP']

DEFAULT_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
DEFAULT_MONTHS = [1, 10, 11, 12]


def compute_payload_hash(data):
    b = data.encode('utf-8') if isinstance(data, str) else data
    return hashlib.sha256(b).hexdigest()


def format_firms_timestamp(row, fallback_iso):
    """FIRMS acq_date + acq_time (HHMM, unpadded) -> ISO-8601 UTC."""
    date_str = str(row.get('acq_date', '')).strip()
    time_str = str(row.get('acq_time', '')).strip().zfill(4)
    if len(date_str) == 10 and len(time_str) == 4:
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H%M")
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    return fallback_iso


def month_chunks(year, month):
    """
    Split a month into (start_date, day_range) pairs no longer than MAX_DAY_RANGE,
    covering every day including the tail (e.g. Jan 31 gets its own 1-day chunk).
    """
    last = calendar.monthrange(year, month)[1]
    out, day = [], 1
    while day <= last:
        span = min(MAX_DAY_RANGE, last - day + 1)
        out.append((date(year, month, day).isoformat(), span))
        day += span
    return out


def get_availability():
    """Ask FIRMS which sources it can serve and over what dates."""
    url = f"{BASE}/data_availability/csv/{FIRMS_MAP_KEY}/all"
    df = pd.read_csv(StringIO(requests.get(url, timeout=60).text))
    return {r['data_id']: (r['min_date'], r['max_date']) for _, r in df.iterrows()}


def transaction_status():
    url = f"https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY={FIRMS_MAP_KEY}"
    try:
        return requests.get(url, timeout=30).json()
    except Exception:
        return {}


def fetch_chunk(source, start_date, day_range, max_retries=4):
    """One area-API call. Returns CSV text, or None when the response isn't CSV."""
    url = f"{BASE}/area/csv/{FIRMS_MAP_KEY}/{source}/{AREA}/{day_range}/{start_date}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                body = r.text
                if 'latitude' in body[:200].lower():
                    return body
                logger.warning(f"  non-CSV for {source} {start_date}: {body[:120].strip()}")
                return None
            # 429 means the 10-minute transaction budget is spent; wait it out.
            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"  rate limited, sleeping {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"  HTTP {r.status_code} for {source} {start_date}")
        except requests.RequestException as e:
            logger.warning(f"  attempt {attempt+1} failed for {source} {start_date}: {e}")
        time.sleep(2 ** attempt)
    return None


def normalise(df):
    """FIRMS CSV -> the column names the cleaned_firms schema expects."""
    # Only MODIS emits 'brightness'; VIIRS emits the I-4 channel as 'bright_ti4'.
    # Fold them together or every VIIRS row lands with a null brightness.
    if 'bright_ti4' in df.columns:
        if 'brightness' not in df.columns:
            df['brightness'] = df['bright_ti4']
        else:
            df['brightness'] = df['brightness'].fillna(df['bright_ti4'])
    df = df.rename(columns={'latitude': 'lat', 'longitude': 'lon',
                            'brightness': 'brightness_raw',
                            'confidence': 'confidence_raw'}, errors='ignore')
    return df


def write_month(df, year, month):
    """QC, contract-check and persist one month to SQLite + Parquet."""
    if df.empty:
        return 0, 0

    df = clean_and_impute(df, 'brightness_raw', 'timestamp', lat_col='lat', lon_col='lon',
                          min_val=200.0, max_val=600.0, max_step_change=150.0)
    df = df.rename(columns={'brightness_raw_clean': 'brightness_clean',
                            'brightness_raw_imputed': 'brightness_imputed',
                            'brightness_raw_qc_flag': 'brightness_qc_flag'}, errors='ignore')
    if 'confidence_raw' in df.columns:
        df['confidence_clean'] = df['confidence_raw']
        df['confidence_imputed'] = 0
    df['source'] = 'firms'
    df['is_synthetic'] = 0

    df, failures = validate(df, 'firms')
    n_fail = len(failures)
    if df.empty:
        logger.warning(f"  {year}-{month:02d}: every row failed contract validation")
        return 0, n_fail

    # executemany rather than the live fetcher's per-row loop: a month of VIIRS
    # runs to tens of thousands of rows.
    rows = [(
        r.get('lat'), r.get('lon'), r.get('timestamp'),
        r.get('brightness_raw'), r.get('brightness_clean'),
        1 if r.get('brightness_imputed', False) else 0,
        r.get('brightness_qc_flag', 'ok'),
        str(r.get('confidence_raw')), str(r.get('confidence_clean')), 0,
        str(r.get('satellite')), 'firms', 0,
    ) for r in df.to_dict('records')]

    sql = """INSERT OR IGNORE INTO cleaned_firms (
                 lat, lon, timestamp,
                 brightness_raw, brightness_clean, brightness_imputed, brightness_qc_flag,
                 confidence_raw, confidence_clean, confidence_imputed,
                 satellite, source, is_synthetic
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    if DB_ENGINE == 'postgres':
        sql = sql.replace('INSERT OR IGNORE INTO', 'INSERT INTO') + ' ON CONFLICT DO NOTHING'
        sql = sql.replace('?', '%s')

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()

    # Partition Parquet by the OBSERVATION date, not the fetch date, so historical
    # rows land in the partition they belong to.
    df['obs_date'] = df['timestamp'].astype(str).str.slice(0, 10)
    for d, part in df.groupby('obs_date'):
        save_cleaned_data_parquet(part.drop(columns='obs_date'), source='firms',
                                  partition_key='date', partition_value=d,
                                  dedup_keys=['lat', 'lon', 'timestamp', 'satellite'],
                                  pure_overwrite=False)
    return len(df), n_fail


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--years', nargs='+', type=int, default=DEFAULT_YEARS)
    ap.add_argument('--months', nargs='+', type=int, default=DEFAULT_MONTHS)
    ap.add_argument('--sources', nargs='+', default=SOURCES)
    ap.add_argument('--sleep', type=float, default=0.4, help='seconds between requests')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if not FIRMS_MAP_KEY or FIRMS_MAP_KEY == 'your_firms_map_key_here':
        logger.error("FIRMS_MAP_KEY missing or placeholder in .env")
        return 1

    init_db()
    periods = [(y, m) for y in sorted(args.years) for m in sorted(args.months)]
    avail = get_availability()

    # Drop any (source, month) the archive cannot actually serve, up front,
    # rather than discovering it mid-run.
    plan = []
    for src in args.sources:
        if src not in avail:
            logger.warning(f"{src}: not offered by FIRMS, skipping")
            continue
        lo, hi = avail[src]
        for (y, m) in periods:
            last = calendar.monthrange(y, m)[1]
            if date(y, m, last).isoformat() < lo or date(y, m, 1).isoformat() > hi:
                logger.warning(f"{src}: {y}-{m:02d} outside archive window {lo}..{hi}, skipping")
                continue
            plan.append((src, y, m))

    n_req = sum(len(month_chunks(y, m)) for _, y, m in plan)
    logger.info(f"Plan: {len(plan)} source-months, {n_req} requests, sources={args.sources}")
    logger.info(f"Transaction budget: {transaction_status()}")
    if args.dry_run:
        for src, y, m in plan:
            logger.info(f"  would fetch {src} {y}-{m:02d} ({len(month_chunks(y, m))} chunks)")
        return 0

    grand_rows = grand_fail = 0
    by_month = {}
    for (y, m) in periods:
        srcs = [s for s, yy, mm in plan if (yy, mm) == (y, m)]
        if not srcs:
            continue
        frames = []
        fallback = datetime(y, m, 1, tzinfo=timezone.utc).isoformat()
        for src in srcs:
            got = 0
            for start, span in month_chunks(y, m):
                csv_text = fetch_chunk(src, start, span)
                time.sleep(args.sleep)
                if not csv_text:
                    continue

                # Raw layer: keep the payload verbatim, same as the live fetcher.
                h = compute_payload_hash(csv_text)
                conn = get_db_connection()
                try:
                    cur = conn.cursor()
                    execute_query(cur,
                        "INSERT OR IGNORE INTO raw_firms (timestamp, raw_data, raw_data_hash, source, is_synthetic) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (f"{start}T00:00:00+00:00", csv_text, h, f'firms_backfill_{src}', 0))
                    conn.commit()
                finally:
                    conn.close()
                save_raw_data('firms', start, csv_text, ext='csv')

                chunk = pd.read_csv(StringIO(csv_text))
                if chunk.empty:
                    continue
                chunk['timestamp'] = chunk.apply(lambda r: format_firms_timestamp(r, fallback), axis=1)
                frames.append(normalise(chunk))
                got += len(chunk)
            logger.info(f"  {y}-{m:02d} {src}: {got} raw detections")

        if not frames:
            logger.warning(f"{y}-{m:02d}: nothing retrieved")
            continue

        month_df = pd.concat(frames, ignore_index=True)
        saved, n_fail = write_month(month_df, y, m)
        by_month[f"{y}-{m:02d}"] = saved
        grand_rows += saved
        grand_fail += n_fail
        logger.info(f"{y}-{m:02d}: saved {saved} rows "
                    f"({n_fail} contract failures) | running total {grand_rows}")

    logger.info("=" * 60)
    logger.info(f"Backfill complete: {grand_rows} rows, {grand_fail} contract failures")
    for k in sorted(by_month):
        logger.info(f"  {k}: {by_month[k]}")
    return 0


if __name__ == '__main__':
    sys.exit(main())

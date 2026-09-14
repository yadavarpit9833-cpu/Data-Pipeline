"""
backfill_weather.py — Historical weather backfill from the Open-Meteo archive.

fetch_weather.py only reads the live `current` endpoint, so the pipeline holds
one observation per station per run and nothing before the day it started. This
loads the reanalysis archive instead, which reaches back to 1940 and needs no
API key.

Defaults to the same window as backfill_firms.py — January, October, November
and December of 2020-2025, across the same 11 stations the live fetcher polls —
so fire and weather line up hour for hour and can actually be correlated.

Usage:
    python backfill_weather.py                     # the default window
    python backfill_weather.py --years 2023 2024
    python backfill_weather.py --months 10 11
    python backfill_weather.py --dry-run

Safe to re-run: inserts are idempotent on (station, timestamp), the same key the
live fetcher uses, and Parquet partitions are read-merge-written.
"""

import os
import sys
import time
import json
import hashlib
import logging
import argparse
import calendar
from datetime import date, datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv

from db import get_db_connection, execute_query, init_db, DB_ENGINE
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate
from fetch_weather import INDIA_STATIONS

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill_weather.log')
_handlers = [logging.FileHandler(LOG_PATH, encoding='utf-8')]
_console = sys.stderr if sys.stderr is not None else sys.stdout
if _console is not None:
    _handlers.append(logging.StreamHandler(_console))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=_handlers, force=True)
logger = logging.getLogger('backfill_weather')

load_dotenv()

ARCHIVE = 'https://archive-api.open-meteo.com/v1/archive'
HOURLY = 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m'

# Open-Meteo returns km/h unless told otherwise, while fetch_weather.py asks for
# m/s. Matching it matters: a silent unit mismatch between the live rows and the
# backfilled ones would quietly corrupt any analysis that spans both.
WIND_UNIT = 'ms'

# Same variable -> column mapping and the same QC thresholds the live fetcher uses.
VAR_MAP = {
    'temperature_2m':       'temperature_raw',
    'relative_humidity_2m': 'humidity_raw',
    'precipitation':        'rainfall_raw',
    'wind_speed_10m':       'wind_speed_raw',
    'wind_direction_10m':   'wind_dir_raw',
}
QC_PARAMS = {
    'temperature_raw': dict(min_val=-50.0, max_val=60.0,  max_step_change=15.0,  is_circular=False, ignore_zero_flatline=False),
    'humidity_raw':    dict(min_val=0.0,   max_val=100.0, max_step_change=50.0,  is_circular=False, ignore_zero_flatline=False),
    'rainfall_raw':    dict(min_val=0.0,   max_val=500.0, max_step_change=100.0, is_circular=False, ignore_zero_flatline=True),
    'wind_speed_raw':  dict(min_val=0.0,   max_val=100.0, max_step_change=30.0,  is_circular=False, ignore_zero_flatline=False),
    'wind_dir_raw':    dict(min_val=0.0,   max_val=360.0, max_step_change=180.0, is_circular=True,  ignore_zero_flatline=False),
}

DEFAULT_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
DEFAULT_MONTHS = [1, 10, 11, 12]


def payload_hash(data):
    b = data.encode('utf-8') if isinstance(data, str) else data
    return hashlib.sha256(b).hexdigest()


def contiguous_spans(year, months):
    """
    Collapse the requested months into contiguous date spans, so Oct-Nov-Dec is
    one request rather than three. The archive endpoint takes a date range, and
    there is no reason to split a run of months apart.
    """
    ms = sorted(set(months))
    spans, run = [], [ms[0]]
    for m in ms[1:]:
        if m == run[-1] + 1:
            run.append(m)
        else:
            spans.append(run)
            run = [m]
    spans.append(run)
    return [(date(year, r[0], 1).isoformat(),
             date(year, r[-1], calendar.monthrange(year, r[-1])[1]).isoformat())
            for r in spans]


def fetch_span(lat, lon, start, end, max_retries=4):
    params = {'latitude': lat, 'longitude': lon, 'start_date': start, 'end_date': end,
              'hourly': HOURLY, 'wind_speed_unit': WIND_UNIT, 'timezone': 'UTC'}
    for attempt in range(max_retries):
        try:
            r = requests.get(ARCHIVE, params=params, timeout=180)
            if r.status_code == 200:
                return r.text
            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"  rate limited, sleeping {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"  HTTP {r.status_code} for {start}..{end}: {r.text[:120]}")
        except requests.RequestException as e:
            logger.warning(f"  attempt {attempt+1} failed for {start}..{end}: {e}")
        time.sleep(2 ** attempt)
    return None


def to_frame(raw_text, station_name):
    """Open-Meteo's column-oriented hourly block -> one row per hour."""
    payload = json.loads(raw_text)
    h = payload.get('hourly') or {}
    times = h.get('time') or []
    if not times:
        return pd.DataFrame()

    df = pd.DataFrame({'timestamp': times})
    # The archive returns naive UTC strings ("2020-11-01T00:00"); make the zone
    # explicit so these sort and join against the live rows.
    df['timestamp'] = df['timestamp'].astype(str).str.replace(' ', 'T', regex=False)
    df['timestamp'] = df['timestamp'].apply(
        lambda s: s if s.endswith('+00:00') else (s if len(s) > 16 else s + ':00') + '+00:00')
    for src_col, dest in VAR_MAP.items():
        df[dest] = pd.to_numeric(pd.Series(h.get(src_col) or [None] * len(times)), errors='coerce')
    df['station'] = station_name
    return df


def write_chunk(df, label):
    """QC, contract-check and persist one station-year chunk."""
    if df.empty:
        return 0, 0

    for metric, opts in QC_PARAMS.items():
        if metric in df.columns:
            df = clean_and_impute(df, metric, 'timestamp', group_cols=['station'], **opts)

    df = df.rename(columns={f'{m}_clean': m.replace('_raw', '_clean') for m in QC_PARAMS},
                   errors='ignore')
    for m in QC_PARAMS:
        base = m.replace('_raw', '')
        df = df.rename(columns={f'{m}_imputed': f'{base}_imputed',
                                f'{m}_qc_flag': f'{base}_qc_flag'}, errors='ignore')

    df['source'] = 'open-meteo-archive'
    df['is_synthetic'] = 0

    df, failures = validate(df, 'weather')
    n_fail = len(failures)
    if df.empty:
        logger.warning(f"  {label}: every row failed contract validation")
        return 0, n_fail

    def col(r, name, dflt=None):
        v = r.get(name, dflt)
        return None if pd.isna(v) else v

    rows = []
    for r in df.to_dict('records'):
        vals = [r.get('station'), r.get('timestamp')]
        for m in ('temperature', 'humidity', 'rainfall', 'wind_speed', 'wind_dir'):
            vals += [col(r, f'{m}_raw'), col(r, f'{m}_clean'),
                     1 if r.get(f'{m}_imputed', False) else 0,
                     r.get(f'{m}_qc_flag', 'ok')]
        vals += ['open-meteo-archive', 0]
        rows.append(tuple(vals))

    sql = """INSERT OR IGNORE INTO cleaned_imd (
                 station, timestamp,
                 temperature_raw, temperature_clean, temperature_imputed, temperature_qc_flag,
                 humidity_raw, humidity_clean, humidity_imputed, humidity_qc_flag,
                 rainfall_raw, rainfall_clean, rainfall_imputed, rainfall_qc_flag,
                 wind_speed_raw, wind_speed_clean, wind_speed_imputed, wind_speed_qc_flag,
                 wind_dir_raw, wind_dir_clean, wind_dir_imputed, wind_dir_qc_flag,
                 source, is_synthetic
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
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

    # Partition by observation date, not fetch date.
    df['obs_date'] = df['timestamp'].astype(str).str.slice(0, 10)
    for d, part in df.groupby('obs_date'):
        save_cleaned_data_parquet(part.drop(columns='obs_date'), source='weather',
                                  partition_key='date', partition_value=d,
                                  dedup_keys=['station', 'timestamp'], pure_overwrite=False)
    return len(df), n_fail


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--years', nargs='+', type=int, default=DEFAULT_YEARS)
    ap.add_argument('--months', nargs='+', type=int, default=DEFAULT_MONTHS)
    ap.add_argument('--sleep', type=float, default=0.3)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    init_db()
    years = sorted(args.years)
    plan = [(y, s, e) for y in years for (s, e) in contiguous_spans(y, args.months)]
    n_req = len(plan) * len(INDIA_STATIONS)
    logger.info(f"Plan: {len(INDIA_STATIONS)} stations x {len(plan)} spans = {n_req} requests")
    for (y, s, e) in plan:
        logger.info(f"  span {s} .. {e}")
    if args.dry_run:
        return 0

    grand = fails = 0
    for (y, start, end) in plan:
        frames = []
        for wmo, (name, lat, lon) in INDIA_STATIONS.items():
            raw = fetch_span(lat, lon, start, end)
            time.sleep(args.sleep)
            if not raw:
                logger.warning(f"  {name} {start}..{end}: no data")
                continue

            h = payload_hash(raw)
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                execute_query(cur,
                    "INSERT OR IGNORE INTO raw_imd (timestamp, raw_data, raw_data_hash, source, is_synthetic) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (f"{start}T00:00:00+00:00", raw, h, f'open-meteo-archive:{name}', 0))
                conn.commit()
            finally:
                conn.close()
            save_raw_data('weather', start, raw, ext='json')

            f = to_frame(raw, name)
            if not f.empty:
                frames.append(f)

        if not frames:
            logger.warning(f"{start}..{end}: nothing retrieved")
            continue

        chunk = pd.concat(frames, ignore_index=True)
        label = f"{start}..{end}"
        saved, nf = write_chunk(chunk, label)
        grand += saved
        fails += nf
        logger.info(f"{label}: {len(chunk)} hours fetched, {saved} rows saved "
                    f"({nf} contract failures) | running total {grand}")

    logger.info("=" * 60)
    logger.info(f"Backfill complete: {grand} rows, {fails} contract failures")
    return 0


if __name__ == '__main__':
    sys.exit(main())

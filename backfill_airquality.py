"""
backfill_airquality.py — Historical city air quality from the CAMS archive.

READ THIS BEFORE USING THE DATA IT WRITES.

**This is not CPCB ground-station data.** CPCB observations are not available
historically through any free interface: the WAQI API that `fetch_cpcb.py` falls
back to serves the current observation only, with no historical endpoint, and
CPCB's own archive is not openly published. What this loads instead is the CAMS
global atmospheric composition reanalysis, served by Open-Meteo — the same source
`fetch_sentinel5p.py` already uses. It is a model product, not a measurement, and
it will not reproduce a specific monitoring station's readings.

Rows go to `cleaned_cams_aq`, deliberately NOT to `cleaned_cpcb`. WAQI's `iaqi`
values are AQI sub-indices on a 0-500 scale - its overall `aqi` equals
`iaqi[dominentpol]` exactly, which is how you can tell - while CAMS reports
physical concentrations. Sharing one column would leave it ambiguous without
inspecting `source` on every row, so the two live apart.

Units are CAMS's own and are not converted: ug/m3 throughout, CO included. CPCB
quotes CO in mg/m3 instead, so divide by 1000 before comparing the two.

**Coverage starts 2022-08-03.** Earlier dates return rows of nulls rather than an
error, so anything before that is silently empty. Of the Jan/Oct/Nov/Dec window
the fire and weather backfills cover, this can fill October 2022 onward — fifteen
of the twenty-four months. 2020, 2021 and January 2022 cannot be filled from here.

Usage:
    python backfill_airquality.py                      # everything available
    python backfill_airquality.py --years 2023 2024
    python backfill_airquality.py --cities delhi lucknow
    python backfill_airquality.py --dry-run

Safe to re-run: idempotent on (city, timestamp).
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

from db import get_db_connection, execute_query, init_db, DB_ENGINE
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backfill_airquality.log')
_handlers = [logging.FileHandler(LOG_PATH, encoding='utf-8')]
_console = sys.stderr if sys.stderr is not None else sys.stdout
if _console is not None:
    _handlers.append(logging.StreamHandler(_console))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=_handlers, force=True)
logger = logging.getLogger('backfill_airquality')

ARCHIVE = 'https://air-quality-api.open-meteo.com/v1/air-quality'
HOURLY = 'pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,ozone'

# Probed against the endpoint: 2022-08-02 returns all nulls, 2022-08-05 returns a
# full day. Anything earlier comes back as rows of nulls rather than an error, so
# the floor is enforced here instead of discovering empty data later.
COVERAGE_START = date(2022, 8, 3)

# The five cities fetch_cpcb.py polls, plus the stubble belt and the city
# immediately downwind of it, which is the point of having this data at all.
CITIES = {
    'delhi':     (28.61, 77.21),
    'mumbai':    (19.08, 72.88),
    'bengaluru': (12.97, 77.59),
    'chennai':   (13.08, 80.27),
    'kolkata':   (22.57, 88.36),
    'lucknow':   (26.85, 80.95),
    'amritsar':  (31.63, 74.87),
    'patiala':   (30.34, 76.38),
}

VAR_MAP = {
    'pm2_5':            'pm25_ugm3',
    'pm10':             'pm10_ugm3',
    'nitrogen_dioxide': 'no2_ugm3',
    'sulphur_dioxide':  'so2_ugm3',
    'carbon_monoxide':  'co_ugm3',
    'ozone':            'o3_ugm3',
}
# Bounds are for concentrations in ug/m3. cleaned_cpcb's QC numbers do not
# transfer: those are sized for the 0-500 AQI scale WAQI returns, and CO in
# particular differs by a factor of 1000 between the two unit systems.
QC_PARAMS = {
    'pm25_ugm3': dict(min_val=0.0, max_val=2000.0,  max_step_change=500.0),
    'pm10_ugm3': dict(min_val=0.0, max_val=3000.0,  max_step_change=800.0),
    'no2_ugm3':  dict(min_val=0.0, max_val=1000.0,  max_step_change=300.0),
    'so2_ugm3':  dict(min_val=0.0, max_val=1000.0,  max_step_change=300.0),
    'co_ugm3':   dict(min_val=0.0, max_val=50000.0, max_step_change=8000.0),
    'o3_ugm3':   dict(min_val=0.0, max_val=1000.0,  max_step_change=300.0),
}

DEFAULT_YEARS = [2022, 2023, 2024, 2025]
DEFAULT_MONTHS = [1, 10, 11, 12]
SOURCE = 'cams-openmeteo-archive'


def payload_hash(data):
    b = data.encode('utf-8') if isinstance(data, str) else data
    return hashlib.sha256(b).hexdigest()


def contiguous_spans(year, months):
    """Collapse requested months into contiguous date ranges, clipped to coverage."""
    ms = sorted(set(months))
    runs, run = [], [ms[0]]
    for m in ms[1:]:
        if m == run[-1] + 1:
            run.append(m)
        else:
            runs.append(run)
            run = [m]
    runs.append(run)

    out = []
    for r in runs:
        start = date(year, r[0], 1)
        end = date(year, r[-1], calendar.monthrange(year, r[-1])[1])
        if end < COVERAGE_START:
            continue                      # entirely before the archive begins
        if start < COVERAGE_START:
            start = COVERAGE_START        # clip rather than fetch nulls
        out.append((start.isoformat(), end.isoformat()))
    return out


def fetch_span(lat, lon, start, end, max_retries=4):
    params = {'latitude': lat, 'longitude': lon, 'start_date': start, 'end_date': end,
              'hourly': HOURLY, 'timezone': 'UTC'}
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


def to_frame(raw_text, city, lat, lon):
    payload = json.loads(raw_text)
    h = payload.get('hourly') or {}
    times = h.get('time') or []
    if not times:
        return pd.DataFrame()

    df = pd.DataFrame({'timestamp': times})
    df['timestamp'] = df['timestamp'].astype(str).str.replace(' ', 'T', regex=False)
    df['timestamp'] = df['timestamp'].apply(
        lambda s: s if s.endswith('+00:00') else (s if len(s) > 16 else s + ':00') + '+00:00')
    for src_col, dest in VAR_MAP.items():
        df[dest] = pd.to_numeric(pd.Series(h.get(src_col) or [None] * len(times)), errors='coerce')

    # Hours the model has no value for are dead weight; drop them rather than
    # storing rows that are entirely null across every pollutant.
    df = df.dropna(subset=list(VAR_MAP.values()), how='all')
    if df.empty:
        return df
    df['lat'], df['lon'] = lat, lon

    df['city'] = city
    return df


def write_chunk(df, label):
    if df.empty:
        return 0, 0

    for metric, opts in QC_PARAMS.items():
        if metric in df.columns:
            df = clean_and_impute(df, metric, 'timestamp', group_cols=['city'], **opts)

    for m in QC_PARAMS:
        base = m.replace('_ugm3', '')
        df = df.rename(columns={f'{m}_clean': f'{base}_clean',
                                f'{m}_imputed': f'{base}_imputed',
                                f'{m}_qc_flag': f'{base}_qc_flag'}, errors='ignore')

    df['source'] = SOURCE
    df['is_synthetic'] = 0

    df, failures = validate(df, 'cams_aq')
    n_fail = len(failures)
    if df.empty:
        logger.warning(f"  {label}: every row failed contract validation")
        return 0, n_fail

    def col(r, name):
        v = r.get(name)
        return None if pd.isna(v) else v

    rows = []
    for r in df.to_dict('records'):
        vals = [r.get('city'), col(r, 'lat'), col(r, 'lon'), r.get('timestamp')]
        for m in ('pm25', 'pm10', 'no2', 'so2', 'co', 'o3'):
            vals += [col(r, f'{m}_ugm3'), col(r, f'{m}_clean'),
                     1 if r.get(f'{m}_imputed', False) else 0,
                     r.get(f'{m}_qc_flag', 'ok')]
        vals += [SOURCE, 0]
        rows.append(tuple(vals))

    sql = """INSERT OR IGNORE INTO cleaned_cams_aq (
                 city, lat, lon, timestamp,
                 pm25_ugm3, pm25_clean, pm25_imputed, pm25_qc_flag,
                 pm10_ugm3, pm10_clean, pm10_imputed, pm10_qc_flag,
                 no2_ugm3, no2_clean, no2_imputed, no2_qc_flag,
                 so2_ugm3, so2_clean, so2_imputed, so2_qc_flag,
                 co_ugm3, co_clean, co_imputed, co_qc_flag,
                 o3_ugm3, o3_clean, o3_imputed, o3_qc_flag,
                 source, is_synthetic
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
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

    df['obs_date'] = df['timestamp'].astype(str).str.slice(0, 10)
    for d, part in df.groupby('obs_date'):
        save_cleaned_data_parquet(part.drop(columns='obs_date'), source='cams_aq',
                                  partition_key='date', partition_value=d,
                                  dedup_keys=['city', 'timestamp'], pure_overwrite=False)
    return len(df), n_fail


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--years', nargs='+', type=int, default=DEFAULT_YEARS)
    ap.add_argument('--months', nargs='+', type=int, default=DEFAULT_MONTHS)
    ap.add_argument('--cities', nargs='+', default=None)
    ap.add_argument('--sleep', type=float, default=0.3)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    init_db()

    cities = dict(CITIES)
    if args.cities:
        want = {c.lower() for c in args.cities}
        unknown = want - set(CITIES)
        if unknown:
            logger.error(f"Unknown cities: {sorted(unknown)}. Known: {sorted(CITIES)}")
            return 1
        cities = {c: v for c, v in cities.items() if c in want}

    plan = [(y, s, e) for y in sorted(args.years) for (s, e) in contiguous_spans(y, args.months)]
    skipped = [y for y in sorted(args.years) if not contiguous_spans(y, args.months)]
    if skipped:
        logger.warning(f"Years entirely before {COVERAGE_START} in the CAMS archive, skipped: {skipped}")

    if not plan:
        logger.error(f"Nothing to fetch: every requested month predates {COVERAGE_START}.")
        return 1

    logger.info(f"Plan: {len(cities)} cities x {len(plan)} spans = {len(plan) * len(cities)} requests")
    for (y, s, e) in plan:
        logger.info(f"  span {s} .. {e}")
    if args.dry_run:
        return 0

    grand = fails = 0
    for (y, start, end) in plan:
        frames = []
        for city, (lat, lon) in cities.items():
            raw = fetch_span(lat, lon, start, end)
            time.sleep(args.sleep)
            if not raw:
                logger.warning(f"  {city} {start}..{end}: no data")
                continue

            h = payload_hash(raw)
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                execute_query(cur,
                    "INSERT OR IGNORE INTO raw_cams_aq (city, lat, lon, span_start, raw_data, raw_data_hash, source, is_synthetic) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (city, lat, lon, start, raw, h, SOURCE, 0))
                conn.commit()
            finally:
                conn.close()
            save_raw_data('cams_aq', start, raw, ext='json')

            f = to_frame(raw, city, lat, lon)
            if not f.empty:
                frames.append(f)

        if not frames:
            logger.warning(f"{start}..{end}: nothing retrieved")
            continue

        chunk = pd.concat(frames, ignore_index=True)
        saved, nf = write_chunk(chunk, f"{start}..{end}")
        grand += saved
        fails += nf
        logger.info(f"{start}..{end}: {len(chunk)} hours fetched, {saved} rows saved "
                    f"({nf} contract failures) | running total {grand}")

    logger.info("=" * 60)
    logger.info(f"Backfill complete: {grand} rows, {fails} contract failures")
    logger.info(f"source='{SOURCE}' -> cleaned_cams_aq (ug/m3). Not CPCB measurements.")
    return 0


if __name__ == '__main__':
    sys.exit(main())

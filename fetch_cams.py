"""
fetch_cams.py — Copernicus CAMS atmospheric composition over India,
served through the Open-Meteo Air Quality API.

Renamed from fetch_sentinel5p.py, which was misleading in three ways at once:

  * It never touched Sentinel-5P. The only implementation was
    `fetch_tropomi_openmeteo_no2`, which calls Open-Meteo. The TROPOMI product
    identifiers, CDSE_BASE and OPENEO_BASE were dead constants.
  * Its docstring claimed TROPOMI needs "no auth required ... from the
    Copernicus Open Access Hub". The Open Access Hub was retired in favour of
    the Copernicus Data Space Ecosystem, and CDSE does require registration.
  * Columns were named no2_ppb, so2_ppb, co_ppb, o3_ppb while the API returns
    micrograms per cubic metre. For NO2 that is a factor of ~1.88 — a silent
    unit error in every downstream calculation. The code even contradicted
    itself: the QC limit next to `no2_ppb` was commented "NO2 µg/m³".

Columns are *_ugm3 now, which is what the values actually are, and that is
what lets gold_layer compute a genuine CPCB National AQI from them.

If you do want real Sentinel-5P TROPOMI column densities, that is a separate
fetcher against CDSE with registered credentials — not a rename of this one.
"""

import os
import json
import time
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv

from db import get_db_connection, execute_many, init_db
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_cams')

LAT_MIN, LAT_MAX = 6.0, 37.0
LON_MIN, LON_MAX = 68.0, 97.0
GRID_STEP_DEG = float(os.getenv('CAMS_GRID_STEP_DEG', '2.0'))

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Open-Meteo accepts comma-separated coordinate lists and returns one result
# object per location. Batching turns ~240 sequential HTTP requests into a
# handful, which is the difference between a job that takes minutes and one
# that takes seconds — and keeps us far inside the free tier's daily limit.
BATCH_SIZE = int(os.getenv('CAMS_BATCH_SIZE', '25'))

# All of these are returned as µg/m³ by the API.
HOURLY_VARIABLES = [
    'nitrogen_dioxide', 'sulphur_dioxide', 'carbon_monoxide',
    'ozone', 'pm2_5', 'pm10',
]

VARIABLE_TO_COLUMN = {
    'nitrogen_dioxide': 'no2_ugm3',
    'sulphur_dioxide':  'so2_ugm3',
    'carbon_monoxide':  'co_ugm3',
    'ozone':            'o3_ugm3',
    'pm2_5':            'pm25_ugm3',
    'pm10':             'pm10_ugm3',
}

# Plausibility bounds in µg/m³. CO is far higher than the rest because it is
# normally present at milligram-per-cubic-metre levels.
QC_LIMITS = {
    'no2_ugm3':  (0.0, 1000.0),
    'so2_ugm3':  (0.0, 2000.0),
    'co_ugm3':   (0.0, 50000.0),
    'o3_ugm3':   (0.0, 1000.0),
    'pm25_ugm3': (0.0, 1000.0),
    'pm10_ugm3': (0.0, 2000.0),
}

# Reference points used by the gold layer to attach a CAMS-derived CPCB AQI
# to a city. Kept here so there is a single definition of each city's centre.
CITY_GRID = {
    'delhi':     (28.61, 77.21),
    'mumbai':    (19.08, 72.88),
    'bengaluru': (12.97, 77.59),
    'chennai':   (13.08, 80.27),
    'kolkata':   (22.57, 88.36),
    'hyderabad': (17.39, 78.49),
    'ahmedabad': (23.02, 72.57),
    'lucknow':   (26.85, 80.95),
    'jaipur':    (26.91, 75.79),
    'patna':     (25.59, 85.14),
}


def compute_payload_hash(data):
    b = data.encode('utf-8') if isinstance(data, str) else data
    return hashlib.sha256(b).hexdigest()


def build_grid():
    """Regular lat/lon grid covering India at GRID_STEP_DEG resolution."""
    lats = np.arange(LAT_MIN, LAT_MAX + 1e-9, GRID_STEP_DEG)
    lons = np.arange(LON_MIN, LON_MAX + 1e-9, GRID_STEP_DEG)
    return [(round(float(la), 2), round(float(lo), 2)) for la in lats for lo in lons]


def fetch_batch(points, date_str, max_retries=3):
    """
    Fetches one batch of coordinates in a single request.

    Open-Meteo returns a bare object for a single location and a list for
    several, so both shapes are normalised here.
    """
    lat_param = ','.join(f'{lat:.2f}' for lat, _ in points)
    lon_param = ','.join(f'{lon:.2f}' for _, lon in points)
    params = {
        'latitude': lat_param,
        'longitude': lon_param,
        'hourly': ','.join(HOURLY_VARIABLES),
        'start_date': date_str,
        'end_date': date_str,
        'timezone': 'UTC',
    }

    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(AIR_QUALITY_URL, params=params, timeout=45)
            r.raise_for_status()
            payload = r.json()
            return payload if isinstance(payload, list) else [payload]
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            logger.warning(f"CAMS batch attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def payload_to_rows(payload, requested_point, fetch_time):
    """Flattens one location's hourly block into per-timestamp rows."""
    hourly = payload.get('hourly', {}) or {}
    times = hourly.get('time', []) or []
    if not times:
        return []

    # Prefer the coordinates the API echoes back — it snaps to its own grid.
    lat = round(float(payload.get('latitude', requested_point[0])), 2)
    lon = round(float(payload.get('longitude', requested_point[1])), 2)

    series = {col: hourly.get(var, []) or []
              for var, col in VARIABLE_TO_COLUMN.items()}

    rows = []
    for i, ts in enumerate(times):
        row = {
            'lat': lat,
            'lon': lon,
            # Open-Meteo returns 'YYYY-MM-DDTHH:MM' with timezone=UTC.
            'timestamp': f"{ts}:00+00:00" if len(ts) == 16 else str(ts),
            'fetched_at': fetch_time,
            'source': 'cams_via_open-meteo',
            'is_synthetic': 0,
        }
        for col, values in series.items():
            value = values[i] if i < len(values) else None
            row[f'{col}_raw'] = float(value) if value is not None else np.nan
        rows.append(row)
    return rows


def fetch_india_grid(date_str):
    """Fetches the whole India grid for one day, in batches."""
    points = build_grid()
    fetch_time = datetime.now(timezone.utc).isoformat()
    rows, errors = [], []

    for start in range(0, len(points), BATCH_SIZE):
        batch = points[start:start + BATCH_SIZE]
        try:
            payloads = fetch_batch(batch, date_str)
        except Exception as e:
            errors.append(f"batch at index {start}: {e}")
            logger.error(f"CAMS batch starting at {start} failed: {e}")
            continue

        for point, payload in zip(batch, payloads):
            rows.extend(payload_to_rows(payload, point, fetch_time))
        time.sleep(0.2)

    return rows, errors, fetch_time


def apply_qc(df):
    """Range-checks every pollutant and records the flag, keeping raw values."""
    for col, (lo, hi) in QC_LIMITS.items():
        raw_col = f'{col}_raw'
        if raw_col not in df.columns:
            df[raw_col] = np.nan
        values = pd.to_numeric(df[raw_col], errors='coerce')
        df[raw_col] = values
        # Out-of-range values are flagged, never deleted — same policy as the
        # rest of the pipeline, so genuine pollution episodes survive.
        df[f'{col}_clean'] = values
        df[f'{col}_qc_flag'] = np.where(
            values.isna(), 'missing',
            np.where((values < lo) | (values > hi), 'range_fail', 'ok'),
        )
    return df


def main():
    logger.info("Starting CAMS air-composition fetch (via Open-Meteo)...")
    init_db()

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    rows, errors, fetch_time = fetch_india_grid(today)

    if not rows:
        return ('failure', 0, "; ".join(errors) or 'No CAMS data retrieved')

    logger.info(f"Retrieved {len(rows)} CAMS grid observations.")
    df = apply_qc(pd.DataFrame(rows))

    df, failures = validate(df, 'cams')
    if df.empty:
        return ('failure', 0, f"All CAMS rows failed contract validation ({len(failures)} failures).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        raw_rows = [
            (r.lat, r.lon, r.timestamp,
             _f(r.no2_ugm3_raw), _f(r.so2_ugm3_raw), _f(r.co_ugm3_raw),
             _f(r.o3_ugm3_raw), _f(r.pm25_ugm3_raw), _f(r.pm10_ugm3_raw),
             r.fetched_at,
             compute_payload_hash(json.dumps(
                 {'lat': r.lat, 'lon': r.lon, 'ts': r.timestamp}, sort_keys=True)),
             'cams_via_open-meteo', 0)
            for r in df.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO raw_cams
                (lat, lon, timestamp, no2_ugm3, so2_ugm3, co_ugm3, o3_ugm3,
                 pm25_ugm3, pm10_ugm3, fetched_at, raw_data_hash, source, is_synthetic)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, raw_rows)

        cleaned_rows = [
            (r.lat, r.lon, r.timestamp,
             _f(r.no2_ugm3_raw), _f(r.no2_ugm3_clean), r.no2_ugm3_qc_flag,
             _f(r.so2_ugm3_raw), _f(r.so2_ugm3_clean), r.so2_ugm3_qc_flag,
             _f(r.co_ugm3_raw), _f(r.co_ugm3_clean), r.co_ugm3_qc_flag,
             _f(r.o3_ugm3_raw), _f(r.o3_ugm3_clean), r.o3_ugm3_qc_flag,
             _f(r.pm25_ugm3_raw), _f(r.pm25_ugm3_clean), r.pm25_ugm3_qc_flag,
             _f(r.pm10_ugm3_raw), _f(r.pm10_ugm3_clean), r.pm10_ugm3_qc_flag,
             r.fetched_at, 'cams_via_open-meteo', 0)
            for r in df.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO cleaned_cams (
                lat, lon, timestamp,
                no2_ugm3_raw, no2_ugm3_clean, no2_ugm3_qc_flag,
                so2_ugm3_raw, so2_ugm3_clean, so2_ugm3_qc_flag,
                co_ugm3_raw, co_ugm3_clean, co_ugm3_qc_flag,
                o3_ugm3_raw, o3_ugm3_clean, o3_ugm3_qc_flag,
                pm25_ugm3_raw, pm25_ugm3_clean, pm25_ugm3_qc_flag,
                pm10_ugm3_raw, pm10_ugm3_clean, pm10_ugm3_qc_flag,
                fetched_at, source, is_synthetic
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cleaned_rows)
        conn.commit()
    except Exception as e:
        logger.error(f"Database error during CAMS save: {e}")
        return ('failure', 0, str(e))
    finally:
        if conn:
            conn.close()

    # One raw payload record per run, not one file per row.
    save_raw_data('cams', fetch_time,
                  json.dumps({'date': today, 'rows': len(rows),
                              'grid_step_deg': GRID_STEP_DEG}, sort_keys=True),
                  ext='json')
    save_cleaned_data_parquet(
        df, source='cams', partition_key='date', partition_value=today,
        dedup_keys=['lat', 'lon', 'timestamp'], pure_overwrite=False,
    )
    logger.info(f"Saved {len(df)} cleaned CAMS observations to database and Parquet.")

    status, err = 'success', None
    if errors:
        status = 'partial'
        err = f"{len(errors)} batch(es) failed: " + "; ".join(errors)
    if not failures.empty:
        status = 'partial'
        msg = f"{len(failures['index'].dropna().unique())} rows dropped by contract validation."
        err = f"{err} | {msg}" if err else msg

    return (status, len(df), err)


def _f(v):
    return None if v is None or pd.isna(v) else float(v)


if __name__ == '__main__':
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('cams', _started, _result)

"""
fetch_weather.py — Surface weather for Indian WMO station locations.

WHAT THIS IS NOT: IMD data.
IMD's endpoint https://mausam.imd.gov.in/api/current_wx_api.php?id={STATION_ID}
exists and returns JSON, but answers HTTP 401 "Your IP/Domain needs to be
whitelisted". To get access, either write to help.mausam@imd.gov.in or deploy
from an already-whitelisted institutional network. Until then this module
queries Open-Meteo at each station's coordinates, which is free and needs no
key. The tables are called *_weather, not *_imd, so nobody reads a number here
and believes it came from the Indian Meteorological Department.

Changes from the previous version:
  * Eleven sequential HTTP requests became one batched request — Open-Meteo
    accepts comma-separated coordinates.
  * Column names carry their units (temperature_c, wind_speed_ms, ...) so a
    unit mismatch is visible at the schema instead of two layers downstream.
  * The import-time `assert` statements are gone. Under `python -O` asserts
    are stripped, so they validated nothing in exactly the deployment mode
    where validation matters; the checks now run as a normal function.
"""

import os
import time
import json
import hashlib
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

from db import get_db_connection, execute_many, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet, load_historical_context
from contracts import validate

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_weather')

load_dotenv()

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
BATCH_SIZE = int(os.getenv('WEATHER_BATCH_SIZE', '25'))

# WMO station ID -> (name, lat, lon). Source: WMO global observing station list.
INDIA_STATIONS = {
    '42182': ('Delhi',       28.58, 77.20),
    '42339': ('Jodhpur',     26.30, 73.02),
    '42369': ('Lucknow',     26.77, 80.88),
    '42647': ('Ahmedabad',   23.07, 72.63),
    '42667': ('Bhopal',      23.28, 77.35),
    '42807': ('Kolkata',     22.65, 88.45),
    '42867': ('Nagpur',      21.10, 79.05),
    '43003': ('Mumbai',      19.12, 72.85),
    '43128': ('Hyderabad',   17.45, 78.47),
    '43279': ('Chennai',     13.00, 80.18),
    '43295': ('Bengaluru',   12.97, 77.58),
}

CURRENT_VARIABLES = [
    'temperature_2m', 'relative_humidity_2m', 'precipitation',
    'wind_speed_10m', 'wind_direction_10m',
]

VARIABLE_TO_COLUMN = {
    'temperature_2m':       'temperature_c_raw',
    'relative_humidity_2m': 'humidity_pct_raw',
    'precipitation':        'rainfall_mm_raw',
    'wind_speed_10m':       'wind_speed_ms_raw',
    'wind_direction_10m':   'wind_dir_deg_raw',
}

QC_PARAMS = {
    'temperature_c_raw': {'min_val': -50.0, 'max_val': 60.0,  'max_step_change': 15.0,
                          'is_circular': False, 'ignore_zero_flatline': False},
    'humidity_pct_raw':  {'min_val': 0.0,   'max_val': 100.0, 'max_step_change': 50.0,
                          'is_circular': False, 'ignore_zero_flatline': False},
    'rainfall_mm_raw':   {'min_val': 0.0,   'max_val': 500.0, 'max_step_change': 100.0,
                          'is_circular': False, 'ignore_zero_flatline': True},
    'wind_speed_ms_raw': {'min_val': 0.0,   'max_val': 100.0, 'max_step_change': 30.0,
                          'is_circular': False, 'ignore_zero_flatline': False},
    'wind_dir_deg_raw':  {'min_val': 0.0,   'max_val': 360.0, 'max_step_change': 180.0,
                          'is_circular': True,  'ignore_zero_flatline': False},
}


def validate_station_table(stations=None):
    """
    Checks the station table for the mistakes that are easy to make when
    editing it by hand: a duplicated WMO ID, the same city name on two IDs, or
    coordinates outside India. Raises ValueError rather than asserting.
    """
    stations = INDIA_STATIONS if stations is None else stations

    names = [meta[0] for meta in stations.values()]
    duplicate_names = {n for n in names if names.count(n) > 1}
    if duplicate_names:
        raise ValueError(f"Duplicate station names in INDIA_STATIONS: {sorted(duplicate_names)}")

    for station_id, (name, lat, lon) in stations.items():
        if not (6.0 <= lat <= 37.0 and 68.0 <= lon <= 97.0):
            raise ValueError(
                f"Station {station_id} ({name}) at ({lat}, {lon}) is outside the India bounding box"
            )
    return True


def compute_payload_hash(data):
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(b).hexdigest()


def fetch_batch(stations, max_retries=3):
    """
    Fetches current conditions for several stations in one request.
    `stations` is a list of (station_id, name, lat, lon).
    """
    params = {
        'latitude': ','.join(f'{lat:.4f}' for _, _, lat, _ in stations),
        'longitude': ','.join(f'{lon:.4f}' for _, _, _, lon in stations),
        'current': ','.join(CURRENT_VARIABLES),
        'wind_speed_unit': 'ms',
        'timezone': 'UTC',
    }

    last_err = None
    for attempt in range(max_retries):
        try:
            r = requests.get(FORECAST_URL, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            return payload if isinstance(payload, list) else [payload]
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            logger.warning(f"Open-Meteo attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def payload_to_record(payload, station, fetch_time):
    """Turns one location's `current` block into a pipeline record."""
    station_id, name, lat, lon = station
    current = payload.get('current', {}) or {}
    obs_time = current.get('time')

    record = {
        'station_id': station_id,
        'station': name,
        'lat': lat,
        'lon': lon,
        'timestamp': f"{obs_time}:00+00:00" if obs_time and len(obs_time) == 16 else (
            str(obs_time) if obs_time else fetch_time),
        'source': 'open-meteo',
    }
    for variable, column in VARIABLE_TO_COLUMN.items():
        value = current.get(variable)
        record[column] = float(value) if value is not None else None
    return record


def main():
    logger.info("Starting surface weather fetch via Open-Meteo "
                "(IMD's own API requires IP whitelisting)")
    validate_station_table()
    init_db()

    fetch_time = datetime.now(timezone.utc).isoformat()
    stations = [(sid, name, lat, lon) for sid, (name, lat, lon) in INDIA_STATIONS.items()]
    records, errors = [], []

    for start in range(0, len(stations), BATCH_SIZE):
        batch = stations[start:start + BATCH_SIZE]
        try:
            payloads = fetch_batch(batch)
        except Exception as e:
            errors.append(f"batch at {start}: {e}")
            logger.error(f"Weather batch starting at {start} failed: {e}")
            continue
        for station, payload in zip(batch, payloads):
            records.append(payload_to_record(payload, station, fetch_time))

    if not records:
        return ('failure', 0, "; ".join(errors) or "No weather data fetched")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        raw_rows = []
        for record in records:
            raw_str = json.dumps(record, sort_keys=True)
            raw_rows.append((record['timestamp'], raw_str,
                             compute_payload_hash(raw_str), 'open-meteo', 0))
            save_raw_data('weather', record['timestamp'], record, ext='json')
        execute_many(cur, """
            INSERT OR IGNORE INTO raw_weather
                (timestamp, raw_data, raw_data_hash, source, is_synthetic)
            VALUES (%s, %s, %s, %s, %s)
        """, raw_rows)
        conn.commit()
        logger.info(f"Saved {len(raw_rows)} raw weather records.")
    except Exception as e:
        logger.error(f"Database error during raw weather save: {e}")
    finally:
        if conn:
            conn.close()

    df = pd.DataFrame(records)
    df['_is_new'] = True

    ctx_df = load_historical_context('weather', fetch_time)
    if ctx_df is not None and not ctx_df.empty:
        ctx_df = ctx_df.copy()
        ctx_df['_is_new'] = False
        full_df = pd.concat([ctx_df, df], ignore_index=True)
    else:
        full_df = df.copy()

    for metric, opts in QC_PARAMS.items():
        if metric in full_df.columns:
            full_df = clean_and_impute(
                full_df, metric, time_col='timestamp', group_cols=['station'],
                min_val=opts['min_val'], max_val=opts['max_val'],
                max_step_change=opts['max_step_change'],
                is_circular=opts['is_circular'],
                ignore_zero_flatline=opts['ignore_zero_flatline'],
            )

    df_clean = full_df[full_df['_is_new']].copy()
    df_clean.drop(columns=['_is_new'], inplace=True)

    rename_map = {}
    for metric in QC_PARAMS:
        base = metric[:-len('_raw')]
        rename_map[f'{metric}_clean'] = f'{base}_clean'
        rename_map[f'{metric}_imputed'] = f'{base}_imputed'
        rename_map[f'{metric}_qc_flag'] = f'{base}_qc_flag'

    stale = [c for c in rename_map.values() if c in df_clean.columns]
    if stale:
        df_clean.drop(columns=stale, inplace=True)
    df_clean.rename(columns=rename_map, inplace=True)

    df_clean['source'] = 'open-meteo'
    df_clean['is_synthetic'] = 0

    df_clean, failures = validate(df_clean, 'weather')
    if df_clean.empty:
        return ('failure', 0,
                f"All weather rows failed contract validation ({len(failures)} failures).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        rows = [
            (r.station, r.station_id, _f(r.lat), _f(r.lon), r.timestamp,
             _f(r.temperature_c_raw), _f(r.temperature_c_clean),
             int(bool(r.temperature_c_imputed)), r.temperature_c_qc_flag,
             _f(r.humidity_pct_raw), _f(r.humidity_pct_clean),
             int(bool(r.humidity_pct_imputed)), r.humidity_pct_qc_flag,
             _f(r.rainfall_mm_raw), _f(r.rainfall_mm_clean),
             int(bool(r.rainfall_mm_imputed)), r.rainfall_mm_qc_flag,
             _f(r.wind_speed_ms_raw), _f(r.wind_speed_ms_clean),
             int(bool(r.wind_speed_ms_imputed)), r.wind_speed_ms_qc_flag,
             _f(r.wind_dir_deg_raw), _f(r.wind_dir_deg_clean),
             int(bool(r.wind_dir_deg_imputed)), r.wind_dir_deg_qc_flag,
             'open-meteo', 0)
            for r in df_clean.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO cleaned_weather (
                station, station_id, lat, lon, timestamp,
                temperature_c_raw, temperature_c_clean, temperature_c_imputed, temperature_c_qc_flag,
                humidity_pct_raw, humidity_pct_clean, humidity_pct_imputed, humidity_pct_qc_flag,
                rainfall_mm_raw, rainfall_mm_clean, rainfall_mm_imputed, rainfall_mm_qc_flag,
                wind_speed_ms_raw, wind_speed_ms_clean, wind_speed_ms_imputed, wind_speed_ms_qc_flag,
                wind_dir_deg_raw, wind_dir_deg_clean, wind_dir_deg_imputed, wind_dir_deg_qc_flag,
                source, is_synthetic
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
        conn.commit()

        save_cleaned_data_parquet(
            df_clean, source='weather', partition_key='date', partition_value=fetch_time[:10],
            dedup_keys=['station', 'timestamp'], pure_overwrite=False,
        )
        logger.info(f"Saved {len(df_clean)} cleaned weather records to database and Parquet.")
    except Exception as e:
        logger.error(f"Database error during cleaned weather save: {e}")
        return ('failure', 0, str(e))
    finally:
        if conn:
            conn.close()

    status, err = 'success', None
    if len(records) != len(INDIA_STATIONS):
        status = 'partial'
        err = f"Fetched {len(records)} of {len(INDIA_STATIONS)} stations. " + "; ".join(errors)
    if not failures.empty:
        status = 'partial'
        msg = f"{len(failures['index'].dropna().unique())} rows dropped by contract validation."
        err = f"{err} | {msg}" if err else msg

    return (status, len(df_clean), err)


def _f(v):
    return None if v is None or pd.isna(v) else float(v)


if __name__ == "__main__":
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('weather', _started, _result)

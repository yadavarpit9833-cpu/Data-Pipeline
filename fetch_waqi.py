"""
fetch_waqi.py — World Air Quality Index (waqi.info) city feeds for India.

Renamed from fetch_cpcb.py, because it never called CPCB. There is no CPCB
endpoint in this module and there never was: the only implementation was
`fetch_waqi_fallback`, and the table it wrote to was called raw_cpcb.

THE UNIT BUG THIS FIXES
-----------------------
WAQI's `iaqi` block contains AQI SUB-INDICES on the US EPA scale, not mass
concentrations. The old code read them into columns called pm25_raw / pm10_raw
as if they were micrograms per cubic metre, and gold_layer.py then pushed those
values through India's CPCB concentration-to-AQI breakpoints — converting an
AQI into an AQI. An input of 155 (already "unhealthy") came out as roughly 370
("very poor"). Every published AQI number the pipeline produced was wrong.

Columns are now named *_aqi_* so the scale is impossible to misread, aqi_scale
records which national scale they follow, and the gold layer combines
sub-indices by taking their maximum, which is what an AQI already is.

If you need real µg/m³ concentrations for Indian CPCB stations, use OpenAQ v3
(https://docs.openaq.org/) — it mirrors CPCB at station level in mass units.
That is a different fetcher, not a rename of this one.
"""

import os
import time
import json
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv

from db import get_db_connection, execute_many, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet, load_historical_context
from contracts import validate

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_waqi')

load_dotenv()

WAQI_TOKEN = os.getenv('WAQI_TOKEN')

CITIES = [c.strip() for c in os.getenv(
    'WAQI_CITIES', 'delhi,mumbai,bengaluru,chennai,kolkata,hyderabad,ahmedabad,lucknow,jaipur,patna'
).split(',') if c.strip()]

# WAQI reports on the US EPA AQI scale, which is a 0-500 index by definition.
# The bound is the scale's own maximum, not a guess about pollution levels.
AQI_INDEX_MIN, AQI_INDEX_MAX = 0.0, 500.0

POLLUTANTS = ['pm25', 'pm10', 'no2', 'so2', 'co', 'o3']


def redact_token(text):
    """Keeps the WAQI token out of logs — it is passed as a query parameter."""
    if not text:
        return text
    text = str(text)
    if WAQI_TOKEN:
        text = text.replace(WAQI_TOKEN, '<WAQI_TOKEN>')
    return text


def compute_payload_hash(data):
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(b).hexdigest()


def fetch_with_retry(url, max_retries=3):
    """Fetches a WAQI feed with exponential backoff."""
    last_err = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed for "
                         f"{redact_token(url)}: {redact_token(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def fetch_city(city):
    """Fetches one city's live feed. Returns None when WAQI reports no data."""
    if not WAQI_TOKEN or WAQI_TOKEN == 'your_waqi_token_here':
        raise RuntimeError("WAQI_TOKEN is missing or still the placeholder in .env")
    data = fetch_with_retry(f"https://api.waqi.info/feed/{city}/?token={WAQI_TOKEN}")
    if data and data.get('status') == 'ok':
        return data['data']
    logger.warning(f"WAQI returned status={data.get('status') if data else 'none'} for {city}")
    return None


def safe_extract_metric(iaqi_dict, metric_name):
    """Reads one AQI sub-index out of the iaqi block, tolerating missing keys."""
    if not isinstance(iaqi_dict, dict):
        return np.nan
    metric_obj = iaqi_dict.get(metric_name)
    if isinstance(metric_obj, dict):
        value = metric_obj.get('v')
        try:
            return float(value)
        except (TypeError, ValueError):
            return np.nan
    return np.nan


def extract_obs_timestamp(data_dict, fallback_iso):
    """Reads the observation time from the payload ('time' -> 'iso', then 's')."""
    time_obj = data_dict.get('time', {})
    if isinstance(time_obj, dict):
        iso_ts = time_obj.get('iso') or time_obj.get('s')
        if iso_ts:
            return str(iso_ts)
    return fallback_iso


def main():
    logger.info("Starting WAQI air quality fetch")
    init_db()

    raw_data_list, errors = [], []
    for city in CITIES:
        try:
            data = fetch_city(city)
            if data:
                data['query_city'] = city
                raw_data_list.append(data)
            else:
                errors.append(f"{city}: no data")
        except Exception as e:
            logger.error(f"Failed to fetch {city}: {redact_token(e)}")
            errors.append(f"{city}: {redact_token(e)}")
        # WAQI's free tier is rate limited; stay well inside it.
        time.sleep(0.3)

    if not raw_data_list:
        return ('failure', 0, "; ".join(errors) or "No WAQI data fetched")

    fetch_time = datetime.now(timezone.utc).isoformat()

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        raw_rows = []
        for data in raw_data_list:
            raw_str = json.dumps(data, sort_keys=True)
            obs_ts = extract_obs_timestamp(data, fetch_time)
            raw_rows.append((obs_ts, raw_str, compute_payload_hash(raw_str), 'waqi', 0))
            save_raw_data('waqi', obs_ts, data, ext='json')
        # INSERT OR IGNORE rather than upsert: raw payloads are historical
        # facts, so an existing row is never rewritten.
        execute_many(cur, """
            INSERT OR IGNORE INTO raw_waqi (timestamp, raw_data, raw_data_hash, source, is_synthetic)
            VALUES (%s, %s, %s, %s, %s)
        """, raw_rows)
        conn.commit()
        logger.info(f"Saved {len(raw_rows)} raw WAQI payloads.")
    except Exception as e:
        logger.error(f"Database error during raw WAQI save: {redact_token(e)}")
    finally:
        if conn:
            conn.close()

    records = []
    for d in raw_data_list:
        iaqi = d.get('iaqi', {})
        record = {
            'station_id': str(d.get('idx', '')),
            'city': d.get('query_city', ''),
            'timestamp': extract_obs_timestamp(d, fetch_time),
            'aqi_scale': 'us_epa',
        }
        for pollutant in POLLUTANTS:
            record[f'{pollutant}_aqi_raw'] = safe_extract_metric(iaqi, pollutant)
        records.append(record)

    df = pd.DataFrame(records)
    df['_is_new'] = True

    # Historical context so step/flatline checks work across the midnight
    # partition boundary instead of restarting blind every day.
    ctx_df = load_historical_context('waqi', fetch_time)
    if ctx_df is not None and not ctx_df.empty:
        ctx_df = ctx_df.copy()
        ctx_df['_is_new'] = False
        full_df = pd.concat([ctx_df, df], ignore_index=True)
    else:
        full_df = df.copy()

    # All six pollutants share one scale now, so one set of thresholds applies.
    # A 150-point jump between consecutive readings at one station is a
    # plausible dust storm or a sensor fault; either way it is worth flagging.
    for pollutant in POLLUTANTS:
        metric = f'{pollutant}_aqi_raw'
        if metric in full_df.columns:
            full_df = clean_and_impute(
                full_df, metric, time_col='timestamp', group_cols=['station_id'],
                min_val=AQI_INDEX_MIN, max_val=AQI_INDEX_MAX, max_step_change=150.0,
            )

    df_clean = full_df[full_df['_is_new']].copy()
    df_clean.drop(columns=['_is_new'], inplace=True)

    rename_map = {}
    for pollutant in POLLUTANTS:
        metric = f'{pollutant}_aqi_raw'
        base = f'{pollutant}_aqi'
        rename_map[f'{metric}_clean'] = f'{base}_clean'
        rename_map[f'{metric}_imputed'] = f'{base}_imputed'
        rename_map[f'{metric}_qc_flag'] = f'{base}_qc_flag'

    stale = [c for c in rename_map.values() if c in df_clean.columns]
    if stale:
        df_clean.drop(columns=stale, inplace=True)
    df_clean.rename(columns=rename_map, inplace=True)

    df_clean['source'] = 'waqi'
    df_clean['is_synthetic'] = 0

    df_clean, failures = validate(df_clean, 'waqi')
    if df_clean.empty:
        return ('failure', 0, f"All WAQI rows failed contract validation ({len(failures)} failures).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        rows = []
        for r in df_clean.itertuples(index=False):
            row = [r.station_id, r.city, r.timestamp, 'us_epa']
            for pollutant in POLLUTANTS:
                row.extend([
                    _f(getattr(r, f'{pollutant}_aqi_raw', None)),
                    _f(getattr(r, f'{pollutant}_aqi_clean', None)),
                    int(bool(getattr(r, f'{pollutant}_aqi_imputed', False))),
                    getattr(r, f'{pollutant}_aqi_qc_flag', 'ok'),
                ])
            row.extend(['waqi', 0])
            rows.append(tuple(row))

        execute_many(cur, """
            INSERT OR IGNORE INTO cleaned_waqi (
                station_id, city, timestamp, aqi_scale,
                pm25_aqi_raw, pm25_aqi_clean, pm25_aqi_imputed, pm25_aqi_qc_flag,
                pm10_aqi_raw, pm10_aqi_clean, pm10_aqi_imputed, pm10_aqi_qc_flag,
                no2_aqi_raw, no2_aqi_clean, no2_aqi_imputed, no2_aqi_qc_flag,
                so2_aqi_raw, so2_aqi_clean, so2_aqi_imputed, so2_aqi_qc_flag,
                co_aqi_raw, co_aqi_clean, co_aqi_imputed, co_aqi_qc_flag,
                o3_aqi_raw, o3_aqi_clean, o3_aqi_imputed, o3_aqi_qc_flag,
                source, is_synthetic
            ) VALUES (%s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s)
        """, rows)
        conn.commit()

        save_cleaned_data_parquet(
            df_clean, source='waqi', partition_key='date', partition_value=fetch_time[:10],
            dedup_keys=['station_id', 'timestamp'], pure_overwrite=False,
        )
        logger.info(f"Saved {len(df_clean)} cleaned WAQI records to database and Parquet.")
    except Exception as e:
        logger.error(f"Database error during cleaned WAQI save: {redact_token(e)}")
        return ('failure', 0, redact_token(str(e)))
    finally:
        if conn:
            conn.close()

    status, err = 'success', None
    if len(raw_data_list) != len(CITIES):
        status = 'partial'
        err = f"Fetched {len(raw_data_list)} of {len(CITIES)} cities. " + "; ".join(errors)
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
    log_run('waqi', _started, _result)

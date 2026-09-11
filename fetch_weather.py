# ==============================================================================
# BEST-EFFORT MODULE: REQUIRES MANUAL ENDPOINT VERIFICATION
# ==============================================================================
#
# IMD (India Meteorological Department) weather data endpoint discovery results:
#
# CONFIRMED ENDPOINT:  https://mausam.imd.gov.in/api/current_wx_api.php?id={STATION_ID}
# STATUS:              HTTP 401 — "Your IP/Domain needs to be whitelisted"
#
# IMD's current_wx_api.php endpoint exists and returns JSON weather data,
# BUT it requires IP whitelisting by IMD. Options to gain access:
#   1. Contact IMD at: help.mausam@imd.gov.in to request API access/whitelisting
#   2. Deploy this script on a whitelisted server (government or research institute)
#   3. Use the Open-Meteo API as an alternative (free, no key needed):
#      https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true
#
# This script now uses Open-Meteo as a real, live, no-key-needed fallback
# for temperature, wind speed, and precipitation data.
# Station WMO IDs are mapped to lat/lon coordinates for Open-Meteo queries.
#
# When IMD whitelisting is granted, replace the Open-Meteo call with:
#   session.get(f"https://mausam.imd.gov.in/api/current_wx_api.php?id={station_id}")
# ==============================================================================

import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from db import get_db_connection, execute_query, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet, load_historical_context
from contracts import validate

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_weather')

load_dotenv()

# Indian station mapping: WMO station ID -> (name, lat, lon)
# Source: WMO global station list for India
INDIA_STATIONS = {
    '42369': ('Lucknow',     26.77, 80.88),
    '42339': ('Jodhpur',     26.30, 73.02),
    '42867': ('Nagpur',      21.10, 79.05),
    '43128': ('Hyderabad',   17.45, 78.47),
    '43279': ('Chennai',     13.00, 80.18),
    '42667': ('Bhopal',      23.28, 77.35),
    '42182': ('Delhi',       28.58, 77.20),
    '42647': ('Ahmedabad',   23.07, 72.63),
    '43295': ('Bengaluru',   12.97, 77.58),
    '42807': ('Kolkata',     22.65, 88.45),
    '43003': ('Mumbai',      19.12, 72.85),
}

# Ensure station list is valid: 11 distinct stations expected
assert len(INDIA_STATIONS) == 11, f"Expected 11 unique stations in INDIA_STATIONS, but found {len(INDIA_STATIONS)}. Check for duplicate WMO IDs."
assert len(set(INDIA_STATIONS.values())) == len(INDIA_STATIONS.values()), "Duplicate station name found in INDIA_STATIONS — check for a name mapped to the wrong ID."

import hashlib

def compute_payload_hash(data):
    """Computes SHA-256 hash of raw payload for fast, indexed uniqueness checks."""
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(b).hexdigest()

def fetch_open_meteo(station_id, name, lat, lon, max_retries=3):
    """
    Fetch current weather from Open-Meteo API (free, no key, real live data).
    Used as fallback while IMD API whitelisting is pending.
    Fields: temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m, wind_direction_10m
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m"
        f"&wind_speed_unit=ms"
    )
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            current = data.get('current', {})
            return {
                'station_id': station_id,
                'station':    name,
                'lat':        lat,
                'lon':        lon,
                'obs_timestamp': current.get('time'),
                'temperature_raw': current.get('temperature_2m'),
                'humidity_raw':    current.get('relative_humidity_2m'),
                'rainfall_raw':    current.get('precipitation'),
                'wind_speed_raw':  current.get('wind_speed_10m'),
                'wind_dir_raw':    current.get('wind_direction_10m'),
                'source':          'open-meteo (IMD fallback)',
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Open-Meteo attempt {attempt+1} failed for {name}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)

def main():
    logger.info("Starting IMD data fetch (via Open-Meteo fallback — IMD API requires IP whitelisting)")
    init_db()
    
    fetch_time = datetime.now(timezone.utc).isoformat()
    raw_data_list = []
    errors = []
    
    for station_id, (name, lat, lon) in INDIA_STATIONS.items():
        try:
            data = fetch_open_meteo(station_id, name, lat, lon)
            if data:
                raw_data_list.append(data)
                logger.info(f"Fetched weather for {name}: temp={data['temperature_raw']} deg C, "
                            f"rh={data['humidity_raw']}%, ws={data['wind_speed_raw']}m/s")
            else:
                errors.append(name)
        except Exception as e:
            logger.error(f"Failed to fetch weather for {name}: {e}")
            errors.append(f"{name} ({e})")
        time.sleep(0.3)
    
    if not raw_data_list:
        logger.warning("No IMD/weather data fetched.")
        return ('failure', 0, "; ".join(errors) if errors else "No weather data fetched")
    
    # 1. Save RAW data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for data in raw_data_list:
            raw_str = json.dumps(data)
            raw_hash = compute_payload_hash(raw_str)
            obs_ts = data.get('obs_timestamp') or fetch_time
            # Note: We intentionally use INSERT OR IGNORE (DO NOTHING) over DO UPDATE to preserve original historical data.
            execute_query(
                cursor,
                "INSERT OR IGNORE INTO raw_imd (timestamp, raw_data, raw_data_hash, source, is_synthetic) VALUES (%s, %s, %s, %s, %s)",
                (obs_ts, raw_str, raw_hash, 'open-meteo', 0)
            )
            # Dual-write RAW to data lake
            save_raw_data('weather', obs_ts, data, ext='json')
        conn.commit()
        logger.info(f"Saved {len(raw_data_list)} raw IMD/weather records to database.")
    except Exception as e:
        logger.error(f"Database error during raw IMD save: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass
    
    # 2. Parse into DataFrame and clean
    df = pd.DataFrame(raw_data_list)
    df['timestamp'] = df['obs_timestamp'].fillna(fetch_time)
    
    # Parameter-specific QC thresholds
    qc_params = {
        'temperature_raw': {'min_val': -50.0, 'max_val': 60.0,  'max_step_change': 15.0,  'is_circular': False, 'ignore_zero_flatline': False},
        'humidity_raw':    {'min_val': 0.0,   'max_val': 100.0, 'max_step_change': 50.0,  'is_circular': False, 'ignore_zero_flatline': False},
        'rainfall_raw':    {'min_val': 0.0,   'max_val': 500.0, 'max_step_change': 100.0, 'is_circular': False, 'ignore_zero_flatline': True},
        'wind_speed_raw':  {'min_val': 0.0,   'max_val': 100.0, 'max_step_change': 30.0,  'is_circular': False, 'ignore_zero_flatline': False},
        'wind_dir_raw':    {'min_val': 0.0,   'max_val': 360.0, 'max_step_change': 180.0, 'is_circular': True,  'ignore_zero_flatline': False},
    }
    df['_is_new'] = True
    
    # Load historical context for accurate QC checks across partition boundaries
    ctx_df = load_historical_context('weather', fetch_time)
    if ctx_df is not None and not ctx_df.empty:
        ctx_df['_is_new'] = False
        full_df = pd.concat([ctx_df, df], ignore_index=True)
    else:
        full_df = df.copy()
    
    for metric, opts in qc_params.items():
        if metric in full_df.columns:
            full_df = clean_and_impute(
                full_df, metric, 'timestamp', group_cols=['station'],
                min_val=opts['min_val'], max_val=opts['max_val'], max_step_change=opts['max_step_change'],
                is_circular=opts.get('is_circular', False), ignore_zero_flatline=opts['ignore_zero_flatline']
            )
            
    # Filter back to only the newly fetched rows to insert/save
    df_clean = full_df[full_df['_is_new']].copy()
    df_clean.drop(columns=['_is_new'], inplace=True)
        
    # Rename columns to match the canonical SQLite schema
    rename_map = {}
    for metric in qc_params.keys():
        base = metric.replace('_raw', '')
        rename_map[f"{metric}_clean"] = f"{base}_clean"
        rename_map[f"{metric}_imputed"] = f"{base}_imputed"
        rename_map[f"{metric}_qc_flag"] = f"{base}_qc_flag"
        
    cols_to_drop = [col for col in rename_map.values() if col in df_clean.columns]
    if cols_to_drop:
        df_clean.drop(columns=cols_to_drop, inplace=True)
        
    df_clean.rename(columns=rename_map, inplace=True)
    df_clean['source'] = 'open-meteo'
    df_clean['is_synthetic'] = 0
    
    # Contract validation ? partial success supported
    df_clean, failures = validate(df_clean, 'weather')
    
    if df_clean.empty and not failures.empty:
        logger.error("All weather data failed contract validation.")
        return ('failure', 0, f"All rows failed contract validation. {len(failures)} failures.")
        
    # 3. Save Cleaned Data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for _, row in df_clean.iterrows():
            execute_query(cursor, """
                INSERT OR IGNORE INTO cleaned_imd (
                    station, timestamp,
                    temperature_raw, temperature_clean, temperature_imputed, temperature_qc_flag,
                    humidity_raw, humidity_clean, humidity_imputed, humidity_qc_flag,
                    rainfall_raw, rainfall_clean, rainfall_imputed, rainfall_qc_flag,
                    wind_speed_raw, wind_speed_clean, wind_speed_imputed, wind_speed_qc_flag,
                    wind_dir_raw, wind_dir_clean, wind_dir_imputed, wind_dir_qc_flag,
                    source, is_synthetic
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
            """, (
                row['station'], row['timestamp'],
                row.get('temperature_raw'), row.get('temperature_clean'), 1 if row.get('temperature_imputed') else 0, row.get('temperature_qc_flag', 'ok'),
                row.get('humidity_raw'),    row.get('humidity_clean'),    1 if row.get('humidity_imputed') else 0,    row.get('humidity_qc_flag', 'ok'),
                row.get('rainfall_raw'),    row.get('rainfall_clean'),    1 if row.get('rainfall_imputed') else 0,    row.get('rainfall_qc_flag', 'ok'),
                row.get('wind_speed_raw'),  row.get('wind_speed_clean'),  1 if row.get('wind_speed_imputed') else 0,  row.get('wind_speed_qc_flag', 'ok'),
                row.get('wind_dir_raw'),    row.get('wind_dir_clean'),    1 if row.get('wind_dir_imputed') else 0,    row.get('wind_dir_qc_flag', 'ok'),
                'open-meteo', 0
            ))
        conn.commit()
        
        # Dual-write Cleaned to Parquet
        date_str = fetch_time[:10]
        
        save_cleaned_data_parquet(
            df_clean, source='weather', partition_key='date', partition_value=date_str,
            dedup_keys=['station', 'timestamp'], pure_overwrite=False
        )
        
        logger.info(f"Saved {len(df_clean)} cleaned IMD/weather records to database and Parquet.")
    except Exception as e:
        logger.error(f"Database error during cleaned IMD save: {e}")
        return ('failure', 0, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

    # Status determination
    total_expected = len(INDIA_STATIONS)
    status = 'success'
    err = None
    
    if len(raw_data_list) != total_expected:
        status = 'partial'
        err = f"Fetched {len(raw_data_list)} of {total_expected} stations. Missing: {', '.join(errors)}"
        
    if not failures.empty:
        status = 'partial'
        bad_indices = len(failures['index'].dropna().unique())
        msg = f"{bad_indices} bad rows dropped due to contract violations."
        err = f"{err} | {msg}" if err else msg
        
    return (status, len(df_clean), err)

if __name__ == "__main__":
    from datetime import datetime, timezone
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('weather', _started, _result)

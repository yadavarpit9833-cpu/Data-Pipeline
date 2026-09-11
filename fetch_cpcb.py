import os
import time
import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv
from db import get_db_connection, execute_query, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet, load_historical_context
from contracts import validate, ContractViolationError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_cpcb')

load_dotenv()

WAQI_TOKEN = os.getenv('WAQI_TOKEN')

def fetch_with_retry(url, headers=None, params=None, max_retries=3):
    """Fetch URL with exponential backoff"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)

def fetch_waqi_fallback(city):
    """Fetch live city air quality data using WAQI API"""
    if not WAQI_TOKEN or WAQI_TOKEN == 'your_waqi_token_here':
        logger.warning(f"WAQI_TOKEN missing or set to placeholder in .env. Skipping fetch for {city}.")
        return None
    url = f"https://api.waqi.info/feed/{city}/?token={WAQI_TOKEN}"
    data = fetch_with_retry(url)
    if data and data.get('status') == 'ok':
        return data['data']
    return None

def safe_extract_metric(iaqi_dict, metric_name):
    """Safely extracts value from iaqi dictionary handling None values."""
    if not isinstance(iaqi_dict, dict):
        return np.nan
    metric_obj = iaqi_dict.get(metric_name)
    if isinstance(metric_obj, dict):
        return metric_obj.get('v', np.nan)
    return np.nan

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

def extract_cpcb_obs_timestamp(data_dict, fallback_iso):
    """Extracts actual observation timestamp from WAQI API payload ('time' -> 'iso' or 's')."""
    time_obj = data_dict.get('time', {})
    if isinstance(time_obj, dict):
        iso_ts = time_obj.get('iso') or time_obj.get('s')
        if iso_ts:
            return str(iso_ts)
    return fallback_iso

def main():
    logger.info("Starting CPCB/WAQI data fetch")
    init_db()
    
    cities = ['delhi', 'mumbai', 'bengaluru', 'chennai', 'kolkata']
    raw_data_list = []
    errors = []
    
    for city in cities:
        try:
            data = fetch_waqi_fallback(city)
            if data:
                data['query_city'] = city
                raw_data_list.append(data)
            else:
                errors.append(f"No data for {city}")
        except Exception as e:
            err_msg = f"Failed to fetch {city}: {e}"
            logger.error(err_msg)
            errors.append(err_msg)
            
    if not raw_data_list:
        logger.warning("No data fetched from CPCB/WAQI.")
        return ('failure', 0, "; ".join(errors) if errors else "No CPCB data fetched")
        
    fetch_time = datetime.now(timezone.utc).isoformat()
    
    # 1. Save RAW data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for data in raw_data_list:
            raw_str = json.dumps(data)
            raw_hash = compute_payload_hash(raw_str)
            obs_ts = extract_cpcb_obs_timestamp(data, fetch_time)
            # Note: We intentionally use INSERT OR IGNORE (DO NOTHING) over DO UPDATE to preserve original historical data.
            execute_query(
                cursor,
                "INSERT OR IGNORE INTO raw_cpcb (timestamp, raw_data, raw_data_hash, source, is_synthetic) VALUES (%s, %s, %s, %s, %s)",
                (obs_ts, raw_str, raw_hash, 'cpcb', 0)
            )
            # Dual-write RAW to data lake
            save_raw_data('cpcb', obs_ts, data, ext='json')
        conn.commit()
        logger.info(f"Saved {len(raw_data_list)} raw CPCB/WAQI records to database.")
    except Exception as e:
        logger.error(f"Database error during raw CPCB save: {e}")
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass
        
    # 2. Parse and Clean Data
    parsed_records = []
    for d in raw_data_list:
        try:
            iaqi = d.get('iaqi', {})
            obs_ts = extract_cpcb_obs_timestamp(d, fetch_time)
            record = {
                'station_id': str(d.get('idx', '')),
                'city': d.get('query_city', ''),
                'timestamp': obs_ts,
                'pm25_raw': safe_extract_metric(iaqi, 'pm25'),
                'pm10_raw': safe_extract_metric(iaqi, 'pm10'),
                'no2_raw': safe_extract_metric(iaqi, 'no2'),
                'so2_raw': safe_extract_metric(iaqi, 'so2'),
                'co_raw': safe_extract_metric(iaqi, 'co'),
                'o3_raw': safe_extract_metric(iaqi, 'o3'),
            }
            parsed_records.append(record)
        except Exception as e:
            logger.error(f"Error parsing CPCB record: {e}")
            
    df = pd.DataFrame(parsed_records)
    
    # Parameter-specific QC thresholds
    qc_params = {
        'pm25_raw': {'min_val': 0.0, 'max_val': 1000.0, 'max_step_change': 300.0},
        'pm10_raw': {'min_val': 0.0, 'max_val': 1500.0, 'max_step_change': 450.0},
        'no2_raw':  {'min_val': 0.0, 'max_val': 1000.0, 'max_step_change': 200.0},
        'so2_raw':  {'min_val': 0.0, 'max_val': 1000.0, 'max_step_change': 200.0},
        'co_raw':   {'min_val': 0.0, 'max_val': 100.0,  'max_step_change': 20.0},
        'o3_raw':   {'min_val': 0.0, 'max_val': 1000.0, 'max_step_change': 200.0},
    }
    df['_is_new'] = True
    
    # Load historical context for accurate QC checks across partition boundaries
    ctx_df = load_historical_context('cpcb', fetch_time)
    if ctx_df is not None and not ctx_df.empty:
        ctx_df['_is_new'] = False
        full_df = pd.concat([ctx_df, df], ignore_index=True)
    else:
        full_df = df.copy()
    
    for metric, opts in qc_params.items():
        if metric in full_df.columns:
            full_df = clean_and_impute(
                full_df, metric, 'timestamp', group_cols=['station_id'],
                min_val=opts['min_val'], max_val=opts['max_val'], max_step_change=opts['max_step_change']
            )
            
    # Filter back to only the newly fetched rows to insert/save
    df_clean = full_df[full_df['_is_new']].copy()
    df_clean.drop(columns=['_is_new'], inplace=True)
        
    # Rename columns to match the canonical SQLite schema before validation
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
    df_clean['source'] = 'cpcb'
    df_clean['is_synthetic'] = 0
    
    # Contract validation ? partial success supported
    df_clean, failures = validate(df_clean, 'cpcb')
    
    if df_clean.empty and not failures.empty:
        logger.error("All CPCB data failed contract validation.")
        return ('failure', 0, f"All rows failed contract validation. {len(failures)} failures.")
        
    # 3. Save Cleaned Data to SQLite
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for _, row in df_clean.iterrows():
            execute_query(cursor, """
                INSERT OR IGNORE INTO cleaned_cpcb (
                    station_id, city, timestamp,
                    pm25_raw, pm25_clean, pm25_imputed, pm25_qc_flag,
                    pm10_raw, pm10_clean, pm10_imputed, pm10_qc_flag,
                    no2_raw, no2_clean, no2_imputed, no2_qc_flag,
                    so2_raw, so2_clean, so2_imputed, so2_qc_flag,
                    co_raw, co_clean, co_imputed, co_qc_flag,
                    o3_raw, o3_clean, o3_imputed, o3_qc_flag,
                    source, is_synthetic
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
            """, (
                row['station_id'], row['city'], row['timestamp'],
                row.get('pm25_raw'), row.get('pm25_clean'), 1 if row.get('pm25_imputed') else 0, row.get('pm25_qc_flag', 'ok'),
                row.get('pm10_raw'), row.get('pm10_clean'), 1 if row.get('pm10_imputed') else 0, row.get('pm10_qc_flag', 'ok'),
                row.get('no2_raw'),  row.get('no2_clean'),  1 if row.get('no2_imputed') else 0,  row.get('no2_qc_flag', 'ok'),
                row.get('so2_raw'),  row.get('so2_clean'),  1 if row.get('so2_imputed') else 0,  row.get('so2_qc_flag', 'ok'),
                row.get('co_raw'),   row.get('co_clean'),   1 if row.get('co_imputed') else 0,   row.get('co_qc_flag', 'ok'),
                row.get('o3_raw'),   row.get('o3_clean'),   1 if row.get('o3_imputed') else 0,   row.get('o3_qc_flag', 'ok'),
                'cpcb', 0
            ))
        conn.commit()
        
        # Dual-write Cleaned to Parquet
        date_str = fetch_time[:10]
        save_cleaned_data_parquet(
            df_clean, source='cpcb', partition_key='date', partition_value=date_str,
            dedup_keys=['station_id', 'timestamp'], pure_overwrite=False
        )
        logger.info(f"Saved {len(df_clean)} cleaned CPCB/WAQI records to database and Parquet.")
    except Exception as e:
        logger.error(f"Database error during cleaned CPCB save: {e}")
        return ('failure', 0, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

    # Status determination
    status = 'success'
    err = None
    if len(raw_data_list) != len(cities):
        status = 'partial'
        err = f"Fetched {len(raw_data_list)} of {len(cities)} cities. Missing: {', '.join(errors)}"
    
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
    log_run('cpcb', _started, _result)

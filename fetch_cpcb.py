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
        
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # 1. Save RAW data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for data in raw_data_list:
            execute_query(
                cursor,
                "INSERT INTO raw_cpcb (timestamp, raw_data) VALUES (%s, %s)",
                (timestamp, json.dumps(data))
            )
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
            record = {
                'station_id': str(d.get('idx', '')),
                'city': d.get('query_city', ''),
                'timestamp': timestamp,
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
    
    metrics = ['pm25_raw', 'pm10_raw', 'no2_raw', 'so2_raw', 'co_raw', 'o3_raw']
    for metric in metrics:
        if metric in df.columns:
            df = clean_and_impute(df, metric, 'timestamp', group_cols=['station_id'])
        
    # 3. Save Cleaned Data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            execute_query(cursor, """
                INSERT INTO cleaned_cpcb (
                    station_id, city, timestamp,
                    pm25_raw, pm25_clean, pm25_imputed,
                    pm10_raw, pm10_clean, pm10_imputed,
                    no2_raw, no2_clean, no2_imputed,
                    so2_raw, so2_clean, so2_imputed,
                    co_raw, co_clean, co_imputed,
                    o3_raw, o3_clean, o3_imputed
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                row['station_id'], row['city'], row['timestamp'],
                row.get('pm25_raw'), row.get('pm25_raw_clean'), 1 if row.get('pm25_raw_imputed') else 0,
                row.get('pm10_raw'), row.get('pm10_raw_clean'), 1 if row.get('pm10_raw_imputed') else 0,
                row.get('no2_raw'), row.get('no2_raw_clean'), 1 if row.get('no2_raw_imputed') else 0,
                row.get('so2_raw'), row.get('so2_raw_clean'), 1 if row.get('so2_raw_imputed') else 0,
                row.get('co_raw'), row.get('co_raw_clean'), 1 if row.get('co_raw_imputed') else 0,
                row.get('o3_raw'), row.get('o3_raw_clean'), 1 if row.get('o3_raw_imputed') else 0
            ))
        conn.commit()
        logger.info(f"Saved {len(df)} cleaned CPCB/WAQI records to database.")
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

    # Status determination: 5 cities total
    if len(raw_data_list) == len(cities):
        status = 'success'
        err = None
    else:
        status = 'partial'
        err = f"Fetched {len(raw_data_list)} of {len(cities)} cities. Missing: {', '.join(errors)}"
        
    return (status, len(df), err)

if __name__ == "__main__":
    main()

import os
import time
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from dotenv import load_dotenv
from db import get_db_connection, execute_query, init_db
from cleaning import clean_and_impute

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_firms')

load_dotenv()

FIRMS_MAP_KEY = os.getenv('FIRMS_MAP_KEY')

def fetch_with_retry(url, headers=None, params=None, max_retries=3):
    """Fetch URL with exponential backoff"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)

def main():
    logger.info("Starting FIRMS data fetch")
    init_db()
    
    if not FIRMS_MAP_KEY or FIRMS_MAP_KEY == 'your_firms_map_key_here':
        logger.error("FIRMS_MAP_KEY is missing or set to placeholder in .env file. Please provide a valid key.")
        return ('failure', 0, "FIRMS_MAP_KEY missing or placeholder in .env")

    sensors = ['VIIRS_SNPP_NRT', 'MODIS_NRT']
    area = '68,6,97,37' # India bounding box
    day_range = '1'
    
    timestamp = datetime.now(timezone.utc).isoformat()
    all_dfs = []
    sensor_errors = []
    
    for sensor in sensors:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{FIRMS_MAP_KEY}/{sensor}/{area}/{day_range}"
        
        conn = None
        cursor = None
        try:
            csv_data = fetch_with_retry(url)
            
            # 1. Save RAW Data
            conn = get_db_connection()
            cursor = conn.cursor()
            execute_query(
                cursor,
                "INSERT INTO raw_firms (timestamp, raw_data) VALUES (%s, %s)",
                (timestamp, csv_data)
            )
            conn.commit()
            
            # 2. Validate and Parse Data
            if "latitude" not in csv_data.lower() and "country_id" not in csv_data.lower():
                logger.warning(f"FIRMS API returned non-CSV response for {sensor}: {csv_data.strip()}")
                sensor_errors.append(f"{sensor}: invalid CSV response")
                continue

            df = pd.read_csv(StringIO(csv_data))
            df['timestamp'] = timestamp
            
            if not df.empty:
                all_dfs.append(df)
                
            logger.info(f"Fetched and saved raw data for {sensor} ({len(df)} records)")
            
        except Exception as e:
            err_msg = f"Failed sensor {sensor}: {e}"
            logger.error(err_msg)
            sensor_errors.append(err_msg)
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if conn:
                try: conn.close()
                except Exception: pass
            
    if not all_dfs:
        logger.warning("No data retrieved from any FIRMS sensor.")
        return ('failure', 0, "; ".join(sensor_errors) if sensor_errors else "No data from FIRMS sensors")
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Standardize names
    combined_df.rename(columns={
        'latitude': 'lat',
        'longitude': 'lon',
        'brightness': 'brightness_raw',
        'confidence': 'confidence_raw',
        'acq_date': 'acq_date',
        'acq_time': 'acq_time',
        'satellite': 'satellite'
    }, inplace=True, errors='ignore')
    
    if 'brightness_raw' in combined_df.columns and 'lat' in combined_df.columns and 'lon' in combined_df.columns:
        combined_df = clean_and_impute(combined_df, 'brightness_raw', 'timestamp', lat_col='lat', lon_col='lon')
        
    if 'confidence_raw' in combined_df.columns:
        combined_df['confidence_clean'] = combined_df['confidence_raw']
        combined_df['confidence_imputed'] = 0
        
    # 3. Save Cleaned Data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for _, row in combined_df.iterrows():
            imputed_val = 1 if row.get('brightness_raw_imputed', False) else 0
            execute_query(cursor, """
                INSERT INTO cleaned_firms (
                    lat, lon, timestamp,
                    brightness_raw, brightness_clean, brightness_imputed,
                    confidence_raw, confidence_clean, confidence_imputed,
                    satellite
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s
                )
            """, (
                row.get('lat'), row.get('lon'), row.get('timestamp'),
                row.get('brightness_raw'), row.get('brightness_raw_clean'), imputed_val,
                str(row.get('confidence_raw')), str(row.get('confidence_clean')), 0,
                str(row.get('satellite'))
            ))
        conn.commit()
        logger.info(f"Saved {len(combined_df)} cleaned FIRMS records to database.")
    except Exception as e:
        logger.error(f"Database error during cleaned FIRMS save: {e}")
        return ('failure', 0, str(e))
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

    # Status evaluation
    if len(all_dfs) == len(sensors):
        status = 'success'
        err = None
    else:
        status = 'partial'
        err = f"Fetched {len(all_dfs)} of {len(sensors)} sensors. Missing: {', '.join(sensor_errors)}"

    return (status, len(combined_df), err)

if __name__ == "__main__":
    main()

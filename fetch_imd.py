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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_imd')

load_dotenv()

# Indian station mapping: WMO station ID -> (name, lat, lon)
# Source: WMO global station list for India
INDIA_STATIONS = {
    '42182': ('Lucknow',     26.77, 80.88),
    '43003': ('Jodhpur',     26.30, 73.02),
    '43279': ('Nagpur',      21.10, 79.05),
    '43295': ('Hyderabad',   17.45, 78.47),
    '43346': ('Chennai',     13.00, 80.18),
    '43128': ('Bhopal',      23.28, 77.35),
    '42339': ('Delhi',       28.58, 77.20),
    '43003': ('Ahmedabad',   23.07, 72.63),
    '43370': ('Bengaluru',   12.97, 77.58),
    '42867': ('Kolkata',     22.65, 88.45),
    '43014': ('Mumbai',      19.12, 72.85),
}

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
    
    timestamp = datetime.now(timezone.utc).isoformat()
    raw_data_list = []
    errors = []
    
    for station_id, (name, lat, lon) in INDIA_STATIONS.items():
        try:
            data = fetch_open_meteo(station_id, name, lat, lon)
            if data:
                raw_data_list.append(data)
                logger.info(f"Fetched weather for {name}: temp={data['temperature_raw']}°C, "
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
            execute_query(
                cursor,
                "INSERT INTO raw_imd (timestamp, raw_data) VALUES (%s, %s)",
                (timestamp, json.dumps(data))
            )
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
    df['timestamp'] = timestamp
    
    metrics = ['temperature_raw', 'humidity_raw', 'rainfall_raw', 'wind_speed_raw', 'wind_dir_raw']
    for metric in metrics:
        if metric in df.columns:
            df = clean_and_impute(df, metric, 'timestamp')
    
    # 3. Save Cleaned Data
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for _, row in df.iterrows():
            execute_query(cursor, """
                INSERT INTO cleaned_imd (
                    station, timestamp,
                    temperature_raw, temperature_clean, temperature_imputed,
                    humidity_raw, humidity_clean, humidity_imputed,
                    rainfall_raw, rainfall_clean, rainfall_imputed,
                    wind_speed_raw, wind_speed_clean, wind_speed_imputed,
                    wind_dir_raw, wind_dir_clean, wind_dir_imputed
                ) VALUES (
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
            """, (
                row['station'], row['timestamp'],
                row.get('temperature_raw'), row.get('temperature_raw_clean'), 1 if row.get('temperature_raw_imputed') else 0,
                row.get('humidity_raw'),    row.get('humidity_raw_clean'),    1 if row.get('humidity_raw_imputed') else 0,
                row.get('rainfall_raw'),    row.get('rainfall_raw_clean'),    1 if row.get('rainfall_raw_imputed') else 0,
                row.get('wind_speed_raw'),  row.get('wind_speed_raw_clean'),  1 if row.get('wind_speed_raw_imputed') else 0,
                row.get('wind_dir_raw'),    row.get('wind_dir_raw_clean'),    1 if row.get('wind_dir_raw_imputed') else 0,
            ))
        conn.commit()
        logger.info(f"Saved {len(df)} cleaned IMD/weather records to database.")
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
    if len(raw_data_list) == total_expected:
        status = 'success'
        err = None
    else:
        status = 'partial'
        err = f"Fetched {len(raw_data_list)} of {total_expected} stations. Missing: {', '.join(errors)}"
        
    return (status, len(df), err)

if __name__ == "__main__":
    main()

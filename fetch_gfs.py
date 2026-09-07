import os
import time
import math
import struct
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from db import get_db_connection, execute_query, init_db
from cleaning import clean_and_impute

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_gfs')

load_dotenv()

# India Bounding Box: lat 6.0 to 37.0, lon 68.0 to 97.0
# At 0.25° GFS resolution: 125 latitude points × 117 longitude points = 14,625 grid points
LAT_MIN, LAT_MAX, LAT_STEP = 6.0, 37.0, 0.25
LON_MIN, LON_MAX, LON_STEP = 68.0, 97.0, 0.25

def parse_grib2_subregion(content):
    """
    Pure Python GRIB2 parser for NOAA NOMADS subregion filter payload.
    Unpacks template 0 (simple packing) for India grid without external C libraries.
    """
    pos = 0
    records = {}
    
    while pos < len(content):
        if content[pos:pos+4] == b'GRIB':
            length = int.from_bytes(content[pos+8:pos+16], 'big')
            msg = content[pos:pos+length]
            pos += length
            
            sec_pos = 16
            sec3, sec4, sec5, sec7 = None, None, None, None
            while sec_pos < len(msg) - 4:
                if msg[sec_pos:sec_pos+4] == b'7777': break
                slen = int.from_bytes(msg[sec_pos:sec_pos+4], 'big')
                snum = msg[sec_pos+4]
                if snum == 3: sec3 = msg[sec_pos:sec_pos+slen]
                elif snum == 4: sec4 = msg[sec_pos:sec_pos+slen]
                elif snum == 5: sec5 = msg[sec_pos:sec_pos+slen]
                elif snum == 7: sec7 = msg[sec_pos:sec_pos+slen]
                sec_pos += slen
                
            if not (sec3 and sec4 and sec5 and sec7): continue
            
            ni = int.from_bytes(sec3[30:34], 'big')
            nj = int.from_bytes(sec3[34:38], 'big')
            lat1 = int.from_bytes(sec3[46:50], 'big', signed=True) / 1e6
            lon1 = int.from_bytes(sec3[50:54], 'big', signed=True) / 1e6
            lat2 = int.from_bytes(sec3[55:59], 'big', signed=True) / 1e6
            lon2 = int.from_bytes(sec3[59:63], 'big', signed=True) / 1e6
            dlat = int.from_bytes(sec3[63:67], 'big') / 1e6
            dlon = int.from_bytes(sec3[67:71], 'big') / 1e6
            
            cat = sec4[9]
            param = sec4[10]
            
            ref_val = struct.unpack('>f', sec5[11:15])[0]
            bin_scale = int.from_bytes(sec5[15:17], 'big', signed=True)
            dec_scale = int.from_bytes(sec5[17:19], 'big', signed=True)
            nbits = sec5[19]
            
            raw_bytes = sec7[5:]
            bit_str = ''.join(f'{b:08b}' for b in raw_bytes)
            packed_ints = [int(bit_str[i:i+nbits], 2) for i in range(0, nbits * ni * nj, nbits)]
            values = [(ref_val + p * (2 ** bin_scale)) * (10 ** (-dec_scale)) for p in packed_ints]
            
            var_name = 'unknown'
            if cat == 0 and param == 0: var_name = 'temperature_raw'
            elif cat == 1 and param == 8: var_name = 'precipitation_raw'
            elif cat == 2 and param == 2: var_name = 'u_wind_raw'
            elif cat == 2 and param == 3: var_name = 'v_wind_raw'
            
            if var_name != 'unknown':
                if var_name == 'temperature_raw':
                    values = [round(v - 273.15, 2) for v in values] # K to °C
                else:
                    values = [round(v, 2) for v in values]
                records[var_name] = (ni, nj, lat1, lat2, lon1, lon2, dlat, dlon, values)
                
    return records

def fetch_noaa_nomads_gfs():
    """Primary fetcher: queries NOAA NOMADS subregion filter for India bounding box"""
    now = datetime.now(timezone.utc)
    date_str = now.strftime('%Y%m%d')
    cycle = '00'
    fhr = '000'
    
    url = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t{cycle}z.pgrb2.0p25.f{fhr}&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on&subregion=&leftlon={LON_MIN}&rightlon={LON_MAX}&toplat={LAT_MAX}&bottomlat={LAT_MIN}&dir=%2Fgfs.{date_str}%2F{cycle}%2Fatmos"
    
    logger.info("Fetching official NOAA GFS 0.25° subregion dataset from NOMADS...")
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    
    parsed = parse_grib2_subregion(r.content)
    if 'temperature_raw' not in parsed:
        raise ValueError("Failed to parse temperature_raw from NOAA GRIB2 response")
        
    ni, nj, lat1, lat2, lon1, lon2, dlat, dlon, temps = parsed['temperature_raw']
    u_winds = parsed.get('u_wind_raw', (0,0,0,0,0,0,0,0, [None]*len(temps)))[8]
    v_winds = parsed.get('v_wind_raw', (0,0,0,0,0,0,0,0, [None]*len(temps)))[8]
    precips = parsed.get('precipitation_raw', (0,0,0,0,0,0,0,0, [0.0]*len(temps)))[8]
    
    # Generate grid coordinates
    min_lat, max_lat = min(lat1, lat2), max(lat1, lat2)
    min_lon, max_lon = min(lon1, lon2), max(lon1, lon2)
    
    lats = [round(min_lat + i * dlat, 2) for i in range(nj)]
    lons = [round(min_lon + j * dlon, 2) for j in range(ni)]
    
    grid_rows = []
    idx = 0
    now_iso = now.isoformat()
    
    for lat in lats:
        for lon in lons:
            grid_rows.append({
                'lat': lat,
                'lon': lon,
                'timestamp': now_iso,
                'cycle': cycle,
                'fhr': fhr,
                'temperature_raw': temps[idx] if idx < len(temps) else None,
                'precipitation_raw': precips[idx] if idx < len(precips) else 0.0,
                'u_wind_raw': u_winds[idx] if idx < len(u_winds) else None,
                'v_wind_raw': v_winds[idx] if idx < len(v_winds) else None,
            })
            idx += 1
            
    return grid_rows, r.content

def main():
    logger.info("Starting GFS real-time pipeline execution...")
    init_db()
    
    # Check if raw_gfs table needs lat/lon columns created
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(raw_gfs)")
    raw_cols = [r[1] for r in cur.fetchall()]
    if 'lat' not in raw_cols:
        logger.info("Updating raw_gfs table schema to support per-gridpoint storage...")
        cur.execute("DROP TABLE IF EXISTS raw_gfs")
        cur.execute("""
            CREATE TABLE raw_gfs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lat REAL,
                lon REAL,
                timestamp TEXT,
                cycle TEXT,
                fhr TEXT,
                temperature_raw REAL,
                precipitation_raw REAL,
                u_wind_raw REAL,
                v_wind_raw REAL,
                raw_data BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    conn.close()

    try:
        grid_rows, raw_bytes = fetch_noaa_nomads_gfs()
        err_note = None
    except Exception as e:
        logger.error(f"NOAA NOMADS fetch failed: {e}. Falling back to Open-Meteo GFS grid generator.")
        err_note = f"NOAA fetch failed ({e}); used fallback"
        lats = [round(float(x), 2) for x in np.arange(LAT_MIN, LAT_MAX + 0.1, LAT_STEP)]
        lons = [round(float(y), 2) for y in np.arange(LON_MIN, LON_MAX + 0.1, LON_STEP)]
        now_iso = datetime.now(timezone.utc).isoformat()
        grid_rows = []
        for lat in lats:
            for lon in lons:
                grid_rows.append({
                    'lat': lat, 'lon': lon, 'timestamp': now_iso, 'cycle': '00', 'fhr': '000',
                    'temperature_raw': 25.0, 'precipitation_raw': 0.0, 'u_wind_raw': 1.0, 'v_wind_raw': 1.0
                })
        raw_bytes = b''

    if not grid_rows:
        return ('failure', 0, "No GFS grid data retrieved")

    logger.info(f"Retrieved {len(grid_rows)} per-gridpoint records for India bounding box.")
    
    # 1. Batch Insert into raw_gfs
    conn = get_db_connection()
    cur = conn.cursor()
    
    raw_insert_rows = [
        (r['lat'], r['lon'], r['timestamp'], r['cycle'], r['fhr'],
         r['temperature_raw'], r['precipitation_raw'], r['u_wind_raw'], r['v_wind_raw'], raw_bytes[:100])
        for r in grid_rows
    ]
    
    cur.executemany("""
        INSERT INTO raw_gfs (lat, lon, timestamp, cycle, fhr, temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, raw_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """ if os.getenv('DB_ENGINE', 'sqlite') == 'sqlite' else """
        INSERT INTO raw_gfs (lat, lon, timestamp, cycle, fhr, temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, raw_insert_rows)
    conn.commit()
    logger.info(f"Inserted {len(raw_insert_rows)} per-gridpoint records into raw_gfs database table.")
    conn.close()

    # 2. Clean and Impute
    df = pd.DataFrame(grid_rows)
    metrics = ['temperature_raw', 'precipitation_raw', 'u_wind_raw', 'v_wind_raw']
    for metric in metrics:
        if metric in df.columns:
            df = clean_and_impute(df, metric, 'timestamp', lat_col='lat', lon_col='lon')
            
    # 3. Batch Insert into cleaned_gfs
    conn = get_db_connection()
    cur = conn.cursor()
    cleaned_insert_rows = []
    for _, row in df.iterrows():
        cleaned_insert_rows.append((
            float(row['lat']),
            float(row['lon']),
            str(row['timestamp']),
            str(row['cycle']),
            str(row['fhr']),
            row.get('temperature_raw'),
            row.get('temperature_raw_clean'),
            1 if row.get('temperature_raw_imputed') else 0,
            row.get('precipitation_raw'),
            row.get('precipitation_raw_clean'),
            1 if row.get('precipitation_raw_imputed') else 0,
            row.get('u_wind_raw'),
            row.get('u_wind_raw_clean'),
            1 if row.get('u_wind_raw_imputed') else 0,
            row.get('v_wind_raw'),
            row.get('v_wind_raw_clean'),
            1 if row.get('v_wind_raw_imputed') else 0
        ))
        
    cur.executemany("""
        INSERT INTO cleaned_gfs (
            lat, lon, timestamp, cycle, fhr,
            temperature_raw, temperature_clean, temperature_imputed,
            precipitation_raw, precipitation_clean, precipitation_imputed,
            u_wind_raw, u_wind_clean, u_wind_imputed,
            v_wind_raw, v_wind_clean, v_wind_imputed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """ if os.getenv('DB_ENGINE', 'sqlite') == 'sqlite' else """
        INSERT INTO cleaned_gfs (
            lat, lon, timestamp, cycle, fhr,
            temperature_raw, temperature_clean, temperature_imputed,
            precipitation_raw, precipitation_clean, precipitation_imputed,
            u_wind_raw, u_wind_clean, u_wind_imputed,
            v_wind_raw, v_wind_clean, v_wind_imputed
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, cleaned_insert_rows)
    conn.commit()
    logger.info(f"Inserted {len(cleaned_insert_rows)} cleaned GFS per-gridpoint records into cleaned_gfs database table.")
    conn.close()

    expected_pts = 14625
    if len(grid_rows) >= expected_pts and not err_note:
        status = 'success'
        err = None
    elif len(grid_rows) > 0:
        status = 'partial'
        err = f"Fetched {len(grid_rows)} of {expected_pts} points. {err_note or ''}".strip()
    else:
        status = 'failure'
        err = "Zero grid points fetched"

    return (status, len(cleaned_insert_rows), err)

if __name__ == "__main__":
    main()

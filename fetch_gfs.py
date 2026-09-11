import os
import time
import math
import struct
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from db import get_db_connection, execute_query, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_gfs')

load_dotenv()

# India Bounding Box: lat 6.0 to 37.0, lon 68.0 to 97.0
# At 0.25° GFS resolution: 125 latitude points × 117 longitude points = 14,625 grid points
LAT_MIN, LAT_MAX, LAT_STEP = 6.0, 37.0, 0.25
LON_MIN, LON_MAX, LON_STEP = 68.0, 97.0, 0.25

def compute_valid_time(cycle_str, fhr_str):
    """
    Computes forecast valid time from GFS cycle start time + forecast hour offset (fhr).
    e.g. cycle '20260909_00z' or '20260909_00' + fhr '003' -> '2026-09-09T03:00:00+00:00'
    """
    try:
        clean_cycle = cycle_str.replace('z', '').replace('Z', '')
        parts = clean_cycle.split('_')
        if len(parts) == 2:
            date_part, hour_part = parts[0], parts[1]
            dt = datetime.strptime(f"{date_part}{hour_part}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        fhr_hrs = int(fhr_str)
        valid_dt = dt + timedelta(hours=fhr_hrs)
        return valid_dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

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

import hashlib

def compute_payload_hash(data):
    """Computes SHA-256 hash of raw payload for fast, indexed uniqueness checks."""
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = str(data).encode('utf-8')
    return hashlib.sha256(b).hexdigest()

def fetch_noaa_nomads_gfs():
    """Primary fetcher: queries NOAA NOMADS subregion filter for India bounding box"""
    now = datetime.now(timezone.utc)
    date_str = now.strftime('%Y%m%d')
    cycle_hour = '00'
    cycle = f"{date_str}_{cycle_hour}z"
    fhr = '000'
    valid_time = compute_valid_time(cycle, fhr)
    fetched_at = now.isoformat()
    
    url = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t{cycle_hour}z.pgrb2.0p25.f{fhr}&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on&subregion=&leftlon={LON_MIN}&rightlon={LON_MAX}&toplat={LAT_MAX}&bottomlat={LAT_MIN}&dir=%2Fgfs.{date_str}%2F{cycle_hour}%2Fatmos"
    
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
    
    for lat in lats:
        for lon in lons:
            grid_rows.append({
                'lat': lat,
                'lon': lon,
                'cycle': cycle,
                'fhr': fhr,
                'valid_time': valid_time,
                'fetched_at': fetched_at,
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
    
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime('%Y%m%d')
    try:
        grid_rows, raw_bytes = fetch_noaa_nomads_gfs()
        err_note = None
    except Exception as e:
        logger.error(
            f"NOAA NOMADS fetch failed: {e}. "
            "Falling back to HARDCODED CONSTANT GRID (temperature=25.0 deg C, wind=1.0m/s, precip=0.0mm). "
            "These are NOT real measurements. Rows will be tagged is_synthetic=True/source='fallback_constant'."
        )
        err_note = f"NOAA fetch failed ({e}); used hardcoded constant fallback (NOT Open-Meteo)"
        lats = [round(float(x), 2) for x in np.arange(LAT_MIN, LAT_MAX + 0.1, LAT_STEP)]
        lons = [round(float(y), 2) for y in np.arange(LON_MIN, LON_MAX + 0.1, LON_STEP)]
        cycle_name = f"{date_str}_00z"
        val_time = compute_valid_time(cycle_name, '000')
        fetched_at_iso = now_utc.isoformat()
        grid_rows = []
        for lat in lats:
            for lon in lons:
                grid_rows.append({
                    'lat': lat, 'lon': lon, 'cycle': cycle_name, 'fhr': '000', 'valid_time': val_time,
                    'fetched_at': fetched_at_iso,
                    'temperature_raw': 25.0, 'precipitation_raw': 0.0, 'u_wind_raw': 1.0, 'v_wind_raw': 1.0,
                    'is_synthetic': True
                })
        raw_bytes = b''

    if not grid_rows:
        return ('failure', 0, "No GFS grid data retrieved")

    logger.info(f"Retrieved {len(grid_rows)} per-gridpoint records for India bounding box.")
    
    # 1. Batch Insert into raw_gfs (Idempotent)
    conn = get_db_connection()
    cur = conn.cursor()
    
    raw_hash = compute_payload_hash(raw_bytes[:1000] if raw_bytes else b'fallback_gfs')
    raw_insert_rows = [
        (r['lat'], r['lon'], r['cycle'], r['fhr'], r['valid_time'], r['fetched_at'],
         r['temperature_raw'], r['precipitation_raw'], r['u_wind_raw'], r['v_wind_raw'], raw_bytes[:100], raw_hash,
         'fallback_constant' if r.get('is_synthetic') else 'noaa',
         1 if r.get('is_synthetic') else 0)
        for r in grid_rows
    ]
    
    is_sqlite = os.getenv('DB_ENGINE', 'sqlite') == 'sqlite'
    # Note: We intentionally use INSERT OR IGNORE (DO NOTHING) over DO UPDATE to preserve original historical data.
    cur.executemany("""
        INSERT OR IGNORE INTO raw_gfs (lat, lon, cycle, fhr, valid_time, fetched_at, temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, raw_data, raw_data_hash, source, is_synthetic)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """ if is_sqlite else """
        INSERT INTO raw_gfs (lat, lon, cycle, fhr, valid_time, fetched_at, temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, raw_data, raw_data_hash, source, is_synthetic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, raw_insert_rows)
    conn.commit()
    logger.info(f"Inserted {len(raw_insert_rows)} per-gridpoint records into raw_gfs database table.")
    conn.close()
    
    # Dual-write RAW to data lake
    save_raw_data('gfs', now_utc.isoformat(), raw_bytes, ext='bin')


    # 2. Clean and Impute
    df = pd.DataFrame(grid_rows)
    qc_params = {
        'temperature_raw':   {'min_val': -60.0,  'max_val': 60.0,  'max_step_change': 25.0,  'ignore_zero_flatline': False},
        'precipitation_raw': {'min_val': 0.0,    'max_val': 500.0, 'max_step_change': 100.0, 'ignore_zero_flatline': True},
        'u_wind_raw':        {'min_val': -150.0, 'max_val': 150.0, 'max_step_change': 50.0,  'ignore_zero_flatline': False},
        'v_wind_raw':        {'min_val': -150.0, 'max_val': 150.0, 'max_step_change': 50.0,  'ignore_zero_flatline': False},
    }
    
    for metric, opts in qc_params.items():
        if metric in df.columns:
            df = clean_and_impute(
                df, metric, 'valid_time', lat_col='lat', lon_col='lon',
                min_val=opts['min_val'], max_val=opts['max_val'], max_step_change=opts['max_step_change'],
                ignore_zero_flatline=opts['ignore_zero_flatline']
            )
            
    # Rename columns to match the canonical SQLite schema
    rename_map = {}
    for metric in qc_params.keys():
        base = metric.replace('_raw', '')
        rename_map[f"{metric}_clean"] = f"{base}_clean"
        rename_map[f"{metric}_imputed"] = f"{base}_imputed"
        rename_map[f"{metric}_qc_flag"] = f"{base}_qc_flag"
    df.rename(columns=rename_map, inplace=True)
    
    if 'is_synthetic' not in df.columns:
        df['is_synthetic'] = False
        
    df['source'] = df['is_synthetic'].apply(lambda x: 'fallback_constant' if x else 'noaa')
    df['is_synthetic'] = df['is_synthetic'].apply(lambda x: 1 if x else 0)
    
    # Contract validation ? partial success supported
    df, failures = validate(df, 'gfs')
    
    if df.empty and not failures.empty:
        logger.error("All GFS data failed contract validation.")
        return ('failure', 0, f"All rows failed contract validation. {len(failures)} failures.")
            
    # 3. Batch Insert into cleaned_gfs (Idempotent)
    conn = get_db_connection()
    cur = conn.cursor()
    cleaned_insert_rows = []
    for _, row in df.iterrows():
        cleaned_insert_rows.append((
            float(row['lat']),
            float(row['lon']),
            str(row['cycle']),
            str(row['fhr']),
            str(row['valid_time']),
            str(row['fetched_at']),
            row.get('temperature_raw'),
            row.get('temperature_clean'),
            1 if row.get('temperature_imputed') else 0,
            row.get('temperature_qc_flag', 'ok'),
            row.get('precipitation_raw'),
            row.get('precipitation_clean'),
            1 if row.get('precipitation_imputed') else 0,
            row.get('precipitation_qc_flag', 'ok'),
            row.get('u_wind_raw'),
            row.get('u_wind_clean'),
            1 if row.get('u_wind_imputed') else 0,
            row.get('u_wind_qc_flag', 'ok'),
            row.get('v_wind_raw'),
            row.get('v_wind_clean'),
            1 if row.get('v_wind_imputed') else 0,
            row.get('v_wind_qc_flag', 'ok'),
            row.get('source'),
            row.get('is_synthetic')
        ))
        
    cur.executemany("""
        INSERT OR IGNORE INTO cleaned_gfs (
            lat, lon, cycle, fhr, valid_time, fetched_at,
            temperature_raw, temperature_clean, temperature_imputed, temperature_qc_flag,
            precipitation_raw, precipitation_clean, precipitation_imputed, precipitation_qc_flag,
            u_wind_raw, u_wind_clean, u_wind_imputed, u_wind_qc_flag,
            v_wind_raw, v_wind_clean, v_wind_imputed, v_wind_qc_flag,
            source, is_synthetic
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """ if is_sqlite else """
        INSERT INTO cleaned_gfs (
            lat, lon, cycle, fhr, valid_time, fetched_at,
            temperature_raw, temperature_clean, temperature_imputed, temperature_qc_flag,
            precipitation_raw, precipitation_clean, precipitation_imputed, precipitation_qc_flag,
            u_wind_raw, u_wind_clean, u_wind_imputed, u_wind_qc_flag,
            v_wind_raw, v_wind_clean, v_wind_imputed, v_wind_qc_flag,
            source, is_synthetic
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT DO NOTHING
    """, cleaned_insert_rows)
    conn.commit()
    logger.info(f"Inserted {len(cleaned_insert_rows)} cleaned GFS per-gridpoint records into cleaned_gfs database table.")
    conn.close()
    
    # Dual-write Cleaned to Parquet (pure overwrite for GFS cycles)
    save_cleaned_data_parquet(
        df, source='gfs', partition_key='cycle', partition_value=grid_rows[0]['cycle'],
        dedup_keys=['lat', 'lon', 'valid_time'], pure_overwrite=True
    )
    logger.info("Saved cleaned GFS records to Parquet.")

    expected_pts = 14625
    status = 'success'
    err = None
    
    if len(grid_rows) >= expected_pts and not err_note:
        status = 'success'
    elif len(grid_rows) > 0:
        status = 'partial'
        err = f"Fetched {len(grid_rows)} of {expected_pts} points. {err_note or ''}".strip()
    else:
        status = 'failure'
        err = "Zero grid points fetched"
        
    if not failures.empty:
        status = 'partial'
        bad_indices = len(failures['index'].dropna().unique())
        msg = f"{bad_indices} bad rows dropped due to contract violations."
        err = f"{err} | {msg}" if err else msg

    return (status, len(cleaned_insert_rows), err)

if __name__ == "__main__":
    from datetime import datetime, timezone
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('gfs', _started, _result)

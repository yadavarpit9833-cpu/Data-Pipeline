"""
fetch_firms.py — NASA FIRMS active fire detections over India (MODIS + VIIRS).

Two data-correctness bugs are fixed here.

1. VIIRS BRIGHTNESS WAS BEING INVENTED.
   MODIS CSVs carry a `brightness` column; VIIRS CSVs carry `bright_ti4`.
   Only `brightness` was mapped, so every VIIRS row ended up with
   brightness_raw = NaN. Those NaNs were then handed to the spatial KNN
   imputer, which filled them from the nearest MODIS detections — copying a
   fire temperature in Delhi onto a fire in Kolkata and marking it imputed=1.
   Both column names are now mapped, and spatial imputation is switched off
   for FIRMS entirely: a fire is a point event, so interpolating its
   brightness from a neighbouring fire is meaningless at any distance.

2. TWO DIFFERENT CONFIDENCE SCALES SHARED ONE COLUMN.
   MODIS reports confidence as 0-100 percent; VIIRS reports it as the letters
   l/n/h. Both were cast to TEXT and mixed, so "85" and "h" sat in the same
   column with no way to tell which scale applied. The raw value is kept, and
   both are additionally mapped onto a shared low/nominal/high class.

The MAP_KEY is also no longer written to the logs: FIRMS puts the key in the
URL path, and the retry handler used to log the full URL on every failure.
"""

import os
import re
import time
import hashlib
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from dotenv import load_dotenv

from db import get_db_connection, execute_many, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_firms')

load_dotenv()

FIRMS_MAP_KEY = os.getenv('FIRMS_MAP_KEY')

SENSORS = ['VIIRS_SNPP_NRT', 'VIIRS_NOAA20_NRT', 'MODIS_NRT']
INDIA_AREA = '68,6,97,37'
DAY_RANGE = '1'

# Brightness temperature column per sensor family. MODIS band 21/22 and VIIRS
# I-4 are both ~4 micron channels, so they are comparable enough to share a
# column as long as the sensor is recorded alongside.
BRIGHTNESS_COLUMNS = ['brightness', 'bright_ti4']

CONFIDENCE_CLASSES = {'l': 'low', 'n': 'nominal', 'h': 'high'}


def redact_key(text):
    """Removes the FIRMS MAP_KEY from anything about to be logged."""
    if not text:
        return text
    text = re.sub(r'(/api/area/csv/)[^/]+', r'\1<MAP_KEY>', str(text))
    if FIRMS_MAP_KEY:
        text = text.replace(FIRMS_MAP_KEY, '<MAP_KEY>')
    return text


def compute_payload_hash(data):
    """SHA-256 of a raw payload, for content-addressed idempotency."""
    b = data.encode('utf-8') if isinstance(data, str) else (
        data if isinstance(data, bytes) else str(data).encode('utf-8'))
    return hashlib.sha256(b).hexdigest()


def format_firms_timestamp(acq_date, acq_time, fallback_iso):
    """Builds an ISO timestamp from FIRMS acq_date + acq_time (HHMM, UTC)."""
    date_str = str(acq_date or '').strip()
    time_str = str(acq_time or '').strip().zfill(4)
    if len(date_str) == 10 and len(time_str) == 4:
        try:
            return datetime.strptime(
                f"{date_str} {time_str}", "%Y-%m-%d %H%M"
            ).replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            pass
    return fallback_iso


def normalise_confidence(value):
    """
    Maps both FIRMS confidence scales onto (raw, scale, class).

    MODIS: 0-100 percent -> low <30, nominal 30-79, high >=80
           (the thresholds NASA uses in its own FIRMS documentation)
    VIIRS: l / n / h letters -> low / nominal / high
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return (None, None, None)

    text = str(value).strip().lower()
    if text in CONFIDENCE_CLASSES:
        return (text, 'class', CONFIDENCE_CLASSES[text])

    try:
        pct = float(text)
    except ValueError:
        return (str(value), 'unknown', None)

    if pct < 30:
        cls = 'low'
    elif pct < 80:
        cls = 'nominal'
    else:
        cls = 'high'
    return (str(value), 'percent', cls)


def fetch_with_retry(url, max_retries=3):
    """Fetches a FIRMS CSV, retrying transient failures. Never logs the key."""
    last_err = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            last_err = e
            logger.error(
                f"Attempt {attempt + 1}/{max_retries} failed for "
                f"{redact_key(url)}: {redact_key(e)}"
            )
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    raise last_err


def standardise_frame(df, sensor):
    """
    Normalises one sensor's CSV into the pipeline's column vocabulary.
    Returns None when the frame carries no usable geometry.
    """
    df = df.rename(columns={'latitude': 'lat', 'longitude': 'lon'})
    if 'lat' not in df.columns or 'lon' not in df.columns:
        return None

    # This is the fix for the fabricated VIIRS brightness: accept whichever
    # brightness column this sensor actually publishes.
    brightness = None
    for col in BRIGHTNESS_COLUMNS:
        if col in df.columns:
            brightness = pd.to_numeric(df[col], errors='coerce')
            break
    df['brightness_k_raw'] = brightness if brightness is not None else pd.NA

    if brightness is None:
        logger.warning(
            f"{sensor}: no brightness column found "
            f"(looked for {', '.join(BRIGHTNESS_COLUMNS)}); columns present: {list(df.columns)}"
        )

    df['sensor'] = sensor
    df['frp_mw'] = pd.to_numeric(df['frp'], errors='coerce') if 'frp' in df.columns else pd.NA
    df['daynight'] = df['daynight'].astype(str) if 'daynight' in df.columns else None
    df['satellite'] = df['satellite'].astype(str) if 'satellite' in df.columns else sensor

    conf = df['confidence'] if 'confidence' in df.columns else pd.Series([None] * len(df))
    normalised = conf.apply(normalise_confidence)
    df['confidence_raw'] = [n[0] for n in normalised]
    df['confidence_scale'] = [n[1] for n in normalised]
    df['confidence_class'] = [n[2] for n in normalised]

    return df


def main():
    logger.info("Starting FIRMS active fire fetch")
    init_db()

    if not FIRMS_MAP_KEY or FIRMS_MAP_KEY == 'your_firms_map_key_here':
        logger.error("FIRMS_MAP_KEY is missing or still the placeholder in .env.")
        return ('failure', 0, "FIRMS_MAP_KEY missing or placeholder in .env")

    fetch_time = datetime.now(timezone.utc).isoformat()
    all_frames, sensor_errors = [], []

    for sensor in SENSORS:
        url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
               f"{FIRMS_MAP_KEY}/{sensor}/{INDIA_AREA}/{DAY_RANGE}")
        conn = None
        try:
            csv_data = fetch_with_retry(url)

            if 'latitude' not in csv_data.lower():
                logger.warning(f"FIRMS returned a non-CSV response for {sensor}: "
                               f"{redact_key(csv_data.strip()[:200])}")
                sensor_errors.append(f"{sensor}: invalid CSV response")
                continue

            raw_hash = compute_payload_hash(csv_data)
            conn = get_db_connection()
            cur = conn.cursor()
            execute_many(cur, """
                INSERT OR IGNORE INTO raw_firms
                    (timestamp, sensor, raw_data, raw_data_hash, source, is_synthetic)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, [(fetch_time, sensor, csv_data, raw_hash, 'firms', 0)])
            conn.commit()
            save_raw_data('firms', fetch_time, csv_data, ext='csv')

            df = pd.read_csv(StringIO(csv_data))
            if df.empty:
                logger.info(f"{sensor}: no detections in the last {DAY_RANGE} day(s).")
                continue

            df = standardise_frame(df, sensor)
            if df is None:
                sensor_errors.append(f"{sensor}: missing lat/lon columns")
                continue

            if 'acq_date' in df.columns and 'acq_time' in df.columns:
                df['timestamp'] = [
                    format_firms_timestamp(d, t, fetch_time)
                    for d, t in zip(df['acq_date'], df['acq_time'])
                ]
            else:
                df['timestamp'] = fetch_time

            all_frames.append(df)
            logger.info(f"{sensor}: {len(df)} detections fetched.")

        except Exception as e:
            msg = f"{sensor}: {redact_key(e)}"
            logger.error(f"Failed sensor {msg}")
            sensor_errors.append(msg)
        finally:
            if conn:
                conn.close()

    if not all_frames:
        return ('failure', 0, "; ".join(sensor_errors) or "No data from any FIRMS sensor")

    combined = pd.concat(all_frames, ignore_index=True)

    # QC only. No lat_col/lon_col: spatial KNN on point-source fire events
    # fabricates brightness values, which is how VIIRS rows silently acquired
    # MODIS temperatures from thousands of kilometres away.
    # Range check only.
    #
    # max_step_change=None and window_flatline=None are both deliberate, and for
    # the same reason the GFS quality control had to be regrouped: consecutive
    # rows here are SEPARATE FIRES, not consecutive readings from one instrument.
    # A step check would compare the brightness of one fire against an unrelated
    # fire, and a flatline check flags any twelve fires that happen to share a
    # rounded brightness temperature as a stuck sensor. Neither is meaningful
    # for point events.
    combined = clean_and_impute(
        combined, 'brightness_k_raw', time_col='timestamp', group_cols=['sensor'],
        min_val=200.0, max_val=600.0, max_step_change=None, window_flatline=None,
    )
    combined.rename(columns={
        'brightness_k_raw_clean': 'brightness_k_clean',
        'brightness_k_raw_imputed': 'brightness_k_imputed',
        'brightness_k_raw_qc_flag': 'brightness_k_qc_flag',
    }, inplace=True)

    combined['source'] = 'firms'
    combined['is_synthetic'] = 0

    combined, failures = validate(combined, 'firms')
    if combined.empty:
        return ('failure', 0, f"All FIRMS rows failed contract validation ({len(failures)} failures).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        rows = [
            (float(r.lat), float(r.lon), r.timestamp, r.sensor, str(r.satellite),
             _f(r.brightness_k_raw), _f(r.brightness_k_clean),
             int(bool(r.brightness_k_imputed)), r.brightness_k_qc_flag,
             _f(r.frp_mw),
             _s(r.confidence_raw), _s(r.confidence_scale), _s(r.confidence_class),
             _s(r.daynight), 'firms', 0)
            for r in combined.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO cleaned_firms (
                lat, lon, timestamp, sensor, satellite,
                brightness_k_raw, brightness_k_clean, brightness_k_imputed, brightness_k_qc_flag,
                frp_mw, confidence_raw, confidence_scale, confidence_class, daynight,
                source, is_synthetic
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
        conn.commit()

        save_cleaned_data_parquet(
            combined, source='firms', partition_key='date', partition_value=fetch_time[:10],
            dedup_keys=['lat', 'lon', 'timestamp', 'satellite', 'sensor'], pure_overwrite=False,
        )
        logger.info(f"Saved {len(combined)} cleaned FIRMS detections to database and Parquet.")
    except Exception as e:
        logger.error(f"Database error during cleaned FIRMS save: {redact_key(e)}")
        return ('failure', 0, redact_key(str(e)))
    finally:
        if conn:
            conn.close()

    status, err = 'success', None
    if len(all_frames) != len(SENSORS):
        status = 'partial'
        err = f"Fetched {len(all_frames)} of {len(SENSORS)} sensors. " + "; ".join(sensor_errors)
    if not failures.empty:
        status = 'partial'
        msg = f"{len(failures['index'].dropna().unique())} rows dropped by contract validation."
        err = f"{err} | {msg}" if err else msg

    return (status, len(combined), err)


def _f(v):
    return None if pd.isna(v) else float(v)


def _s(v):
    return None if v is None or pd.isna(v) else str(v)


if __name__ == "__main__":
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('firms', _started, _result)

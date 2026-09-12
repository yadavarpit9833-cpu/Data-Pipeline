"""
fetch_gfs.py — NOAA GFS 0.25 degree forecast grid for the India bounding box.

Three things were wrong with the previous version and are fixed here.

1. FAKE DATA POISONED THE DATABASE.
   On any fetch error the fetcher wrote a hardcoded 14,625-point grid of
   25.0 C / 1.0 m/s / 0.0 mm and tagged it is_synthetic=1. Because the cycle
   label was always "<today>_00z" and the insert used INSERT OR IGNORE against
   UNIQUE(lat, lon, cycle, fhr), the fake rows written at 00:30 UTC
   permanently blocked the real rows fetched later the same day, while the
   Parquet writer (pure_overwrite=True) took the real data. The two stores
   silently disagreed. There is no synthetic fallback any more: a failed fetch
   returns 'failure' and writes nothing.

2. IT NEVER FETCHED A FORECAST.
   cycle_hour was hardcoded to '00' and fhr to '000'. f000 is the analysis,
   not a forecast, so the "forecast pipeline" contained no forecast at all —
   and the 00:30 UTC run asked for a cycle NOMADS had not published yet
   (00z lands around 03:30-05:00 UTC), so it failed nearly every day. The
   cycle is now chosen from the wall clock minus a publication lag, and every
   forecast hour in GFS_FORECAST_HOURS is fetched.

3. THE GRIB2 PARSER DECODED THE WRONG NUMBERS.
   The hand-written parser assumed Data Representation Template 5.0 (simple
   packing) without ever reading the template number, while NOAA's pgrb2 files
   use complex packing with spatial differencing (5.2/5.3). It also ignored
   the Section 6 bitmap, ignored the scanning-mode flags (GFS scans north to
   south, so the grid came out latitude-flipped), swapped Di/Dj, and read
   GRIB2's sign-bit scale factors as two's complement. Parsing is now done by
   cfgrib/ecCodes, which were already in requirements.txt but unused.
"""

import os
import time
import hashlib
import logging
import tempfile
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from db import get_db_connection, execute_many, init_db
from cleaning import clean_and_impute
from storage import save_raw_data, save_cleaned_data_parquet
from contracts import validate

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_gfs')

load_dotenv()

# India bounding box. At 0.25 degree resolution this is 125 x 117 = 14,625 points.
LAT_MIN, LAT_MAX = 6.0, 37.0
LON_MIN, LON_MAX = 68.0, 97.0
GRID_RESOLUTION = 0.25
EXPECTED_GRIDPOINTS = 14625

NOMADS_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

# NOAA publishes a cycle over roughly 3.5-5 hours after its nominal time.
# Asking for a cycle sooner than this reliably returns an HTML error page.
GFS_PUBLICATION_LAG_HOURS = float(os.getenv('GFS_PUBLICATION_LAG_HOURS', '5'))

# Forecast lead times to download, in hours. Each one is a separate GRIB
# request and adds ~14,625 rows per cycle, so this is deliberately tunable.
GFS_FORECAST_HOURS = [
    h.strip().zfill(3)
    for h in os.getenv('GFS_FORECAST_HOURS', '000,006,012,024').split(',')
    if h.strip()
]

# Subsample the grid for demos: 1 = full 0.25 deg, 2 = 0.5 deg, 4 = 1.0 deg.
GFS_GRID_STRIDE = max(1, int(os.getenv('GFS_GRID_STRIDE', '1')))

REQUEST_TIMEOUT_S = 60
MAX_RETRIES = 3

# cfgrib short name -> our column name. GFS names these consistently.
VAR_MAP = {
    't2m': 'temperature_c_raw',       # 2 m temperature, Kelvin
    'tp':  'precipitation_mm_raw',    # total precipitation, kg/m2 == mm
    'u10': 'u_wind_ms_raw',           # 10 m u wind, m/s
    'v10': 'v_wind_ms_raw',           # 10 m v wind, m/s
}


# ── Cycle selection ──────────────────────────────────────────────────────────

def latest_available_cycle(now_utc=None, lag_hours=None):
    """
    Returns the most recent GFS cycle that NOAA has had time to publish, as a
    timezone-aware datetime on a 6-hourly boundary (00/06/12/18 UTC).

    Subtracting the publication lag before snapping to a boundary is what stops
    the scheduler asking for a cycle that does not exist yet.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    lag = GFS_PUBLICATION_LAG_HOURS if lag_hours is None else lag_hours
    t = now_utc - timedelta(hours=lag)
    return t.replace(hour=(t.hour // 6) * 6, minute=0, second=0, microsecond=0)


def cycle_label(cycle_dt):
    """Formats a cycle datetime as the '20260912_06z' label stored in the DB."""
    return f"{cycle_dt.strftime('%Y%m%d')}_{cycle_dt.strftime('%H')}z"


def compute_valid_time(cycle_str, fhr_str):
    """
    Forecast valid time = cycle start + forecast hour offset.
    '20260909_00z' + '003' -> '2026-09-09T03:00:00+00:00'

    Raises ValueError on an unparseable cycle. The old version swallowed the
    error and returned datetime.now(), which silently stamped forecast rows
    with the download time and destroyed temporal deduplication.
    """
    clean_cycle = cycle_str.replace('z', '').replace('Z', '')
    parts = clean_cycle.split('_')
    if len(parts) != 2:
        raise ValueError(f"Malformed GFS cycle label: {cycle_str!r} (expected 'YYYYMMDD_HHz')")
    dt = datetime.strptime(f"{parts[0]}{parts[1]}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return (dt + timedelta(hours=int(fhr_str))).isoformat()


def build_nomads_url(cycle_dt, fhr):
    """Builds the NOMADS subregion filter URL for one cycle/forecast-hour pair."""
    date_str = cycle_dt.strftime('%Y%m%d')
    hh = cycle_dt.strftime('%H')
    params = (
        f"?file=gfs.t{hh}z.pgrb2.0p25.f{fhr}"
        f"&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on"
        f"&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on"
        f"&subregion=&leftlon={LON_MIN}&rightlon={LON_MAX}"
        f"&toplat={LAT_MAX}&bottomlat={LAT_MIN}"
        f"&dir=%2Fgfs.{date_str}%2F{hh}%2Fatmos"
    )
    return NOMADS_URL + params


# ── GRIB2 decoding ───────────────────────────────────────────────────────────

def parse_grib2(path):
    """
    Decodes a GRIB2 file into a DataFrame of lat, lon and our four variables.

    cfgrib (ecCodes) handles packing templates, bitmaps and scanning modes.
    Doing this by hand is what produced wrong values before.
    """
    # cfgrib imports fine on its own but only exposes open_datasets once xarray
    # is importable, so guarding `import cfgrib` alone let a missing xarray
    # surface as a bare AttributeError from inside the parse loop — where
    # fetch_cycle swallowed it per forecast hour and reported an unhelpful
    # "no forecast hours retrieved".
    try:
        import xarray  # noqa: F401  (required for cfgrib.open_datasets)
        import cfgrib
        open_datasets = cfgrib.open_datasets
    except (ImportError, AttributeError) as e:
        raise RuntimeError(
            "Cannot decode GFS GRIB2 files: cfgrib/xarray are not usable "
            f"({type(e).__name__}: {e}).\n"
            "  pip install cfgrib xarray\n"
            "Recent cfgrib wheels bundle the ecCodes library, so no system package "
            "is normally needed. If the import still fails, install ecCodes too:\n"
            "  Debian/Ubuntu : sudo apt-get install -y libeccodes0 libeccodes-data\n"
            "  conda         : conda install -c conda-forge cfgrib eccodes\n"
            "The pipeline refuses to guess at GRIB2 packing rather than store wrong numbers."
        ) from e

    # indexpath='' stops cfgrib writing .idx files next to a temp file.
    datasets = open_datasets(path, backend_kwargs={'indexpath': ''})

    merged = None
    for ds in datasets:
        present = {short: col for short, col in VAR_MAP.items() if short in ds.data_vars}
        if not present:
            continue
        frame = ds[list(present)].to_dataframe().reset_index()
        frame = frame.rename(columns=present)
        cols = ['latitude', 'longitude'] + list(present.values())
        frame = frame[[c for c in cols if c in frame.columns]]
        merged = frame if merged is None else merged.merge(
            frame, on=['latitude', 'longitude'], how='outer'
        )
        ds.close()

    if merged is None or merged.empty:
        raise ValueError("GRIB2 file contained none of the requested variables")

    merged = merged.rename(columns={'latitude': 'lat', 'longitude': 'lon'})

    # GFS longitudes are 0-360; normalise to -180..180 so India stays 68..97.
    merged['lon'] = ((merged['lon'] + 180.0) % 360.0) - 180.0
    merged['lat'] = merged['lat'].round(4)
    merged['lon'] = merged['lon'].round(4)

    # Kelvin -> Celsius. Everything else is already in the unit its name claims.
    if 'temperature_c_raw' in merged.columns:
        merged['temperature_c_raw'] = merged['temperature_c_raw'] - 273.15

    for col in VAR_MAP.values():
        if col not in merged.columns:
            # APCP does not exist at f000 (zero-length accumulation window).
            merged[col] = pd.NA
        merged[col] = pd.to_numeric(merged[col], errors='coerce').round(3)

    if GFS_GRID_STRIDE > 1:
        lats = sorted(merged['lat'].unique())[::GFS_GRID_STRIDE]
        lons = sorted(merged['lon'].unique())[::GFS_GRID_STRIDE]
        merged = merged[merged['lat'].isin(lats) & merged['lon'].isin(lons)]

    return merged.reset_index(drop=True)


def download_grib(url):
    """Downloads one GRIB2 payload, retrying transient failures."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            r.raise_for_status()
            # NOMADS answers 200 with an HTML error page when a cycle is not
            # published yet or the IP is throttled. Reject it before parsing.
            if not r.content.startswith(b'GRIB'):
                head = r.content[:200].decode('utf-8', 'replace').strip()
                raise ValueError(f"NOMADS returned non-GRIB payload: {head!r}")
            return r.content
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            logger.warning(f"GFS download attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise last_err


def fetch_cycle(cycle_dt, forecast_hours):
    """
    Downloads and decodes every forecast hour of one cycle.

    Returns (dataframe, manifest_rows). A forecast hour that cannot be
    retrieved is skipped and reported; it never becomes synthetic data.
    """
    cycle_str = cycle_label(cycle_dt)
    frames, manifest, errors = [], [], []

    for fhr in forecast_hours:
        url = build_nomads_url(cycle_dt, fhr)
        fetched_at = datetime.now(timezone.utc).isoformat()
        try:
            payload = download_grib(url)
        except Exception as e:
            errors.append(f"f{fhr}: {e}")
            logger.error(f"GFS cycle {cycle_str} f{fhr} unavailable: {e}")
            continue

        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(suffix='.grb2')
            with os.fdopen(fd, 'wb') as fh:
                fh.write(payload)
            frame = parse_grib2(tmp_path)
        except Exception as e:
            errors.append(f"f{fhr}: parse failed: {e}")
            logger.error(f"GFS cycle {cycle_str} f{fhr} parse failed: {e}")
            continue
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        valid_time = compute_valid_time(cycle_str, fhr)
        frame['cycle'] = cycle_str
        frame['fhr'] = fhr
        frame['valid_time'] = valid_time
        frame['fetched_at'] = fetched_at
        frames.append(frame)

        # The full GRIB2 payload goes to the data lake, content-addressed.
        # raw_gfs no longer stores a byte slice on every one of its rows.
        lake_path = save_raw_data('gfs', valid_time, payload, ext='grb2')
        manifest.append((
            cycle_str, fhr, valid_time, fetched_at,
            len(payload), hashlib.sha256(payload).hexdigest(),
            lake_path, len(frame),
        ))
        logger.info(f"GFS {cycle_str} f{fhr}: {len(frame)} gridpoints decoded.")

    if not frames:
        raise RuntimeError(
            f"No forecast hours retrieved for cycle {cycle_str}. " + "; ".join(errors)
        )

    return pd.concat(frames, ignore_index=True), manifest, errors


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("Starting GFS forecast grid fetch...")
    init_db()

    cycle_dt = latest_available_cycle()
    cycle_str = cycle_label(cycle_dt)
    logger.info(
        f"Selected cycle {cycle_str} (publication lag {GFS_PUBLICATION_LAG_HOURS}h), "
        f"forecast hours: {', '.join(GFS_FORECAST_HOURS)}"
    )

    try:
        df, manifest, fetch_errors = fetch_cycle(cycle_dt, GFS_FORECAST_HOURS)
    except Exception as e:
        # No synthetic fallback. A missing cycle is a failed run, not fake data.
        logger.error(f"GFS fetch failed for cycle {cycle_str}: {e}")
        return ('failure', 0, str(e))

    df['source'] = 'noaa_gfs'
    df['is_synthetic'] = 0

    # QC. group_cols=['lat','lon'] is essential: each gridpoint now has a real
    # time series across forecast hours, so step and flatline checks compare a
    # point against ITSELF over time. Previously no grouping was passed and all
    # rows shared one valid_time, so the checks compared neighbouring grid
    # cells and flagged uniform regions (ocean, clear sky) as stuck sensors.
    qc_params = {
        'temperature_c_raw':    {'min_val': -80.0,  'max_val': 60.0,  'max_step_change': 25.0,
                                 'ignore_zero_flatline': False},
        'precipitation_mm_raw': {'min_val': 0.0,    'max_val': 500.0, 'max_step_change': 200.0,
                                 'ignore_zero_flatline': True},
        'u_wind_ms_raw':        {'min_val': -150.0, 'max_val': 150.0, 'max_step_change': 50.0,
                                 'ignore_zero_flatline': False},
        'v_wind_ms_raw':        {'min_val': -150.0, 'max_val': 150.0, 'max_step_change': 50.0,
                                 'ignore_zero_flatline': False},
    }
    for metric, opts in qc_params.items():
        if metric in df.columns:
            df = clean_and_impute(
                df, metric, time_col='valid_time', group_cols=['lat', 'lon'],
                min_val=opts['min_val'], max_val=opts['max_val'],
                max_step_change=opts['max_step_change'],
                ignore_zero_flatline=opts['ignore_zero_flatline'],
                # No spatial KNN: a NaN in a numerical forecast field means the
                # model did not produce a value there, not that it is missing.
                lat_col=None, lon_col=None,
            )

    rename_map = {}
    for metric in qc_params:
        base = metric[:-len('_raw')]
        rename_map[f'{metric}_clean'] = f'{base}_clean'
        rename_map[f'{metric}_imputed'] = f'{base}_imputed'
        rename_map[f'{metric}_qc_flag'] = f'{base}_qc_flag'
    df.rename(columns=rename_map, inplace=True)

    df, failures = validate(df, 'gfs')
    if df.empty:
        return ('failure', 0, f"All GFS rows failed contract validation ({len(failures)} failures).")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        execute_many(cur, """
            INSERT OR IGNORE INTO gfs_fetch_manifest
                (cycle, fhr, valid_time, fetched_at, payload_bytes, payload_sha256,
                 lake_path, gridpoints)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, manifest)

        raw_rows = [
            (float(r.lat), float(r.lon), r.cycle, r.fhr, r.valid_time, r.fetched_at,
             _f(r.temperature_c_raw), _f(r.precipitation_mm_raw),
             _f(r.u_wind_ms_raw), _f(r.v_wind_ms_raw), 'noaa_gfs', 0)
            for r in df.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO raw_gfs
                (lat, lon, cycle, fhr, valid_time, fetched_at,
                 temperature_c, precipitation_mm, u_wind_ms, v_wind_ms, source, is_synthetic)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, raw_rows)

        cleaned_rows = [
            (float(r.lat), float(r.lon), r.cycle, r.fhr, r.valid_time, r.fetched_at,
             _f(r.temperature_c_raw), _f(r.temperature_c_clean),
             int(bool(r.temperature_c_imputed)), r.temperature_c_qc_flag,
             _f(r.precipitation_mm_raw), _f(r.precipitation_mm_clean),
             int(bool(r.precipitation_mm_imputed)), r.precipitation_mm_qc_flag,
             _f(r.u_wind_ms_raw), _f(r.u_wind_ms_clean),
             int(bool(r.u_wind_ms_imputed)), r.u_wind_ms_qc_flag,
             _f(r.v_wind_ms_raw), _f(r.v_wind_ms_clean),
             int(bool(r.v_wind_ms_imputed)), r.v_wind_ms_qc_flag,
             'noaa_gfs', 0)
            for r in df.itertuples(index=False)
        ]
        execute_many(cur, """
            INSERT OR IGNORE INTO cleaned_gfs (
                lat, lon, cycle, fhr, valid_time, fetched_at,
                temperature_c_raw, temperature_c_clean, temperature_c_imputed, temperature_c_qc_flag,
                precipitation_mm_raw, precipitation_mm_clean, precipitation_mm_imputed, precipitation_mm_qc_flag,
                u_wind_ms_raw, u_wind_ms_clean, u_wind_ms_imputed, u_wind_ms_qc_flag,
                v_wind_ms_raw, v_wind_ms_clean, v_wind_ms_imputed, v_wind_ms_qc_flag,
                source, is_synthetic
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cleaned_rows)

        conn.commit()
        logger.info(f"Inserted {len(cleaned_rows)} cleaned GFS gridpoint records.")
    except Exception as e:
        logger.error(f"Database error during GFS save: {e}")
        return ('failure', 0, str(e))
    finally:
        if conn:
            conn.close()

    # One Parquet file per cycle. pure_overwrite is safe now because a run
    # always writes the complete set of forecast hours for that cycle.
    save_cleaned_data_parquet(
        df, source='gfs', partition_key='cycle', partition_value=cycle_str,
        dedup_keys=['lat', 'lon', 'cycle', 'fhr'], pure_overwrite=True,
    )

    expected = EXPECTED_GRIDPOINTS * len(GFS_FORECAST_HOURS) // (GFS_GRID_STRIDE ** 2)
    status, err = 'success', None
    if fetch_errors:
        status = 'partial'
        err = f"{len(fetch_errors)} of {len(GFS_FORECAST_HOURS)} forecast hours unavailable: " \
              + "; ".join(fetch_errors)
    if not failures.empty:
        status = 'partial'
        msg = f"{len(failures['index'].dropna().unique())} rows dropped by contract validation."
        err = f"{err} | {msg}" if err else msg
    if len(df) < expected * 0.9:
        status = 'partial'
        msg = f"Got {len(df)} rows, expected about {expected}."
        err = f"{err} | {msg}" if err else msg

    return (status, len(df), err)


def _f(v):
    """None for NaN/NA, plain float otherwise — sqlite3 rejects numpy NA types."""
    return None if pd.isna(v) else float(v)


if __name__ == "__main__":
    from run_logger import log_run
    _started = datetime.now(timezone.utc).isoformat()
    _result = main()
    log_run('gfs', _started, _result)

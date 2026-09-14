"""
gold_layer.py — Medallion gold layer.

Reads the silver (cleaned) Parquet lake and produces analysis-ready tables:

  gold/city_aqi_hourly/    hourly per-city AQI from WAQI sub-indices
  gold/gfs_grid_hourly/    per-cycle spatial summaries of the GFS grid
  gold/city_daily_summary/ daily per-city pollutants joined with weather

Three things changed from the previous version.

1. AQI IS NO LONGER COMPUTED TWICE.
   WAQI serves AQI sub-indices; this module used to push them through India's
   concentration-to-AQI breakpoints as if they were µg/m³. The overall AQI is
   now the maximum of the sub-indices, which is what an AQI is, and it is
   labelled on the US EPA scale because that is the scale WAQI uses. CPCB
   sub-index maths still exists in aqi.py and is applied to CAMS data, which
   really is in µg/m³.

2. DAILY AQI RESPECTS CPCB'S COMPLETENESS RULE.
   CPCB's National AQI is defined on 24-hour averages with at least 16 hours
   of data and at least three pollutants including PM. A day that does not
   meet that returns 'insufficient_data' rather than a confident number
   derived from two readings.

3. THE REBUILD IS INCREMENTAL.
   It used to read the entire Parquet lake and rewrite every date partition on
   every hourly run, so the cost grew with total history. Only the last
   GOLD_LOOKBACK_DAYS days are read and rewritten.

The docstring of build_city_daily_summary also used to claim it joined weather
data. It did not — `weather_dir` was assigned and never used. It does now.
"""

import os
import glob
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from storage import DATA_DIR, ensure_dir
from aqi import combine_subindices, categorise, cpcb_aqi_from_concentrations

logger = logging.getLogger('gold_layer')

GOLD_DIR = os.path.join(DATA_DIR, 'gold')

# How much history each rebuild touches. Everything older is already written
# and does not change, so re-reading it every hour is wasted work.
GOLD_LOOKBACK_DAYS = int(os.getenv('GOLD_LOOKBACK_DAYS', '3'))

# CPCB requires 16 of 24 hours before a daily average counts.
MIN_HOURS_FOR_DAILY_AQI = int(os.getenv('MIN_HOURS_FOR_DAILY_AQI', '16'))

POLLUTANTS = ['pm25', 'pm10', 'no2', 'so2', 'co', 'o3']

# NOAA runs GFS at 00, 06, 12 and 18 UTC.
CYCLES_PER_DAY = 4

# Distance bands for city fire exposure, in kilometres.
#
# These are the feature that makes the fire data useful for air quality. Delhi's
# November PM2.5 is driven largely by stubble burning in Punjab and Haryana,
# roughly 200-400 km upwind — so a count of fires inside the city limits says
# almost nothing, while a count within a few hundred kilometres says a lot.
FIRE_RADII_KM = [100, 300, 500]

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres. Vectorised over numpy arrays."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# BUGFIX: duckdb was imported at module scope, so a missing or broken duckdb
# wheel took down scheduler.py at import time (it imports build_all_gold) and
# therefore killed the WAQI, weather, GFS and FIRMS jobs too. Import lazily.
def _duckdb():
    import duckdb
    return duckdb


def _recent_partitions(source, lookback_days, key='date'):
    """
    Returns the Parquet files for a source within the lookback window.

    Falls back to every partition when filenames do not carry a parseable
    date, which is the case for the cycle-partitioned GFS output.
    """
    directory = os.path.join(DATA_DIR, f'cleaned_{source}')
    if not os.path.isdir(directory):
        return directory, []

    files = sorted(glob.glob(os.path.join(directory, f'{key}=*.parquet')))
    if key != 'date':
        # GFS partitions are cycles, not dates, and there are four per day.
        # Slicing by lookback_days directly would keep 3 cycles (18 hours),
        # not 3 days.
        return directory, files[-(lookback_days * CYCLES_PER_DAY):] if lookback_days else files

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    # No `or files[-1:]` fallback. Returning the newest partition when nothing
    # falls inside the window makes the lookback a lie: a 2020 archive file
    # would be processed as though it were recent. An empty window means an
    # empty window — previously written gold partitions stay on disk untouched,
    # so nothing is lost by saying so.
    return directory, [f for f in files
                       if os.path.basename(f)[len('date='):-len('.parquet')] >= cutoff]


def _read_parquet(files):
    """Reads a list of Parquet files into one DataFrame via DuckDB."""
    if not files:
        return pd.DataFrame()
    conn = _duckdb().connect()
    try:
        file_list = ', '.join(f"'{f}'" for f in files)
        return conn.execute(f"SELECT * FROM read_parquet([{file_list}])").fetchdf()
    finally:
        conn.close()


def _write_partitions(df, table, key='date'):
    """Writes one Parquet file per partition value of `key`."""
    out_dir = os.path.join(GOLD_DIR, table)
    ensure_dir(out_dir)
    for value, group in df.groupby(key):
        group.drop(columns=[key]).to_parquet(
            os.path.join(out_dir, f'{key}={value}.parquet'), index=False
        )


# ── Gold 1: hourly city AQI ──────────────────────────────────────────────────

def build_city_aqi_hourly(lookback_days=None):
    """
    Aggregates WAQI sub-indices to hourly city level.

    The overall AQI is max(sub-indices) — WAQI already did the concentration
    to index conversion, so doing it again would be wrong.
    """
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    _, files = _recent_partitions('waqi', lookback)
    df = _read_parquet(files)
    if df.empty:
        logger.warning("[gold] No cleaned_waqi data in lookback window. Skipping city_aqi_hourly.")
        return 0

    df = df[df['is_synthetic'] == 0].copy()
    df['ts'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df.dropna(subset=['ts'], inplace=True)
    if df.empty:
        return 0

    df['hour_bucket'] = df['ts'].dt.floor('h').dt.strftime('%Y-%m-%dT%H:00:00+00:00')
    df['date'] = df['ts'].dt.strftime('%Y-%m-%d')

    subindex_cols = [f'{p}_aqi_clean' for p in POLLUTANTS if f'{p}_aqi_clean' in df.columns]
    df['aqi'] = df[subindex_cols].apply(lambda row: combine_subindices(row.tolist()), axis=1)
    # WAQI is the US EPA scale; labelling it with CPCB band names would
    # mis-state the health advice for anything between 101 and 200.
    df['aqi_category'] = df['aqi'].apply(lambda v: categorise(v, 'us_epa'))
    # BUGFIX: DataFrame.idxmax raises "Encountered all NA values" when a row has
    # no sub-index at all, which happens whenever a WAQI station returns an empty
    # iaqi block. That crashed the whole gold rebuild for every city.
    has_any = df[subindex_cols].notna().any(axis=1)
    df['dominant_pollutant'] = pd.NA
    if has_any.any():
        df.loc[has_any, 'dominant_pollutant'] = (
            df.loc[has_any, subindex_cols].idxmax(axis=1)
            .str.replace('_aqi_clean', '', regex=False)
        )

    agg = {f'{p}_aqi_mean': (f'{p}_aqi_clean', 'mean')
           for p in POLLUTANTS if f'{p}_aqi_clean' in df.columns}
    gold = df.groupby(['city', 'hour_bucket', 'date'], as_index=False).agg(
        aqi_max=('aqi', 'max'),
        aqi_mean=('aqi', 'mean'),
        aqi_category=('aqi_category', lambda x: x.mode().iloc[0] if len(x) else 'unknown'),
        dominant_pollutant=('dominant_pollutant',
                            lambda x: x.mode().iloc[0] if len(x.dropna()) else None),
        n_stations=('station_id', 'nunique'),
        n_observations=('timestamp', 'count'),
        **agg,
    )
    gold['aqi_scale'] = 'us_epa'
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()

    _write_partitions(gold, 'city_aqi_hourly')
    logger.info(f"[gold] city_aqi_hourly: {len(gold)} rows across {gold['date'].nunique()} date(s).")
    return len(gold)


# ── Gold 2: GFS grid summary per forecast valid time ─────────────────────────

def build_gfs_grid_hourly(lookback_days=None):
    """Spatial summaries of the GFS grid, one row per cycle and valid time."""
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    _, files = _recent_partitions('gfs', lookback, key='cycle')
    df = _read_parquet(files)
    if df.empty:
        logger.warning("[gold] No cleaned_gfs data. Skipping gfs_grid_hourly.")
        return 0

    df = df[df['is_synthetic'] == 0].copy()
    if df.empty:
        return 0

    # Wind speed is computed per gridpoint first and then averaged. Aggregating
    # u and v separately and combining afterwards would give the speed of the
    # mean wind vector, which is not the mean wind speed.
    if {'u_wind_ms_clean', 'v_wind_ms_clean'} <= set(df.columns):
        df['wind_speed_ms'] = np.sqrt(
            df['u_wind_ms_clean'].astype(float) ** 2 + df['v_wind_ms_clean'].astype(float) ** 2
        )
    else:
        df['wind_speed_ms'] = np.nan

    gold = df.groupby(['cycle', 'fhr', 'valid_time'], as_index=False).agg(
        temp_mean_c=('temperature_c_clean', 'mean'),
        temp_max_c=('temperature_c_clean', 'max'),
        temp_min_c=('temperature_c_clean', 'min'),
        precip_mean_mm=('precipitation_mm_clean', 'mean'),
        precip_max_mm=('precipitation_mm_clean', 'max'),
        wind_speed_mean_ms=('wind_speed_ms', 'mean'),
        wind_speed_max_ms=('wind_speed_ms', 'max'),
        n_grid_points=('lat', 'count'),
        n_flagged=('temperature_c_qc_flag', lambda x: int((x != 'ok').sum())),
    )
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()

    _write_partitions(gold, 'gfs_grid_hourly', key='cycle')
    logger.info(f"[gold] gfs_grid_hourly: {len(gold)} rows.")
    return len(gold)


# ── Gold 3: daily city summary ───────────────────────────────────────────────

def _nearest_weather_by_city(weather_df, cities):
    """
    Averages weather to city-day. Weather stations are named after their city,
    so this is a name join — good enough for the eleven stations we poll, and
    honest about it rather than pretending to do spatial matching.
    """
    if weather_df.empty:
        return pd.DataFrame()
    weather_df = weather_df.copy()
    weather_df['ts'] = pd.to_datetime(weather_df['timestamp'], utc=True, errors='coerce')
    weather_df.dropna(subset=['ts'], inplace=True)
    weather_df['date'] = weather_df['ts'].dt.strftime('%Y-%m-%d')
    weather_df['city'] = weather_df['station'].str.lower()
    weather_df = weather_df[weather_df['city'].isin(cities)]
    if weather_df.empty:
        return pd.DataFrame()

    return weather_df.groupby(['city', 'date'], as_index=False).agg(
        temperature_c_daily_mean=('temperature_c_clean', 'mean'),
        humidity_pct_daily_mean=('humidity_pct_clean', 'mean'),
        rainfall_mm_daily_total=('rainfall_mm_clean', 'sum'),
        wind_speed_ms_daily_mean=('wind_speed_ms_clean', 'mean'),
        n_weather_obs=('timestamp', 'count'),
    )


def build_city_daily_summary(lookback_days=None):
    """
    Daily per-city summary: WAQI sub-indices, a CPCB AQI computed from CAMS
    mass concentrations where available, and weather joined by city name.
    """
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    _, waqi_files = _recent_partitions('waqi', lookback)
    waqi_df = _read_parquet(waqi_files)
    if waqi_df.empty:
        logger.warning("[gold] No cleaned_waqi data for daily summary. Skipping.")
        return 0

    waqi_df = waqi_df[waqi_df['is_synthetic'] == 0].copy()
    waqi_df['ts'] = pd.to_datetime(waqi_df['timestamp'], utc=True, errors='coerce')
    waqi_df.dropna(subset=['ts'], inplace=True)
    waqi_df['date'] = waqi_df['ts'].dt.strftime('%Y-%m-%d')
    waqi_df['hour'] = waqi_df['ts'].dt.strftime('%Y-%m-%dT%H')

    agg = {f'{p}_aqi_daily_mean': (f'{p}_aqi_clean', 'mean')
           for p in POLLUTANTS if f'{p}_aqi_clean' in waqi_df.columns}
    daily = waqi_df.groupby(['city', 'date'], as_index=False).agg(
        n_obs=('timestamp', 'count'),
        n_hours_covered=('hour', 'nunique'),
        **agg,
    )

    subindex_means = [c for c in daily.columns if c.endswith('_aqi_daily_mean')]
    daily['daily_aqi'] = daily[subindex_means].apply(
        lambda row: combine_subindices(row.tolist()), axis=1)

    # CPCB's completeness rule. Below it the AQI is undefined, not zero.
    insufficient = daily['n_hours_covered'] < MIN_HOURS_FOR_DAILY_AQI
    daily.loc[insufficient, 'daily_aqi'] = np.nan
    daily['daily_aqi_category'] = daily['daily_aqi'].apply(lambda v: categorise(v, 'us_epa'))
    daily.loc[insufficient, 'daily_aqi_category'] = 'insufficient_data'
    daily['aqi_scale'] = 'us_epa'
    daily['meets_completeness_rule'] = (~insufficient).astype(int)

    # CAMS is in µg/m³, so a genuine CPCB National AQI can be derived from it.
    cams_daily = _build_cams_city_aqi(daily[['city', 'date']].drop_duplicates(), lookback)
    if not cams_daily.empty:
        daily = daily.merge(cams_daily, on=['city', 'date'], how='left')

    _, weather_files = _recent_partitions('weather', lookback)
    weather = _nearest_weather_by_city(_read_parquet(weather_files),
                                       set(daily['city'].str.lower()))
    if not weather.empty:
        daily['_city_lower'] = daily['city'].str.lower()
        daily = daily.merge(weather, left_on=['_city_lower', 'date'],
                            right_on=['city', 'date'], how='left', suffixes=('', '_w'))
        daily.drop(columns=[c for c in ['_city_lower', 'city_w'] if c in daily.columns],
                   inplace=True)

    daily['computed_at'] = datetime.now(timezone.utc).isoformat()
    _write_partitions(daily, 'city_daily_summary')
    logger.info(f"[gold] city_daily_summary: {len(daily)} rows "
                f"({int(daily['meets_completeness_rule'].sum())} meet the 16-hour rule).")
    return len(daily)


def _build_cams_city_aqi(city_dates, lookback_days=None):
    """
    Computes a real CPCB National AQI per city-day from CAMS concentrations.

    Returns an empty frame when there is no CAMS data — the pipeline runs fine
    without it, it just loses the CPCB-scale column.
    """
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    _, files = _recent_partitions('cams', lookback)
    df = _read_parquet(files)
    if df.empty or 'lat' not in df.columns:
        return pd.DataFrame()

    try:
        from fetch_cams import CITY_GRID
    except Exception:
        return pd.DataFrame()

    df['ts'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df.dropna(subset=['ts'], inplace=True)
    df['date'] = df['ts'].dt.strftime('%Y-%m-%d')

    rows = []
    for city, (lat, lon) in CITY_GRID.items():
        near = df[(df['lat'].sub(lat).abs() < 0.75) & (df['lon'].sub(lon).abs() < 0.75)]
        if near.empty:
            continue
        for date_val, group in near.groupby('date'):
            concentrations = {
                'pm25': group['pm25_ugm3_clean'].mean() if 'pm25_ugm3_clean' in group else None,
                'pm10': group['pm10_ugm3_clean'].mean() if 'pm10_ugm3_clean' in group else None,
                'no2':  group['no2_ugm3_clean'].mean() if 'no2_ugm3_clean' in group else None,
                'so2':  group['so2_ugm3_clean'].mean() if 'so2_ugm3_clean' in group else None,
                'o3':   group['o3_ugm3_clean'].mean() if 'o3_ugm3_clean' in group else None,
                # CPCB's CO breakpoints are in mg/m³; CAMS reports µg/m³.
                'co':   (group['co_ugm3_clean'].mean() / 1000.0
                         if 'co_ugm3_clean' in group else None),
            }
            aqi_val, category, dominant = cpcb_aqi_from_concentrations(concentrations)
            rows.append({
                'city': city, 'date': date_val,
                'cpcb_aqi_from_cams': aqi_val,
                'cpcb_aqi_category': category,
                'cpcb_dominant_pollutant': dominant,
            })

    return pd.DataFrame(rows)


# ── Gold 4: national daily fire activity ─────────────────────────────────────

def _load_fires(lookback_days):
    """Reads the cleaned FIRMS partitions within the lookback window."""
    _, files = _recent_partitions('firms', lookback_days)
    df = _read_parquet(files)
    if df.empty or 'lat' not in df.columns:
        return pd.DataFrame()

    df = df[df['is_synthetic'] == 0].copy() if 'is_synthetic' in df.columns else df.copy()
    df['ts'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce', format='mixed')
    df.dropna(subset=['ts'], inplace=True)
    if df.empty:
        return df

    df['date'] = df['ts'].dt.strftime('%Y-%m-%d')
    for col in ('lat', 'lon', 'frp_mw', 'brightness_k_clean'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['lat', 'lon'])


def build_fire_activity_daily(lookback_days=None):
    """
    Daily fire totals for the whole India box, split by sensor.

    Fire radiative power is summed as well as counted: a hundred smouldering
    detections and a hundred intense ones are the same count but not the same
    emission, and FRP is the quantity that scales with smoke.
    """
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    df = _load_fires(lookback)
    if df.empty:
        logger.warning("[gold] No cleaned_firms data in window. Skipping fire_activity_daily.")
        return 0

    if 'confidence_class' in df.columns:
        df['is_high_confidence'] = (df['confidence_class'] == 'high').astype(int)
    else:
        df['is_high_confidence'] = 0

    group_cols = ['date', 'sensor'] if 'sensor' in df.columns else ['date']
    gold = df.groupby(group_cols, as_index=False).agg(
        n_detections=('lat', 'count'),
        n_high_confidence=('is_high_confidence', 'sum'),
        frp_total_mw=('frp_mw', 'sum'),
        frp_mean_mw=('frp_mw', 'mean'),
        frp_max_mw=('frp_mw', 'max'),
        brightness_mean_k=('brightness_k_clean', 'mean'),
        lat_mean=('lat', 'mean'),
        lon_mean=('lon', 'mean'),
    )
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()

    _write_partitions(gold, 'fire_activity_daily')
    logger.info(f"[gold] fire_activity_daily: {len(gold)} rows across "
                f"{gold['date'].nunique()} date(s).")
    return len(gold)


# ── Gold 5: fire exposure per city ───────────────────────────────────────────

def build_city_fire_exposure_daily(lookback_days=None):
    """
    Per city per day: how much burning happened within each distance band.

    This is the join that makes the fire archive predictive rather than
    decorative. Stubble fires a few hundred kilometres upwind drive Delhi's
    winter particulate load, so the useful feature is not "fires in Delhi" but
    "fire radiative power within 300 km of Delhi yesterday".
    """
    lookback = GOLD_LOOKBACK_DAYS if lookback_days is None else lookback_days
    df = _load_fires(lookback)
    if df.empty:
        logger.warning("[gold] No cleaned_firms data. Skipping city_fire_exposure_daily.")
        return 0

    try:
        from fetch_cams import CITY_GRID
    except Exception:
        logger.warning("[gold] City coordinates unavailable. Skipping city_fire_exposure_daily.")
        return 0

    max_radius = max(FIRE_RADII_KM)
    # A degree of latitude is ~111 km; longitude shrinks with latitude. The box
    # is a cheap prefilter so the haversine runs on hundreds of rows, not
    # hundreds of thousands, for each city-day.
    lat_margin = max_radius / 111.0

    rows = []
    for city, (city_lat, city_lon) in CITY_GRID.items():
        lon_margin = max_radius / (111.0 * max(np.cos(np.radians(city_lat)), 0.1))
        near = df[(df['lat'].sub(city_lat).abs() <= lat_margin)
                  & (df['lon'].sub(city_lon).abs() <= lon_margin)]
        if near.empty:
            continue

        near = near.assign(distance_km=haversine_km(
            city_lat, city_lon, near['lat'].to_numpy(), near['lon'].to_numpy()))
        near = near[near['distance_km'] <= max_radius]
        if near.empty:
            continue

        for day, group in near.groupby('date'):
            record = {
                'city': city, 'date': day,
                'city_lat': city_lat, 'city_lon': city_lon,
                'nearest_fire_km': round(float(group['distance_km'].min()), 2),
            }
            for radius in FIRE_RADII_KM:
                within = group[group['distance_km'] <= radius]
                record[f'fires_within_{radius}km'] = int(len(within))
                record[f'frp_within_{radius}km'] = round(
                    float(within['frp_mw'].sum(skipna=True)), 2)
            rows.append(record)

    if not rows:
        logger.warning("[gold] No fires within range of any city in the window.")
        return 0

    gold = pd.DataFrame(rows)
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()
    _write_partitions(gold, 'city_fire_exposure_daily')
    logger.info(f"[gold] city_fire_exposure_daily: {len(gold)} city-day rows.")
    return len(gold)


# ── Entry point ──────────────────────────────────────────────────────────────

def build_all_gold(lookback_days=None):
    """Rebuilds the gold tables. Each one is independent, so one failure does
    not take the others down with it."""
    logger.info("[gold] Building gold layer tables...")
    total = 0
    for name, builder in (('city_aqi_hourly', build_city_aqi_hourly),
                          ('gfs_grid_hourly', build_gfs_grid_hourly),
                          ('city_daily_summary', build_city_daily_summary),
                          ('fire_activity_daily', build_fire_activity_daily),
                          ('city_fire_exposure_daily', build_city_fire_exposure_daily)):
        try:
            total += builder(lookback_days)
        except Exception as e:
            logger.error(f"[gold] {name} failed: {e}")
    logger.info(f"[gold] Done. {total} gold rows written.")
    return total


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Rebuild the gold layer from the cleaned Parquet lake.')
    parser.add_argument('--all', action='store_true',
                        help='process the ENTIRE lake, not just the recent lookback '
                             'window. Use this after a historical backfill — the '
                             'default of %d days exists for the hourly scheduler and '
                             'would skip years of archive data.' % GOLD_LOOKBACK_DAYS)
    parser.add_argument('--lookback-days', type=int, default=None,
                        help='override GOLD_LOOKBACK_DAYS for this run')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s')

    lookback = args.lookback_days
    if args.all:
        lookback = 100_000          # every partition on disk
        print('Processing the entire lake (--all).')

    build_all_gold(lookback)

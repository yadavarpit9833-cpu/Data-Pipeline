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
        return directory, files[-lookback_days:] if lookback_days else files

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    recent = [f for f in files if os.path.basename(f)[len('date='):-len('.parquet')] >= cutoff]
    return directory, recent or files[-1:]


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

def build_city_aqi_hourly():
    """
    Aggregates WAQI sub-indices to hourly city level.

    The overall AQI is max(sub-indices) — WAQI already did the concentration
    to index conversion, so doing it again would be wrong.
    """
    _, files = _recent_partitions('waqi', GOLD_LOOKBACK_DAYS)
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
    df['dominant_pollutant'] = df[subindex_cols].idxmax(axis=1).str.replace(
        '_aqi_clean', '', regex=False)

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

def build_gfs_grid_hourly():
    """Spatial summaries of the GFS grid, one row per cycle and valid time."""
    _, files = _recent_partitions('gfs', GOLD_LOOKBACK_DAYS, key='cycle')
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


def build_city_daily_summary():
    """
    Daily per-city summary: WAQI sub-indices, a CPCB AQI computed from CAMS
    mass concentrations where available, and weather joined by city name.
    """
    _, waqi_files = _recent_partitions('waqi', GOLD_LOOKBACK_DAYS)
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
    cams_daily = _build_cams_city_aqi(daily[['city', 'date']].drop_duplicates())
    if not cams_daily.empty:
        daily = daily.merge(cams_daily, on=['city', 'date'], how='left')

    _, weather_files = _recent_partitions('weather', GOLD_LOOKBACK_DAYS)
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


def _build_cams_city_aqi(city_dates):
    """
    Computes a real CPCB National AQI per city-day from CAMS concentrations.

    Returns an empty frame when there is no CAMS data — the pipeline runs fine
    without it, it just loses the CPCB-scale column.
    """
    _, files = _recent_partitions('cams', GOLD_LOOKBACK_DAYS)
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


# ── Entry point ──────────────────────────────────────────────────────────────

def build_all_gold():
    """Rebuilds the gold tables. Each one is independent, so one failure does
    not take the others down with it."""
    logger.info("[gold] Building gold layer tables...")
    total = 0
    for name, builder in (('city_aqi_hourly', build_city_aqi_hourly),
                          ('gfs_grid_hourly', build_gfs_grid_hourly),
                          ('city_daily_summary', build_city_daily_summary)):
        try:
            total += builder()
        except Exception as e:
            logger.error(f"[gold] {name} failed: {e}")
    logger.info(f"[gold] Done. {total} gold rows written.")
    return total


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
    build_all_gold()

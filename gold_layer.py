"""
gold_layer.py  — Medallion Gold Layer (Item 10)
-------------------------------------------------
Reads Silver (cleaned) data from Parquet and produces Gold
analysis-ready features:

  Gold 1: Hourly city-level AQI aggregates  (gold/city_aqi_hourly/)
  Gold 2: Hourly GFS grid aggregates         (gold/gfs_grid_hourly/)
  Gold 3: Daily city summary + AQI index     (gold/city_daily_summary/)

Gold tables are what ML models actually consume. They are rebuilt on
every scheduler run — they are derived, never primary storage.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from storage import DATA_DIR, ensure_dir

logger = logging.getLogger('gold_layer')

GOLD_DIR = os.path.join(DATA_DIR, 'gold')

# BUGFIX: duckdb was imported at module scope, so a missing/broken duckdb wheel
# took down scheduler.py at import time (it imports build_all_gold) and therefore
# killed CPCB, weather, GFS and FIRMS too. Import it lazily instead.
def _duckdb():
    import duckdb
    return duckdb


# ── AQI sub-index formulas (India CPCB standard) ─────────────────────────────

def _pm25_aqi(c):
    """PM2.5 (µg/m³) → AQI sub-index per India CPCB breakpoints."""
    breakpoints = [
        (0,   30,   0,   50),
        (30,  60,   51,  100),
        (60,  90,   101, 200),
        (90,  120,  201, 300),
        (120, 250,  301, 400),
        (250, 500,  401, 500),
    ]
    if pd.isna(c) or c < 0:
        return np.nan
    for Clo, Chi, Ilo, Ihi in breakpoints:
        if Clo <= c <= Chi:
            return ((Ihi - Ilo) / (Chi - Clo)) * (c - Clo) + Ilo
    return 500.0

def _pm10_aqi(c):
    """PM10 (µg/m³) → AQI sub-index per India CPCB breakpoints."""
    breakpoints = [
        (0,   50,   0,   50),
        (50,  100,  51,  100),
        (100, 250,  101, 200),
        (250, 350,  201, 300),
        (350, 430,  301, 400),
        (430, 600,  401, 500),
    ]
    if pd.isna(c) or c < 0:
        return np.nan
    for Clo, Chi, Ilo, Ihi in breakpoints:
        if Clo <= c <= Chi:
            return ((Ihi - Ilo) / (Chi - Clo)) * (c - Clo) + Ilo
    return 500.0

def _circular_mean_deg(series):
    """
    Vector mean of compass bearings. A plain arithmetic mean is wrong here:
    mean(350, 10) is 180 (due south) when the true mean bearing is 0 (north).
    """
    vals = pd.to_numeric(series, errors='coerce').dropna()
    if vals.empty:
        return np.nan
    rad = np.deg2rad(vals.to_numpy(dtype=float))
    ang = np.arctan2(np.sin(rad).mean(), np.cos(rad).mean())
    # Snap float residue so due north reads 0.0, not 359.99999999999994
    return round(float(np.rad2deg(ang) % 360.0), 6) % 360.0


def aqi_category(aqi_val):
    """Map numeric AQI to India CPCB category string."""
    if pd.isna(aqi_val):
        return 'unknown'
    v = float(aqi_val)
    if v <= 50:   return 'Good'
    if v <= 100:  return 'Satisfactory'
    if v <= 200:  return 'Moderate'
    if v <= 300:  return 'Poor'
    if v <= 400:  return 'Very Poor'
    return 'Severe'


# ── Gold 1: Hourly city AQI ──────────────────────────────────────────────────

def build_city_aqi_hourly():
    """
    Reads Silver cleaned_cpcb Parquet files, computes India CPCB AQI
    sub-indices for PM2.5 and PM10, takes their max as the overall AQI,
    and aggregates to hourly city level.

    Output: gold/city_aqi_hourly/date=YYYY-MM-DD.parquet
    """
    cleaned_dir = os.path.join(DATA_DIR, 'cleaned_cpcb')
    if not os.path.exists(cleaned_dir) or not os.listdir(cleaned_dir):
        logger.warning("[gold] No cleaned_cpcb Parquet files found. Skipping city_aqi_hourly.")
        return 0

    conn = _duckdb().connect()
    df = conn.execute(f"SELECT * FROM '{cleaned_dir}/*.parquet'").fetchdf()
    conn.close()

    if df.empty:
        logger.warning("[gold] cleaned_cpcb is empty. Skipping.")
        return 0

    # Filter out synthetic rows
    df = df[df['is_synthetic'] == 0].copy()

    # Parse timestamp → hour bucket
    df['ts'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df.dropna(subset=['ts'], inplace=True)
    df['hour_bucket'] = df['ts'].dt.floor('h').dt.strftime('%Y-%m-%dT%H:00:00+00:00')
    df['date'] = df['ts'].dt.strftime('%Y-%m-%d')

    # Compute AQI sub-indices
    df['pm25_aqi'] = df['pm25_clean'].apply(_pm25_aqi)
    df['pm10_aqi'] = df['pm10_clean'].apply(_pm10_aqi)
    df['aqi']      = df[['pm25_aqi', 'pm10_aqi']].max(axis=1)

    # Aggregate per city per hour
    gold = (
        df.groupby(['city', 'hour_bucket', 'date'], as_index=False)
        .agg(
            pm25_mean=('pm25_clean', 'mean'),
            pm10_mean=('pm10_clean', 'mean'),
            no2_mean=('no2_clean', 'mean'),
            aqi_max=('aqi', 'max'),
            n_stations=('station_id', 'nunique'),
        )
    )
    # BUGFIX: aqi_category used to be computed per-station and then reduced with
    # .mode(), while aqi_max is a max — so a city-hour could report aqi_max=300
    # ("Poor") alongside aqi_category="Moderate", and .mode() broke ties
    # alphabetically. Derive the label from the aggregate it describes.
    gold['aqi_category'] = gold['aqi_max'].apply(aqi_category)
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()

    # Save partitioned by date
    out_dir = os.path.join(GOLD_DIR, 'city_aqi_hourly')
    ensure_dir(out_dir)
    for date_val, group in gold.groupby('date'):
        path = os.path.join(out_dir, f"date={date_val}.parquet")
        group.drop(columns='date').to_parquet(path, index=False)

    logger.info(f"[gold] city_aqi_hourly: {len(gold)} rows across {gold['date'].nunique()} date(s).")
    return len(gold)


# ── Gold 2: Hourly GFS grid summary ─────────────────────────────────────────

def build_gfs_grid_hourly():
    """
    Reads Silver cleaned_gfs Parquet files, aggregates to hourly
    spatial summaries (mean/max over the India grid).

    Output: gold/gfs_grid_hourly/cycle=<cycle>.parquet
    """
    cleaned_dir = os.path.join(DATA_DIR, 'cleaned_gfs')
    if not os.path.exists(cleaned_dir) or not os.listdir(cleaned_dir):
        logger.warning("[gold] No cleaned_gfs Parquet files found. Skipping gfs_grid_hourly.")
        return 0

    conn = _duckdb().connect()
    df = conn.execute(f"SELECT * FROM '{cleaned_dir}/*.parquet'").fetchdf()
    conn.close()

    if df.empty:
        return 0

    df = df[df['is_synthetic'] == 0].copy()

    gold = (
        df.groupby(['cycle', 'valid_time'], as_index=False)
        .agg(
            temp_mean=('temperature_clean', 'mean'),
            temp_max=('temperature_clean', 'max'),
            temp_min=('temperature_clean', 'min'),
            precip_total=('precipitation_clean', 'sum'),
            wind_speed_mean=(
                'u_wind_clean',
                lambda u: np.sqrt((u**2 + df.loc[u.index, 'v_wind_clean']**2)).mean()
                if 'v_wind_clean' in df.columns else u.mean()
            ),
            n_grid_points=('lat', 'count'),
        )
    )
    gold['computed_at'] = datetime.now(timezone.utc).isoformat()

    out_dir = os.path.join(GOLD_DIR, 'gfs_grid_hourly')
    ensure_dir(out_dir)
    for cycle_val, group in gold.groupby('cycle'):
        path = os.path.join(out_dir, f"cycle={cycle_val}.parquet")
        group.to_parquet(path, index=False)

    logger.info(f"[gold] gfs_grid_hourly: {len(gold)} rows.")
    return len(gold)


# ── Gold 3: Daily city summary ───────────────────────────────────────────────

# Weather feature columns, declared once so the output schema stays identical
# whether or not weather data happened to be available for a given run.
_WEATHER_FEATURES = [
    'temp_daily_mean', 'temp_daily_max', 'temp_daily_min',
    'humidity_daily_mean', 'rainfall_daily_total',
    'wind_speed_daily_mean', 'wind_dir_daily_mean', 'n_weather_obs',
]


def _load_daily_weather(conn, weather_dir):
    """
    Aggregates Silver cleaned_weather to one row per city per day.
    Returns None when no weather Parquet exists yet.
    """
    if not os.path.exists(weather_dir) or not os.listdir(weather_dir):
        return None

    w_df = conn.execute(f"SELECT * FROM '{weather_dir}/*.parquet'").fetchdf()
    if w_df.empty:
        return None

    w_df = w_df[w_df['is_synthetic'] == 0].copy()
    w_df['ts'] = pd.to_datetime(w_df['timestamp'], utc=True, errors='coerce')
    w_df.dropna(subset=['ts'], inplace=True)
    w_df['date'] = w_df['ts'].dt.strftime('%Y-%m-%d')
    # cleaned_weather keys on 'station' ("Delhi"); cleaned_cpcb on 'city'
    # ("delhi"). Case-fold both so the join actually matches.
    w_df['city_key'] = w_df['station'].astype(str).str.strip().str.lower()

    daily_wx = (
        w_df.groupby(['city_key', 'date'], as_index=False)
        .agg(
            temp_daily_mean=('temperature_clean', 'mean'),
            temp_daily_max=('temperature_clean', 'max'),
            temp_daily_min=('temperature_clean', 'min'),
            humidity_daily_mean=('humidity_clean', 'mean'),
            rainfall_daily_total=('rainfall_clean', 'sum'),
            wind_speed_daily_mean=('wind_speed_clean', 'mean'),
            wind_dir_daily_mean=('wind_dir_clean', _circular_mean_deg),
            n_weather_obs=('timestamp', 'count'),
        )
    )
    return daily_wx


def build_city_daily_summary():
    """
    Joins cleaned_cpcb + cleaned_weather on city+date and produces
    a combined daily feature row per city: avg pollutants + avg weather.

    The join is a LEFT join on the case-folded city name, so a city with
    pollution data but no weather station still produces a row (weather
    columns NaN) rather than silently vanishing from the Gold table.

    Output: gold/city_daily_summary/date=YYYY-MM-DD.parquet
    """
    cpcb_dir    = os.path.join(DATA_DIR, 'cleaned_cpcb')
    weather_dir = os.path.join(DATA_DIR, 'cleaned_weather')

    if not os.path.exists(cpcb_dir) or not os.listdir(cpcb_dir):
        logger.warning("[gold] No cleaned_cpcb data for daily summary. Skipping.")
        return 0

    # BUGFIX: the connection used to be opened before the guard above and leaked
    # on every early return. Open it after the guard and always close it.
    conn = _duckdb().connect()
    try:
        cpcb_df = conn.execute(f"SELECT * FROM '{cpcb_dir}/*.parquet'").fetchdf()
        cpcb_df = cpcb_df[cpcb_df['is_synthetic'] == 0].copy()
        cpcb_df['ts']   = pd.to_datetime(cpcb_df['timestamp'], utc=True, errors='coerce')
        cpcb_df.dropna(subset=['ts'], inplace=True)
        cpcb_df['date'] = cpcb_df['ts'].dt.strftime('%Y-%m-%d')

        daily_poll = (
            cpcb_df.groupby(['city', 'date'], as_index=False)
            .agg(
                pm25_daily_mean=('pm25_clean', 'mean'),
                pm10_daily_mean=('pm10_clean', 'mean'),
                no2_daily_mean=('no2_clean', 'mean'),
                so2_daily_mean=('so2_clean', 'mean'),
                co_daily_mean=('co_clean', 'mean'),
                o3_daily_mean=('o3_clean', 'mean'),
                n_obs=('timestamp', 'count'),
            )
        )

        # BUGFIX: weather_dir was computed and never used — the docstring
        # promised a pollutant+weather row but only pollutants were ever
        # written. Actually join the weather features now.
        daily_poll['city_key'] = daily_poll['city'].astype(str).str.strip().str.lower()
        daily_wx = _load_daily_weather(conn, weather_dir)

        if daily_wx is None:
            logger.warning("[gold] No cleaned_weather data — weather features will be null.")
            for col in _WEATHER_FEATURES:
                daily_poll[col] = np.nan
        else:
            daily_poll = daily_poll.merge(daily_wx, on=['city_key', 'date'], how='left')
            # Guarantee a stable schema even if a column dropped out upstream
            for col in _WEATHER_FEATURES:
                if col not in daily_poll.columns:
                    daily_poll[col] = np.nan
            matched = int(daily_poll['n_weather_obs'].notna().sum())
            logger.info(
                f"[gold] city_daily_summary: matched weather for "
                f"{matched}/{len(daily_poll)} city-days."
            )

        daily_poll.drop(columns='city_key', inplace=True)

        # Compute daily AQI
        daily_poll['daily_aqi'] = daily_poll[['pm25_daily_mean', 'pm10_daily_mean']].apply(
            lambda r: max(
                _pm25_aqi(r['pm25_daily_mean']) if not pd.isna(r['pm25_daily_mean']) else 0,
                _pm10_aqi(r['pm10_daily_mean']) if not pd.isna(r['pm10_daily_mean']) else 0,
            ), axis=1
        )
        daily_poll['daily_aqi_category'] = daily_poll['daily_aqi'].apply(aqi_category)
        daily_poll['computed_at'] = datetime.now(timezone.utc).isoformat()

        out_dir = os.path.join(GOLD_DIR, 'city_daily_summary')
        ensure_dir(out_dir)
        for date_val, group in daily_poll.groupby('date'):
            path = os.path.join(out_dir, f"date={date_val}.parquet")
            group.to_parquet(path, index=False)
    finally:
        conn.close()

    logger.info(f"[gold] city_daily_summary: {len(daily_poll)} rows.")
    return len(daily_poll)


# ── Entry point ──────────────────────────────────────────────────────────────

def build_all_gold():
    """Rebuild all Gold tables. Call from scheduler or standalone."""
    logger.info("[gold] Building all Gold layer tables...")
    r1 = build_city_aqi_hourly()
    r2 = build_gfs_grid_hourly()
    r3 = build_city_daily_summary()
    logger.info(f"[gold] Done. city_aqi_hourly={r1}, gfs_grid_hourly={r2}, city_daily_summary={r3}")
    return r1 + r2 + r3


if __name__ == '__main__':
    import logging as _l
    _l.basicConfig(level=_l.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
    build_all_gold()

-- schema.sql
--
-- Canonical schema for the environmental data pipeline.
-- Written as SQLite DDL; db.translate_ddl() converts it for PostgreSQL.
--
-- NAMING RULE: a table is named after the source the data ACTUALLY comes from,
-- and every value column carries its unit. Earlier revisions named tables after
-- the source we wished we had (cpcb, imd, sentinel5p) while storing data from
-- a different provider on a different scale, which produced real numeric bugs
-- downstream. See docs/DATA_SOURCES.md.

-- ============================================================================
-- RAW LAYER (bronze) — unparsed provider payloads, preserved verbatim
-- ============================================================================

-- World Air Quality Index (waqi.info) city feeds.
-- WAQI aggregates and republishes CPCB station data for India, but the values
-- in its `iaqi` block are US EPA AQI SUB-INDICES, not µg/m³ concentrations.
CREATE TABLE IF NOT EXISTS raw_waqi (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_waqi UNIQUE (timestamp, raw_data_hash)
);

-- Open-Meteo surface weather for Indian WMO station coordinates.
-- This is NOT IMD data: mausam.imd.gov.in requires IP whitelisting.
CREATE TABLE IF NOT EXISTS raw_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_weather UNIQUE (timestamp, raw_data_hash)
);

-- NOAA GFS 0.25 degree forecast grid, decoded per gridpoint.
-- The GRIB2 payload itself lives in the data lake (data/raw/gfs/), not here:
-- storing a byte slice on all 14,625 rows of a cycle duplicated the same
-- bytes 14,625 times and preserved nothing usable for audit.
CREATE TABLE IF NOT EXISTS raw_gfs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    cycle TEXT,
    fhr TEXT,
    valid_time TEXT,
    fetched_at TEXT,
    temperature_c REAL,
    precipitation_mm REAL,
    u_wind_ms REAL,
    v_wind_ms REAL,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_gfs UNIQUE (lat, lon, cycle, fhr)
);

-- One row per GRIB2 file actually downloaded: the audit trail for raw_gfs.
CREATE TABLE IF NOT EXISTS gfs_fetch_manifest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle TEXT NOT NULL,
    fhr TEXT NOT NULL,
    valid_time TEXT,
    fetched_at TEXT NOT NULL,
    payload_bytes INTEGER,
    payload_sha256 TEXT,
    lake_path TEXT,
    gridpoints INTEGER,
    CONSTRAINT unq_gfs_manifest UNIQUE (cycle, fhr, payload_sha256)
);

-- NASA FIRMS active fire detections (MODIS + VIIRS).
CREATE TABLE IF NOT EXISTS raw_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    sensor TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_firms UNIQUE (timestamp, raw_data_hash)
);

-- Copernicus CAMS global atmospheric composition forecast, served through the
-- Open-Meteo Air Quality API. Values are SURFACE MASS CONCENTRATIONS in
-- micrograms per cubic metre — not TROPOMI column densities, and not ppb.
CREATE TABLE IF NOT EXISTS raw_cams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    no2_ugm3 REAL,
    so2_ugm3 REAL,
    co_ugm3 REAL,
    o3_ugm3 REAL,
    pm25_ugm3 REAL,
    pm10_ugm3 REAL,
    fetched_at TEXT,
    raw_data_hash TEXT,
    source TEXT DEFAULT 'cams_via_open-meteo',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_cams UNIQUE (lat, lon, timestamp)
);

-- ============================================================================
-- CLEANED LAYER (silver) — QC-flagged values, originals never overwritten
-- ============================================================================

-- Columns are *_aqi_* because WAQI serves AQI sub-indices. Feeding these
-- through concentration-to-AQI breakpoints would convert an AQI into an AQI.
CREATE TABLE IF NOT EXISTS cleaned_waqi (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT,
    city TEXT,
    timestamp TEXT,
    aqi_scale TEXT DEFAULT 'us_epa',
    pm25_aqi_raw REAL,
    pm25_aqi_clean REAL,
    pm25_aqi_imputed INTEGER DEFAULT 0,
    pm25_aqi_qc_flag TEXT DEFAULT 'ok',
    pm10_aqi_raw REAL,
    pm10_aqi_clean REAL,
    pm10_aqi_imputed INTEGER DEFAULT 0,
    pm10_aqi_qc_flag TEXT DEFAULT 'ok',
    no2_aqi_raw REAL,
    no2_aqi_clean REAL,
    no2_aqi_imputed INTEGER DEFAULT 0,
    no2_aqi_qc_flag TEXT DEFAULT 'ok',
    so2_aqi_raw REAL,
    so2_aqi_clean REAL,
    so2_aqi_imputed INTEGER DEFAULT 0,
    so2_aqi_qc_flag TEXT DEFAULT 'ok',
    co_aqi_raw REAL,
    co_aqi_clean REAL,
    co_aqi_imputed INTEGER DEFAULT 0,
    co_aqi_qc_flag TEXT DEFAULT 'ok',
    o3_aqi_raw REAL,
    o3_aqi_clean REAL,
    o3_aqi_imputed INTEGER DEFAULT 0,
    o3_aqi_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_waqi UNIQUE (station_id, timestamp)
);

CREATE TABLE IF NOT EXISTS cleaned_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station TEXT,
    station_id TEXT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    temperature_c_raw REAL,
    temperature_c_clean REAL,
    temperature_c_imputed INTEGER DEFAULT 0,
    temperature_c_qc_flag TEXT DEFAULT 'ok',
    humidity_pct_raw REAL,
    humidity_pct_clean REAL,
    humidity_pct_imputed INTEGER DEFAULT 0,
    humidity_pct_qc_flag TEXT DEFAULT 'ok',
    rainfall_mm_raw REAL,
    rainfall_mm_clean REAL,
    rainfall_mm_imputed INTEGER DEFAULT 0,
    rainfall_mm_qc_flag TEXT DEFAULT 'ok',
    wind_speed_ms_raw REAL,
    wind_speed_ms_clean REAL,
    wind_speed_ms_imputed INTEGER DEFAULT 0,
    wind_speed_ms_qc_flag TEXT DEFAULT 'ok',
    wind_dir_deg_raw REAL,
    wind_dir_deg_clean REAL,
    wind_dir_deg_imputed INTEGER DEFAULT 0,
    wind_dir_deg_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_weather UNIQUE (station, timestamp)
);

CREATE TABLE IF NOT EXISTS cleaned_gfs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    cycle TEXT,
    fhr TEXT,
    valid_time TEXT,
    fetched_at TEXT,
    temperature_c_raw REAL,
    temperature_c_clean REAL,
    temperature_c_imputed INTEGER DEFAULT 0,
    temperature_c_qc_flag TEXT DEFAULT 'ok',
    precipitation_mm_raw REAL,
    precipitation_mm_clean REAL,
    precipitation_mm_imputed INTEGER DEFAULT 0,
    precipitation_mm_qc_flag TEXT DEFAULT 'ok',
    u_wind_ms_raw REAL,
    u_wind_ms_clean REAL,
    u_wind_ms_imputed INTEGER DEFAULT 0,
    u_wind_ms_qc_flag TEXT DEFAULT 'ok',
    v_wind_ms_raw REAL,
    v_wind_ms_clean REAL,
    v_wind_ms_imputed INTEGER DEFAULT 0,
    v_wind_ms_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_gfs UNIQUE (lat, lon, cycle, fhr)
);

-- confidence_scale records whether confidence_raw is a MODIS percentage
-- ('percent') or a VIIRS class letter ('class'); confidence_class is the
-- low/nominal/high value both sensors are mapped onto.
CREATE TABLE IF NOT EXISTS cleaned_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    sensor TEXT,
    satellite TEXT,
    brightness_k_raw REAL,
    brightness_k_clean REAL,
    brightness_k_imputed INTEGER DEFAULT 0,
    brightness_k_qc_flag TEXT DEFAULT 'ok',
    frp_mw REAL,
    confidence_raw TEXT,
    confidence_scale TEXT,
    confidence_class TEXT,
    daynight TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_firms UNIQUE (lat, lon, timestamp, satellite, sensor)
);

CREATE TABLE IF NOT EXISTS cleaned_cams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    no2_ugm3_raw REAL,
    no2_ugm3_clean REAL,
    no2_ugm3_qc_flag TEXT DEFAULT 'ok',
    so2_ugm3_raw REAL,
    so2_ugm3_clean REAL,
    so2_ugm3_qc_flag TEXT DEFAULT 'ok',
    co_ugm3_raw REAL,
    co_ugm3_clean REAL,
    co_ugm3_qc_flag TEXT DEFAULT 'ok',
    o3_ugm3_raw REAL,
    o3_ugm3_clean REAL,
    o3_ugm3_qc_flag TEXT DEFAULT 'ok',
    pm25_ugm3_raw REAL,
    pm25_ugm3_clean REAL,
    pm25_ugm3_qc_flag TEXT DEFAULT 'ok',
    pm10_ugm3_raw REAL,
    pm10_ugm3_clean REAL,
    pm10_ugm3_qc_flag TEXT DEFAULT 'ok',
    fetched_at TEXT,
    source TEXT DEFAULT 'cams_via_open-meteo',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_cams UNIQUE (lat, lon, timestamp)
);

-- ============================================================================
-- OPERATIONS
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    run_started_at TEXT NOT NULL,
    run_finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_inserted INTEGER DEFAULT 0,
    error_message TEXT
);

-- monitor_health.py filters by source and start time on every check.
CREATE INDEX IF NOT EXISTS idx_run_log_source_started
    ON pipeline_run_log (source, run_started_at);
CREATE INDEX IF NOT EXISTS idx_run_log_source_status
    ON pipeline_run_log (source, status, id);

CREATE INDEX IF NOT EXISTS idx_cleaned_waqi_city_ts
    ON cleaned_waqi (city, timestamp);
CREATE INDEX IF NOT EXISTS idx_cleaned_weather_station_ts
    ON cleaned_weather (station, timestamp);
CREATE INDEX IF NOT EXISTS idx_cleaned_gfs_valid_time
    ON cleaned_gfs (valid_time);
CREATE INDEX IF NOT EXISTS idx_cleaned_gfs_point
    ON cleaned_gfs (lat, lon, valid_time);
CREATE INDEX IF NOT EXISTS idx_cleaned_firms_ts
    ON cleaned_firms (timestamp);
CREATE INDEX IF NOT EXISTS idx_cleaned_cams_ts
    ON cleaned_cams (timestamp);

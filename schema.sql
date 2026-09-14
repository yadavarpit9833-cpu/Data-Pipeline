-- schema.sql
CREATE TABLE IF NOT EXISTS raw_cpcb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0, -- Note: This flag is row-level, not column-level. Cannot express partial fallbacks where some fields are real and others are synthetic.
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_cpcb UNIQUE (timestamp, raw_data_hash)
);

-- Note: The 'imd' tables actually store Open-Meteo data, acting as a fallback because the real IMD API requires IP whitelisting. This is NOT real Indian government IMD data.
CREATE TABLE IF NOT EXISTS raw_imd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0, -- Note: This flag is row-level, not column-level. Cannot express partial fallbacks where some fields are real and others are synthetic.
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_imd UNIQUE (timestamp, raw_data_hash)
);

CREATE TABLE IF NOT EXISTS raw_gfs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    cycle TEXT,
    fhr TEXT,
    valid_time TEXT,
    fetched_at TEXT,
    temperature_raw REAL,
    precipitation_raw REAL,
    u_wind_raw REAL,
    v_wind_raw REAL,
    raw_data BLOB,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0, -- Note: This flag is row-level, not column-level. Cannot express partial fallbacks where some fields are real and others are synthetic.
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_gfs UNIQUE (lat, lon, cycle, fhr)
);

CREATE TABLE IF NOT EXISTS raw_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0, -- Note: This flag is row-level, not column-level. Cannot express partial fallbacks where some fields are real and others are synthetic.
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_firms UNIQUE (timestamp, raw_data_hash)
);


-- Cleaned tables
CREATE TABLE IF NOT EXISTS cleaned_cpcb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT,
    city TEXT,
    timestamp TEXT,
    pm25_raw REAL,
    pm25_clean REAL,
    pm25_imputed INTEGER DEFAULT 0,
    pm25_qc_flag TEXT DEFAULT 'ok',
    pm10_raw REAL,
    pm10_clean REAL,
    pm10_imputed INTEGER DEFAULT 0,
    pm10_qc_flag TEXT DEFAULT 'ok',
    no2_raw REAL,
    no2_clean REAL,
    no2_imputed INTEGER DEFAULT 0,
    no2_qc_flag TEXT DEFAULT 'ok',
    so2_raw REAL,
    so2_clean REAL,
    so2_imputed INTEGER DEFAULT 0,
    so2_qc_flag TEXT DEFAULT 'ok',
    co_raw REAL,
    co_clean REAL,
    co_imputed INTEGER DEFAULT 0,
    co_qc_flag TEXT DEFAULT 'ok',
    o3_raw REAL,
    o3_clean REAL,
    o3_imputed INTEGER DEFAULT 0,
    o3_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_cpcb UNIQUE (station_id, timestamp)
);

-- Note: The 'imd' tables actually store Open-Meteo data, acting as a fallback because the real IMD API requires IP whitelisting. This is NOT real Indian government IMD data.
CREATE TABLE IF NOT EXISTS cleaned_imd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station TEXT,
    timestamp TEXT,
    temperature_raw REAL,
    temperature_clean REAL,
    temperature_imputed INTEGER DEFAULT 0,
    temperature_qc_flag TEXT DEFAULT 'ok',
    humidity_raw REAL,
    humidity_clean REAL,
    humidity_imputed INTEGER DEFAULT 0,
    humidity_qc_flag TEXT DEFAULT 'ok',
    rainfall_raw REAL,
    rainfall_clean REAL,
    rainfall_imputed INTEGER DEFAULT 0,
    rainfall_qc_flag TEXT DEFAULT 'ok',
    wind_speed_raw REAL,
    wind_speed_clean REAL,
    wind_speed_imputed INTEGER DEFAULT 0,
    wind_speed_qc_flag TEXT DEFAULT 'ok',
    wind_dir_raw REAL,
    wind_dir_clean REAL,
    wind_dir_imputed INTEGER DEFAULT 0,
    wind_dir_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_imd UNIQUE (station, timestamp)
);

CREATE TABLE IF NOT EXISTS cleaned_gfs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    cycle TEXT,
    fhr TEXT,
    valid_time TEXT,
    fetched_at TEXT,
    temperature_raw REAL,
    temperature_clean REAL,
    temperature_imputed INTEGER DEFAULT 0,
    temperature_qc_flag TEXT DEFAULT 'ok',
    precipitation_raw REAL,
    precipitation_clean REAL,
    precipitation_imputed INTEGER DEFAULT 0,
    precipitation_qc_flag TEXT DEFAULT 'ok',
    u_wind_raw REAL,
    u_wind_clean REAL,
    u_wind_imputed INTEGER DEFAULT 0,
    u_wind_qc_flag TEXT DEFAULT 'ok',
    v_wind_raw REAL,
    v_wind_clean REAL,
    v_wind_imputed INTEGER DEFAULT 0,
    v_wind_qc_flag TEXT DEFAULT 'ok',
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_gfs UNIQUE (lat, lon, cycle, fhr)
);

CREATE TABLE IF NOT EXISTS cleaned_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    brightness_raw REAL,
    brightness_clean REAL,
    brightness_imputed INTEGER DEFAULT 0,
    brightness_qc_flag TEXT DEFAULT 'ok',
    confidence_raw TEXT,
    confidence_clean TEXT,
    confidence_imputed INTEGER DEFAULT 0,
    satellite TEXT,
    source TEXT,
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_firms UNIQUE (lat, lon, timestamp, satellite)
);

-- Note: Sentinel-5P rows are sourced from the CAMS/Open-Meteo composite, not
-- direct TROPOMI L2 granules. Unique on the grid cell + observation timestamp.
CREATE TABLE IF NOT EXISTS raw_sentinel5p (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    no2_ppb REAL,
    so2_ppb REAL,
    co_ppb REAL,
    o3_ppb REAL,
    fetched_at TEXT,
    raw_data_hash TEXT,
    source TEXT DEFAULT 'cams_open-meteo',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_sentinel5p UNIQUE (lat, lon, timestamp)
);

CREATE TABLE IF NOT EXISTS cleaned_sentinel5p (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    no2_ppb REAL,
    no2_clean REAL,
    no2_qc_flag TEXT DEFAULT 'ok',
    so2_ppb REAL,
    so2_clean REAL,
    so2_qc_flag TEXT DEFAULT 'ok',
    co_ppb REAL,
    co_clean REAL,
    co_qc_flag TEXT DEFAULT 'ok',
    o3_ppb REAL,
    o3_clean REAL,
    o3_qc_flag TEXT DEFAULT 'ok',
    fetched_at TEXT,
    source TEXT DEFAULT 'cams_open-meteo',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_sentinel5p UNIQUE (lat, lon, timestamp)
);

-- CAMS air quality. Deliberately NOT stored in cleaned_cpcb: WAQI's iaqi values
-- are AQI sub-indices on a 0-500 scale, while CAMS reports physical
-- concentrations. Putting both in one column would make that column ambiguous
-- without inspecting `source` on every row. Units are CAMS's own, as fetched:
-- ug/m3 for every pollutant including CO (CPCB quotes CO in mg/m3 instead).
CREATE TABLE IF NOT EXISTS raw_cams_aq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    lat REAL,
    lon REAL,
    span_start TEXT,
    raw_data TEXT,
    raw_data_hash TEXT,
    source TEXT DEFAULT 'cams-openmeteo-archive',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_raw_cams_aq UNIQUE (city, span_start, raw_data_hash)
);

CREATE TABLE IF NOT EXISTS cleaned_cams_aq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    pm25_ugm3 REAL, pm25_clean REAL, pm25_imputed INTEGER DEFAULT 0, pm25_qc_flag TEXT DEFAULT 'ok',
    pm10_ugm3 REAL, pm10_clean REAL, pm10_imputed INTEGER DEFAULT 0, pm10_qc_flag TEXT DEFAULT 'ok',
    no2_ugm3 REAL,  no2_clean REAL,  no2_imputed INTEGER DEFAULT 0,  no2_qc_flag TEXT DEFAULT 'ok',
    so2_ugm3 REAL,  so2_clean REAL,  so2_imputed INTEGER DEFAULT 0,  so2_qc_flag TEXT DEFAULT 'ok',
    co_ugm3 REAL,   co_clean REAL,   co_imputed INTEGER DEFAULT 0,   co_qc_flag TEXT DEFAULT 'ok',
    o3_ugm3 REAL,   o3_clean REAL,   o3_imputed INTEGER DEFAULT 0,   o3_qc_flag TEXT DEFAULT 'ok',
    source TEXT DEFAULT 'cams-openmeteo-archive',
    is_synthetic INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_cleaned_cams_aq UNIQUE (city, timestamp)
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    run_started_at TEXT NOT NULL,
    run_finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_inserted INTEGER DEFAULT 0,
    error_message TEXT
);


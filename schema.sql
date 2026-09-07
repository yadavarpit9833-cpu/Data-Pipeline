-- schema.sql
CREATE TABLE IF NOT EXISTS raw_cpcb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_imd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_gfs (
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
);

CREATE TABLE IF NOT EXISTS raw_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    raw_data TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
    pm10_raw REAL,
    pm10_clean REAL,
    pm10_imputed INTEGER DEFAULT 0,
    no2_raw REAL,
    no2_clean REAL,
    no2_imputed INTEGER DEFAULT 0,
    so2_raw REAL,
    so2_clean REAL,
    so2_imputed INTEGER DEFAULT 0,
    co_raw REAL,
    co_clean REAL,
    co_imputed INTEGER DEFAULT 0,
    o3_raw REAL,
    o3_clean REAL,
    o3_imputed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned_imd (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station TEXT,
    timestamp TEXT,
    temperature_raw REAL,
    temperature_clean REAL,
    temperature_imputed INTEGER DEFAULT 0,
    humidity_raw REAL,
    humidity_clean REAL,
    humidity_imputed INTEGER DEFAULT 0,
    rainfall_raw REAL,
    rainfall_clean REAL,
    rainfall_imputed INTEGER DEFAULT 0,
    wind_speed_raw REAL,
    wind_speed_clean REAL,
    wind_speed_imputed INTEGER DEFAULT 0,
    wind_dir_raw REAL,
    wind_dir_clean REAL,
    wind_dir_imputed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned_gfs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    cycle TEXT,
    fhr TEXT,
    temperature_raw REAL,
    temperature_clean REAL,
    temperature_imputed INTEGER DEFAULT 0,
    precipitation_raw REAL,
    precipitation_clean REAL,
    precipitation_imputed INTEGER DEFAULT 0,
    u_wind_raw REAL,
    u_wind_clean REAL,
    u_wind_imputed INTEGER DEFAULT 0,
    v_wind_raw REAL,
    v_wind_clean REAL,
    v_wind_imputed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned_firms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lon REAL,
    timestamp TEXT,
    brightness_raw REAL,
    brightness_clean REAL,
    brightness_imputed INTEGER DEFAULT 0,
    confidence_raw TEXT,
    confidence_clean TEXT,
    confidence_imputed INTEGER DEFAULT 0,
    satellite TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

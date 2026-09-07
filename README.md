# Real-Time Environmental Data Pipeline

This repository contains a production-grade real-time environmental data pipeline for India. It fetches data from CPCB (Air Quality), IMD (Weather), NOAA GFS (Forecasts), and NASA FIRMS (Active Fire), cleans the data, and stores it in a PostgreSQL database.

## Architecture

The pipeline consists of four separate fetcher modules orchestrated by a central scheduler:
1. `fetch_cpcb.py`: Fetches Air Quality data from CPCB (fallback WAQI). (Runs every 15 min)
2. `fetch_imd.py`: Fetches real-time weather from IMD API endpoints. (Runs every 60 min)
3. `fetch_gfs.py`: Downloads GFS GRIB2 data from NOAA NOMADS. (Runs every 6 hours)
4. `fetch_firms.py`: Fetches active fire events from NASA FIRMS. (Runs every 20 min)

`cleaning.py` acts as a shared module for outlier detection (3 standard deviations) and imputation (time-based interpolation + spatial KNN). The original raw responses are saved into `raw_*` tables, and cleaned values are stored in `cleaned_*` tables, maintaining the audit trail of exact values received.

## Setup Instructions

### 1. Database Setup

Ensure you have a PostgreSQL database running.

```bash
# Create the database tables
psql -U postgres -d env_data -a -f schema.sql
```

### 2. Environment Variables

Copy the example environment file and fill in your credentials.

```bash
cp .env.example .env
```

You will need two API keys:
1. **WAQI Token**: Serves as a fallback for CPCB data. Get yours from [WAQI Data Platform](https://aqicn.org/data-platform/token/).
2. **FIRMS MAP_KEY**: Required for fetching NASA active fire data. Get yours from [NASA FIRMS API](https://firms.modaps.eosdis.nasa.gov/api/).

GFS and IMD do not require API keys.

### 3. Install Python Dependencies

Note: For `cfgrib` and `xarray` to parse GFS GRIB2 data, you will likely need the `ecCodes` library installed on your system (e.g., `sudo apt-get install libeccodes0` on Debian/Ubuntu, or via `conda install -c conda-forge eccodes`).

```bash
pip install -r requirements.txt
```

### 4. Running the Pipeline

You can run individual fetchers manually to verify them:

```bash
python fetch_cpcb.py
```

To run the continuous pipeline orchestration:

```bash
python scheduler.py
```

## Data Cleaning & Imputation Rules

- **Outlier Detection**: Values beyond 3 standard deviations are flagged and set to `NaN` for imputation.
- **Time-based Imputation**: Missing values (including outliers) are interpolated over time if the gap is less than 3 consecutive readings.
- **Spatial Imputation**: Longer gaps fall back to spatial KNN imputation from nearby stations based on latitude/longitude (if applicable).
- **Imputation Tracking**: Imputed values are never written over original raw data. Clean tables store `*_raw`, `*_clean`, and a boolean `*_imputed` flag.

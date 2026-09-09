# Real-Time Environmental Data Pipeline

This repository contains a production-grade real-time environmental data pipeline for India. It fetches data from CPCB (Air Quality), Open-Meteo (Weather, fallback for IMD), NOAA GFS (Forecasts), and NASA FIRMS (Active Fire), cleans the data, and stores it in a PostgreSQL database.

## Architecture

The pipeline consists of four separate fetcher modules orchestrated by a central scheduler:
1. `fetch_cpcb.py`: Fetches Air Quality data from CPCB (fallback WAQI). (Runs every 15 min)
2. `fetch_weather.py`: Fetches real-time weather using Open-Meteo API as a fallback (the original IMD API requires IP whitelisting). (Runs every 60 min)
3. `fetch_gfs.py`: Downloads GFS GRIB2 data from NOAA NOMADS. (Runs every 6 hours)
4. `fetch_firms.py`: Fetches active fire events from NASA FIRMS. (Runs every 20 min)

`cleaning.py` acts as a shared Quality Control (QC) module. The pipeline preserves original raw responses in `raw_*` tables and stores cleaned values with explicit quality flags in `cleaned_*` tables, maintaining full auditability without discarding real readings or pollution spikes.

## Setup Instructions

### 1. Database Setup

Ensure you have SQLite or PostgreSQL running.

```bash
# Initialize database schema
python db.py
```

### 2. Environment Variables

Copy the example environment file and fill in your credentials.

```bash
cp .env.example .env
```

You will need two API keys:
1. **WAQI Token**: Serves as a fallback for CPCB data. Get yours from [WAQI Data Platform](https://aqicn.org/data-platform/token/).
2. **FIRMS MAP_KEY**: Required for fetching NASA active fire data. Get yours from [NASA FIRMS API](https://firms.modaps.eosdis.nasa.gov/api/).

GFS and Open-Meteo do not require API keys.

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

## Data Quality Control & Imputation Rules

- **Preservation of Real Pollution Spikes**: Flagged readings are **never deleted or set to NaN**. Raw values are preserved in full so downstream models can analyze genuine pollution events.
- **Multi-Stage QC Chain**:
  1. **Range Check (`range_fail`)**: Flags physically impossible readings based on metric limits (e.g., PM2.5 outside 0–1000 µg/m³).
  2. **Step Check (`step_fail`)**: Flags implausibly rapid jumps compared to prior readings at the same station. Uses circular difference arithmetic for wind direction (`wind_dir`).
  3. **Flatline Check (`flatline`)**: Flags values that repeat identically for 12+ consecutive hours (stuck sensors). Ignores consecutive zeros for rainfall/precipitation (`ignore_zero_flatline=True`), as dry spells are genuine weather.
  4. **Log-Space MAD Check (`mad_outlier`)**: Optional skew-robust statistical outlier detection operating in log-space ($\log(x + 1)$) using Median Absolute Deviation.
- **QC Flag Formatting**: Multi-failure results are stored as comma-separated strings (e.g. `"range_fail,step_fail"` or `"ok"`).
- **NaN-Only Imputation**: Interpolation (temporal & spatial KNN) applies **only** to originally missing (`NaN`) values.
- **GFS Forecast Timestamps**: GFS records store explicit `valid_time` (the physical forecast-valid timestamp computed from cycle start + `fhr` offset) separately from `fetched_at` (download audit timestamp), enabling exact temporal deduplication and real-world time matching.



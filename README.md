# Real-Time Environmental Data Pipeline

A real-time environmental data pipeline for India. It fetches air quality, weather,
forecast, satellite and active-fire data, runs a shared quality-control chain over it,
and lands it in a medallion architecture (raw → cleaned → gold) backed by SQLite and a
Parquet data lake.

## Quick start

```bash
pip install -r requirements.txt     # deps (GFS also needs the ecCodes system library)
python db.py                        # create env_data.db from schema.sql
printf 'WAQI_TOKEN=...\nFIRMS_MAP_KEY=...\n' > .env
python scheduler.py                 # run everything
```

Keep the checkout **out of OneDrive** — see [Location](#1-location). Without API keys
the CPCB and FIRMS jobs log a clean failure and skip; the other four still run.

To verify a single source instead of the whole pipeline:

```bash
python fetch_weather.py             # no API key needed
python monitor_health.py            # per-source freshness report
```

## How data flows

```mermaid
flowchart LR
    CPCB[CPCB via WAQI]:::src --> RAW
    FIRMS[NASA FIRMS]:::src --> RAW
    OM[Open-Meteo]:::src --> RAW
    GFS[NOAA GFS GRIB2]:::src --> RAW
    CAMS[CAMS / Sentinel-5P]:::src --> RAW

    RAW["<b>Bronze — raw</b><br/>raw_* tables<br/>data/raw/"]:::bronze
    RAW -->|cleaning.py QC chain| QC
    QC{"range · step<br/>flatline · log-MAD"}:::qc
    QC -->|contracts.py Pandera| CLEAN
    CLEAN["<b>Silver — cleaned</b><br/>cleaned_* tables<br/>data/cleaned_*.parquet"]:::silver
    CLEAN -->|gold_layer.py| GOLD
    GOLD["<b>Gold — analysis-ready</b><br/>city_aqi_hourly<br/>city_daily_summary<br/>gfs_grid_hourly"]:::gold

    QC -.->|failed rows| CSV["contract_failures_*.csv"]:::fail

    classDef src fill:#c3d0e4,stroke:#5b7ba8,color:#1a2b45
    classDef bronze fill:#dcc5ac,stroke:#9c6a3d,color:#3d2a17
    classDef silver fill:#ccd1d7,stroke:#7c858e,color:#212529
    classDef gold fill:#e2cf95,stroke:#b58d12,color:#3d3000
    classDef qc fill:#d4c3e6,stroke:#8265ad,color:#2e1a45
    classDef fail fill:#e5bdbd,stroke:#b34a4a,color:#4a1010
```

Nothing is discarded along the way: raw payloads stay verbatim, QC failures are
*flagged* rather than deleted, and gold is derived output that is rebuilt every run.

## Architecture

Five fetchers plus a gold-layer build, orchestrated by `scheduler.py`:

| Job | Module | Source | Cadence |
|---|---|---|---|
| CPCB Air Quality | `fetch_cpcb.py` | CPCB via WAQI | every 15 min |
| FIRMS Active Fires | `fetch_firms.py` | NASA FIRMS (VIIRS + MODIS) | every 20 min |
| Open-Meteo Weather | `fetch_weather.py` | Open-Meteo | every 60 min |
| Medallion Gold Layer | `gold_layer.py` | derived from cleaned data | every 60 min |
| GFS Grid Weather | `fetch_gfs.py` | NOAA NOMADS GRIB2 | 00/06/12/18 UTC + 30 min |
| Sentinel-5P TROPOMI | `fetch_sentinel5p.py` | CAMS via Open-Meteo | daily, 12:00 UTC |

Note on naming: the `imd` tables hold **Open-Meteo** data, not Indian government IMD
data — the real IMD API requires IP whitelisting. Likewise, Sentinel-5P rows come from
the CAMS/Open-Meteo composite rather than direct TROPOMI L2 granules.

### Medallion layers

- **Bronze / raw** — `raw_*` tables and `data/raw/<source>/date=…/`. Original payloads,
  preserved verbatim for auditability.
- **Silver / cleaned** — `cleaned_*` tables and `data/cleaned_<source>/*.parquet`.
  QC-flagged values; nothing is discarded.
- **Gold** — `data/gold/`. Analysis-ready features, rebuilt on every run and never
  primary storage:
  - `city_aqi_hourly/date=YYYY-MM-DD.parquet` — hourly city AQI (India CPCB breakpoints)
  - `gfs_grid_hourly/cycle=<cycle>.parquet` — hourly GFS grid aggregates
  - `city_daily_summary/date=YYYY-MM-DD.parquet` — daily pollutants **left-joined** with
    daily weather on the case-folded city name

#### Sample gold output

Real rows the pipeline produced, abridged to the columns that matter most.

**`city_aqi_hourly`** — `aqi_category` is derived from the aggregated `aqi_max`,
so the two can never disagree:

| city | pm25_mean | aqi_max | aqi_category | n_stations |
|---|---|---|---|---|
| delhi | 112.0 | 273.6 | Poor | 1 |

Also carries `hour_bucket`, `pm10_mean`, `no2_mean` and `computed_at`.

**`city_daily_summary`** — pollutants left-joined with weather. `wind_dir_daily_mean`
is a circular mean, and `n_weather_obs` shows how many weather rows backed the join
(null for a city with no matching station):

| city | daily_aqi | daily_aqi_category | temp_daily_mean | wind_dir_daily_mean | n_weather_obs |
|---|---|---|---|---|---|
| delhi | 273.6 | Poor | 28.32 | 106.4 | 5 |

Also carries the six `*_daily_mean` pollutant columns, `humidity_daily_mean`,
`rainfall_daily_total`, `temp_daily_max/min`, `wind_speed_daily_mean` and `n_obs`.

**`gfs_grid_hourly`** — one row per cycle and valid time over the India bounding box:

| cycle | temp_mean | temp_max | precip_total | n_grid_points |
|---|---|---|---|---|
| 20260914_00z | 20.74 | 33.23 | 0.0 | 14625 |

Also carries `valid_time`, `temp_min` and `wind_speed_mean`.

### Supporting modules

| Module | Role |
|---|---|
| `cleaning.py` | Shared QC chain (range, step, flatline, log-MAD) |
| `contracts.py` | Pandera data contracts; violations logged to `contract_failures_<source>.csv` |
| `storage.py` | Parquet data lake, read-merge-write with atomic rename |
| `db.py` | Connection + schema management, SQLite or Postgres |
| `run_logger.py` | Writes `pipeline_run_log` rows from any execution context |
| `monitor_health.py` | Per-source freshness and success/failure report |

## Setup

### 1. Location

Keep the checkout **outside OneDrive** (or any sync client). OneDrive locks
`env_data.db` and the `data/*.parquet` files mid-write, producing
`database is locked` and `PermissionError` on the atomic Parquet rename. A plain local
path such as `D:\DataPipeline` is fine.

Paths inside the pipeline resolve against the module directory, not the caller's
working directory, so scripts work when launched from anywhere.

### 2. Dependencies

```bash
pip install -r requirements.txt
```

`cfgrib` and `xarray` need the `ecCodes` system library to parse GFS GRIB2
(`sudo apt-get install libeccodes0`, or `conda install -c conda-forge eccodes`).

### 3. Database

SQLite is the default and needs no server — the file is created for you:

```bash
python db.py
```

For PostgreSQL instead, set `DB_ENGINE=postgres` plus the `DB_*` variables in `.env`.
`db.py` translates placeholders and conflict clauses between the two dialects.

### 4. API keys

Create a `.env` in the repo root:

```
WAQI_TOKEN=your_waqi_token_here
FIRMS_MAP_KEY=your_firms_map_key_here
```

- **WAQI Token** — CPCB air quality. [aqicn.org/data-platform/token](https://aqicn.org/data-platform/token/)
- **FIRMS MAP_KEY** — NASA active fire. [firms.modaps.eosdis.nasa.gov/api](https://firms.modaps.eosdis.nasa.gov/api/)

GFS, Open-Meteo and CAMS need no keys. Missing or placeholder keys make the affected
fetcher log a clean failure and skip — the pipeline does not crash. `.env` is gitignored.

### 5. Running

See [Quick start](#quick-start) for the short version. Any fetcher runs standalone and
logs its own `pipeline_run_log` row:

```bash
python fetch_cpcb.py
```

The full orchestration in the foreground — Ctrl+C to stop:

```bash
python scheduler.py
```

To leave it running unattended, register it as a background task instead (next section).

## Running as a Windows background task

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_background_task.ps1
```

Registers `DataPipelineScheduler` to start at logon and run indefinitely. Two details
matter, both learned the hard way:

- **It runs headless under `pythonw.exe`.** `python.exe` allocates a console, and the
  logon sequence tears that console down moments later, killing the scheduler with
  `0xC000013A` (`STATUS_CONTROL_C_EXIT`) before it writes a single log line. `pythonw`
  has no console, so nothing can close it. Logging is unaffected: `scheduler.py` always
  writes `scheduler.log` via a `FileHandler`, and attaches a console handler only when a
  real stream exists.
- **`ExecutionTimeLimit` is 0 (unlimited).** The default is `PT72H`, which is why a
  long-running task appears to "disappear" after exactly three days.

Tasks register at `RunLevel Limited` — the pipeline only makes HTTP calls and writes
inside the repo. Re-registering a task that was first created from an elevated shell
requires an elevated shell again, because its task file is owned by Administrators.

### Health monitoring

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_monitor_task.ps1
```

Registers `DataPipelineSchedulerMonitor` to run `check_task_alive.ps1` every 30 minutes.
For a continuously-running scheduler only the `Running` state counts as alive — `Ready`
means the process has exited. Alerts append to `task_health.log` and a file on the
Desktop. Pass `-AutoRestart` to have the probe restart a dead scheduler instead of only
recording it.

For a point-in-time view of every source:

```bash
python monitor_health.py
```

## Historical backfill

The live fetchers only reach near-real-time feeds, so the pipeline holds nothing
from before the day it started. Two scripts load history instead, both defaulting
to the same window — January, October, November and December of 2020–2025 —
so fire and weather line up and can actually be compared.

### Fire — `backfill_firms.py`

FIRMS NRT spans roughly the last two months, so historical fire data needs the
Standard Processing (SP) archives:

```bash
python backfill_firms.py                       # Jan/Oct/Nov/Dec of 2020-2025
python backfill_firms.py --years 2023 2024     # specific years
python backfill_firms.py --months 10 11        # specific months
python backfill_firms.py --dry-run             # print the plan, fetch nothing
```

The default window is January, October, November and December of 2020-2025 — the
months that matter for stubble burning and winter pollution. `--sources` selects
the sensors; the default pair mirrors the live fetcher.

| Source | Archive coverage | Resolution |
|---|---|---|
| `VIIRS_SNPP_SP` | 2012-01-20 onward | 375 m |
| `MODIS_SP` | 2000-11-01 onward | 1 km |
| `VIIRS_NOAA20_SP` | 2018-04-01 onward | 375 m |

Availability is checked against the FIRMS `data_availability` endpoint before any
month is requested, so a period the archive cannot serve is skipped up front rather
than failing mid-run.

Three things differ from the live fetcher, deliberately:

- **The area API caps `day_range` at 5**, not 10. Each month is split into chunks
  that cover the tail as well, so a 31-day month ends with a single-day chunk
  rather than losing the 31st.
- **Parquet partitions by observation date**, not fetch date, so historical rows
  land in the partition they belong to.
- **Rows are written with `executemany`.** One month of VIIRS runs to tens of
  thousands of detections; the live fetcher's per-row loop does not keep up.

Re-running is safe. Inserts are idempotent on the same keys the live fetcher uses
and Parquet partitions are read-merge-written, so an interrupted run can simply be
started again.

Measured on January 2020: 42,502 rows in 50 seconds, zero contract failures, full
month coverage. Burning-season months run several times larger — November 2020
returned 113,659 rows against January's 42,502.

Rate limits are worth a glance before a large run; the MAP_KEY status endpoint
reports the current budget:

```bash
curl "https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY=$FIRMS_MAP_KEY"
```

### Weather — `backfill_weather.py`

Open-Meteo's reanalysis archive reaches back to 1940 and needs no API key:

```bash
python backfill_weather.py                     # Jan/Oct/Nov/Dec of 2020-2025
python backfill_weather.py --years 2023 2024
python backfill_weather.py --dry-run
```

It polls the same 11 stations as `fetch_weather.py` and applies the same QC
thresholds, including the circular check on wind direction.

**`wind_speed_unit=ms` is not optional.** Open-Meteo returns km/h by default while
the live fetcher asks for m/s. Mixing the two in one column would corrupt any
analysis spanning live and backfilled rows without ever raising an error.

Contiguous months collapse into a single request, so October–December is one span
rather than three: 132 requests against the 324 the FIRMS backfill needs, because
the archive endpoint takes a date range where the FIRMS area API caps at five days.

Measured: 194,832 rows for the default window in about five minutes, zero contract
failures — 11 stations × 123 days × 24 hours × 6 years.

## Analysis

`analyze_fire_weather.py` joins daily FIRMS detections inside a lat/lon box to one
station's daily weather and reports Pearson and Spearman — pooled, inside the
burning window, and per year — plus a rain-suppression test.

```bash
python analyze_fire_weather.py                            # stubble belt vs Delhi
python analyze_fire_weather.py --station Lucknow
python analyze_fire_weather.py --belt 24 31 74 88 --years 2023 2025
```

It needs both backfills to have run over the same window. Three things decide
whether any number it prints means anything:

- **Pooled figures are confounded by season.** January is cold and quiet, November
  warm and peaking, so a pooled temperature correlation mostly measures the
  calendar. Pooled temperature reads `+0.65` Spearman and collapses to `-0.12`
  inside October–November. Read the burning window.
- **A correlation is only trustworthy if its sign holds across years.** That is what
  the per-year table is for. Rainfall stays negative in every year that had rain
  (−0.28 to −0.55); temperature flips sign three times and is therefore noise.
- **Cloud cover suppresses detections as well as burning.** Rain days are cloudy
  days and the sensor sees less through cloud, so the rain result mixes a real
  effect with an observational artefact. Separating them needs cloud-mask data this
  pipeline does not carry.

The strongest signal in the default window is rain: median 45 detections the day
after rain against 318 after a dry day, a ratio of 0.14.

One structural caveat — the station list has nothing in Punjab, so Delhi stands in
at roughly 250 km from the belt.

## Data quality control

- **Real pollution spikes are preserved.** Flagged readings are never deleted or set to
  NaN; raw values survive in full so downstream models can analyse genuine events.
- **Multi-stage QC chain:**
  1. **Range (`range_fail`)** — physically impossible readings (PM2.5 outside 0–1000 µg/m³).
  2. **Step (`step_fail`)** — implausible jumps against the prior reading at the same
     station. Uses circular difference arithmetic for `wind_dir`.
  3. **Flatline (`flatline`)** — identical values for 12+ consecutive hours (stuck
     sensors). Consecutive zeros are ignored for rainfall, since dry spells are real.
  4. **Log-space MAD (`mad_outlier`)** — optional skew-robust outlier detection in
     `log(x + 1)` space using Median Absolute Deviation.
- **QC flags** are comma-separated: `"ok"`, or `"range_fail,step_fail"`.
- **NaN-only imputation** — temporal and spatial KNN interpolation touches *only*
  originally missing values.
- **GFS timestamps** — records carry `valid_time` (cycle start + `fhr`) separately from
  `fetched_at` (download audit time), enabling exact temporal deduplication.
- **Data contracts** — every cleaned frame is validated against a Pandera schema before
  it lands. Failing rows are dropped from the batch and written to
  `contract_failures_<source>.csv` rather than silently polluting training data.

### Aggregation rules worth knowing

- **AQI category is derived from the aggregate it labels.** `aqi_category` is computed
  from the aggregated `aqi_max`, not reduced from per-station categories — a mode over
  per-station labels can contradict the max it sits beside.
- **Wind direction averages circularly.** The daily mean bearing uses a vector mean; an
  arithmetic mean of 350° and 10° gives 180° (due south) when the answer is 0° (north).
- **The daily summary join is a LEFT join.** A city with pollution data but no matching
  weather station keeps its row with null weather columns instead of vanishing. The
  weather column set is fixed, so the output schema is stable whether or not weather
  data was available.

## Idempotency

Every table has a uniqueness constraint and every insert is `INSERT OR IGNORE`
(translated to `ON CONFLICT DO NOTHING` for Postgres), so re-running a fetcher never
duplicates rows:

| Table | Unique on |
|---|---|
| `raw_cpcb`, `raw_imd`, `raw_firms` | `(timestamp, raw_data_hash)` |
| `raw_gfs`, `cleaned_gfs` | `(lat, lon, cycle, fhr)` |
| `cleaned_cpcb` | `(station_id, timestamp)` |
| `cleaned_imd` | `(station, timestamp)` |
| `cleaned_firms` | `(lat, lon, timestamp, satellite)` |
| `raw_sentinel5p`, `cleaned_sentinel5p` | `(lat, lon, timestamp)` |

Parquet writes use read-merge-write with the same dedup keys, then an atomic rename.

## Tests

```bash
python -m unittest test_cleaning test_idempotency
```

19 tests: the QC chain (`test_cleaning.py`) and database idempotency including GFS
`valid_time` computation (`test_idempotency.py`). The idempotency suite builds its test
database from `schema.sql`, so any table must be declared there to be covered.

## Utility scripts

| Script | Purpose |
|---|---|
| `query_db.py` | Row counts for every table |
| `diagnose_gfs.py` | GFS coverage, cycles and valid times |
| `inspect_firms.py` | FIRMS entries and duplicate check |
| `backfill_firms.py` | Load historical FIRMS fires from the SP archives |
| `backfill_weather.py` | Load historical weather from the Open-Meteo archive |
| `analyze_fire_weather.py` | Correlate belt fire counts against station weather |
| `normalize_gfs_cycles.py` | Backfill/normalise GFS cycle labels |
| `migrate_db.py` | Rebuild tables against the current schema |
| `migrate_sqlite_to_parquet.py` | Export existing SQLite rows into the Parquet lake |

## Known limitations

- **Sleep creates gaps.** APScheduler defaults apply (`misfire_grace_time=1`,
  `coalesce=True`), so runs that come due while the machine sleeps are skipped, not
  backfilled. Raise `misfire_grace_time` on a job if you need catch-up behaviour.
- **`is_synthetic` is row-level, not column-level**, so it cannot express a partial
  fallback where some fields are real and others synthetic.
- **WAQI returns stale observation timestamps for some stations**, occasionally months
  old. These are stored at their reported time, so they land in older date partitions
  and will not join against current weather.

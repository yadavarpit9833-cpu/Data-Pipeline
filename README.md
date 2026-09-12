# Real-Time Environmental Data Pipeline — India

Ingests air quality, weather, forecast and active-fire data for India, runs a
multi-stage quality-control chain over it, and stores both the raw payloads and
the QC-flagged values in a medallion (bronze / silver / gold) layout.

The thing this project is actually good at is **not** fetching — anyone can
call an API. It is the quality-control and lineage layer: every reading keeps
its original value, carries an explicit QC flag, records whether it was
imputed, and is traceable back to the exact payload it came from.

---

## What the data really is

Tables are named after the provider the data actually comes from, and value
columns carry their units. Read [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md)
before using any of it — the short version:

| Table | Real source | Unit | Notes |
|---|---|---|---|
| `cleaned_waqi` | waqi.info (republishes CPCB) | **US EPA AQI index, 0–500** | Not µg/m³ |
| `cleaned_weather` | Open-Meteo | °C, %, mm, m/s | **Not IMD** — IMD's API needs IP whitelisting |
| `cleaned_gfs` | NOAA NCEP GFS 0.25° | °C, mm, m/s | Real forecasts at multiple lead times |
| `cleaned_firms` | NASA FIRMS (MODIS + VIIRS) | K, MW | Two sensors, two confidence scales |
| `cleaned_cams` | Copernicus CAMS via Open-Meteo | **µg/m³** | Model output, **not** Sentinel-5P |

---

## Architecture

```
   ┌──────────────┐
   │  scheduler   │  APScheduler, one process, staggered job starts
   └──────┬───────┘
          │
   ┌──────┴───────────────────────────────────────────┐
   │  fetch_waqi    every 30 min                      │
   │  fetch_firms   every 20 min                      │   bronze
   │  fetch_weather every 60 min                      │   raw payloads,
   │  fetch_gfs     05/11/17/23:15 UTC                │   content-addressed
   │  fetch_cams    daily 12:30 UTC                   │
   └──────┬───────────────────────────────────────────┘
          │
   ┌──────┴───────┐   cleaning.py   range → step → flatline → optional log-MAD
   │  QC + flags  │   contracts.py  pandera schema per source
   └──────┬───────┘                                          silver
          │                              SQLite/Postgres + Parquet
   ┌──────┴───────┐
   │  gold_layer  │  hourly city AQI, GFS grid summaries, daily city summary
   └──────────────┘                                          gold (DuckDB)
```

---

## Quick start

### With Docker (recommended — ecCodes is preinstalled)

```bash
cp .env.example .env    # then fill in WAQI_TOKEN and FIRMS_MAP_KEY
docker compose up --build
```

### Locally

```bash
# ecCodes is a SYSTEM library; cfgrib cannot decode GFS GRIB2 without it
sudo apt-get install -y libeccodes0 libeccodes-data   # Debian/Ubuntu
# brew install eccodes                                # macOS
# conda install -c conda-forge eccodes                # conda

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env     # fill in the two API keys
python db.py             # create the schema
python scheduler.py      # run continuously
```

Run a single fetcher to check it end to end:

```bash
python fetch_waqi.py
python fetch_gfs.py
```

Check pipeline health (exits non-zero when anything is stale):

```bash
python monitor_health.py
python monitor_health.py --json
```

### API keys

| Key | Needed for | Where |
|---|---|---|
| `WAQI_TOKEN` | air quality | https://aqicn.org/data-platform/token/ |
| `FIRMS_MAP_KEY` | active fires | https://firms.modaps.eosdis.nasa.gov/api/map_key/ |

NOAA GFS and Open-Meteo need no key.

---

## Quality control

The guiding rule: **a flagged value is never deleted, zeroed or replaced.**
Delhi's winter PM spikes are real data, and a QC chain that smooths them away
destroys exactly the events the pipeline exists to capture. Flags describe the
reading; they do not censor it.

| Check | Flag | What it catches |
|---|---|---|
| Range | `range_fail` | Physically impossible values, per parameter |
| Step | `step_fail` | Implausible jumps vs the previous reading at the **same** station or gridpoint. Uses circular arithmetic for wind direction, so 350° → 10° is a 20° change, not 340°. |
| Flatline | `flatline` | A value repeating for 12+ consecutive readings (stuck sensor). Consecutive zeros are ignored for rainfall — a dry spell is real weather. |
| Log-MAD | `mad_outlier` | Optional skew-robust outlier detection in log space |

Multiple failures are stored comma-separated: `range_fail,step_fail`, or `ok`.

Interpolation only ever fills values that were **originally missing** (`NaN`),
and `*_imputed` records every fill. Spatial KNN is used only where it is
physically defensible — never for fire detections, and never for forecast grid
fields, where a missing value means the model produced none.

`contracts.py` validates every cleaned frame against a pandera schema before it
is written. Rows that violate the contract are dropped and appended to
`data/contract_failures/<source>_<date>.csv`.

---

## AQI: two scales, kept apart

`aqi.py` holds both and they are not interchangeable:

- **`combine_subindices`** — for values that are *already* indices (everything
  WAQI returns). The overall AQI is the worst sub-index.
- **`cpcb_subindex` / `cpcb_aqi_from_concentrations`** — for mass
  concentrations in µg/m³ (CAMS). India CPCB breakpoints, CPCB categories, and
  CPCB's own completeness rule: at least three pollutants including PM, at
  least 16 of 24 hours, or the AQI is `insufficient_data` rather than a number.

Running the first through the second converts an AQI into an AQI. That bug is
what most of `aqi.py`'s documentation exists to prevent.

---

## Configuration

Everything is environment driven; see [`.env.example`](.env.example). The knobs
worth knowing:

| Variable | Default | What it does |
|---|---|---|
| `DB_ENGINE` | `sqlite` | `sqlite` or `postgres` (the schema is translated for both) |
| `GFS_FORECAST_HOURS` | `000,006,012,024` | Forecast lead times. Each adds ~14,625 rows per cycle. |
| `GFS_GRID_STRIDE` | `1` | `2` = 0.5°, `4` = 1.0°. Raise it for a laptop demo. |
| `GFS_PUBLICATION_LAG_HOURS` | `5` | How long to wait before asking NOMADS for a cycle |
| `GOLD_LOOKBACK_DAYS` | `3` | How much history each gold rebuild touches |
| `MIN_HOURS_FOR_DAILY_AQI` | `16` | CPCB's completeness rule |

---

## Tests

```bash
python -m unittest discover -s . -p "test_*.py" -v
```

`test_cleaning.py` covers the QC chain, `test_aqi.py` the index maths,
`test_pipeline_fixes.py` is a regression suite pinning each data-correctness
bug that was found and fixed, and `test_idempotency.py` proves that reruns do
not duplicate rows.

CI runs all of it on every push — see `.github/workflows/ci.yml`.

---

## Repository layout

```
fetch_waqi.py      fetch_weather.py   fetch_gfs.py
fetch_firms.py     fetch_cams.py                     ingestion
cleaning.py        contracts.py       aqi.py         quality + domain logic
db.py              storage.py         schema.sql     persistence
scheduler.py       run_logger.py      monitor_health.py   operations
gold_layer.py                                        analytics
scripts/           one-off migrations and inspection helpers
docs/              data source, unit and licence reference
```

---

## Known limitations

Stated plainly, because a pipeline that hides these is worse than one that
does not have them:

1. **WAQI is not CPCB.** It republishes CPCB, in index units, roughly hourly.
   For station-level µg/m³, add an OpenAQ v3 fetcher.
2. **Open-Meteo is not IMD.** See `docs/DATA_SOURCES.md` for how to get IMD
   access if you need it.
3. **CAMS is model output, not measurement.** Use it as a spatial prior.
4. **The weather-to-city join is by station name**, not spatial matching.
   Adequate for eleven stations; it would need real nearest-neighbour matching
   at any larger scale.
5. **There is no ML yet, on purpose.** Any forecasting model added here should
   first be measured against a persistence baseline before anything more
   elaborate is justified.

---

## Data attribution

Air quality index data from the [World Air Quality Index project](https://waqi.info),
which republishes CPCB station data. Weather data from
[Open-Meteo](https://open-meteo.com) (CC-BY-4.0). Forecast data from
[NOAA NCEP GFS](https://nomads.ncep.noaa.gov) (public domain). Active fire data
from [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov). Atmospheric
composition from Copernicus CAMS via Open-Meteo.

## Licence

MIT — see [LICENSE](LICENSE).

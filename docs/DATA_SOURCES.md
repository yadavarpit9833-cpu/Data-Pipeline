# Data sources, units and licences

Every table is named after the provider the data **actually** comes from, and
every value column carries its unit. This document is the reference for what
each source really is, because the earlier naming caused real numeric bugs:
a table called `cleaned_cpcb` held WAQI AQI indices that the gold layer then
converted to AQI a second time, and a table called `cleaned_sentinel5p` held
CAMS model output in µg/m³ under column names ending in `_ppb`.

---

## 1. WAQI — `raw_waqi`, `cleaned_waqi`

| | |
|---|---|
| **Provider** | World Air Quality Index project, https://waqi.info |
| **Endpoint** | `https://api.waqi.info/feed/{city}/?token={TOKEN}` |
| **Auth** | Free token from https://aqicn.org/data-platform/token/ |
| **Update cadence** | Roughly hourly per station |
| **Unit** | **US EPA AQI sub-index (0–500), not µg/m³** |
| **Licence** | Attribution required. See https://aqicn.org/api/ — non-commercial use of the free token. |

WAQI aggregates and republishes CPCB station readings for India, but its
`iaqi` block reports **already-computed AQI sub-indices**, not concentrations.
The columns are therefore `pm25_aqi_raw`, `pm10_aqi_raw` and so on, and the
`aqi_scale` column records `us_epa`.

**Do not run these values through `aqi.cpcb_subindex`.** Combine them with
`aqi.combine_subindices`, which takes the maximum — that is what an overall
AQI is.

### If you need real concentrations

This is not a CPCB feed. For CPCB station data in µg/m³, use
[OpenAQ v3](https://docs.openaq.org/) (free API key, mirrors CPCB at station
level in mass units). That is a new fetcher, not a rename of `fetch_waqi.py`.

---

## 2. Open-Meteo forecast — `raw_weather`, `cleaned_weather`

| | |
|---|---|
| **Provider** | Open-Meteo, https://open-meteo.com |
| **Endpoint** | `https://api.open-meteo.com/v1/forecast` |
| **Auth** | None |
| **Update cadence** | Hourly |
| **Units** | °C, %, mm, m/s, degrees — encoded in the column names |
| **Licence** | CC-BY-4.0. Attribution required. |

**This is not IMD data.** IMD's own endpoint
(`https://mausam.imd.gov.in/api/current_wx_api.php?id={STATION_ID}`) exists and
returns JSON but responds HTTP 401 — "Your IP/Domain needs to be whitelisted".
To get access, write to help.mausam@imd.gov.in or deploy from an already
whitelisted institutional network.

Open-Meteo is queried at the coordinates of eleven Indian WMO stations, so the
geography is right even though the provider is not IMD.

---

## 3. NOAA GFS — `raw_gfs`, `cleaned_gfs`, `gfs_fetch_manifest`

| | |
|---|---|
| **Provider** | NOAA NCEP, https://nomads.ncep.noaa.gov |
| **Endpoint** | `filter_gfs_0p25.pl` subregion filter |
| **Auth** | None (rate limited by IP) |
| **Cycles** | 00, 06, 12, 18 UTC |
| **Publication lag** | Roughly 3.5–5 hours after the nominal cycle time |
| **Resolution** | 0.25° — 125 × 117 = 14,625 gridpoints over India |
| **Units** | °C (converted from K), mm, m/s |
| **Licence** | US Government work, public domain. Attribution appreciated. |

### Two things to know

**Cycle selection.** `fetch_gfs.latest_available_cycle()` subtracts
`GFS_PUBLICATION_LAG_HOURS` from the wall clock before snapping to a 6-hourly
boundary. Asking for a cycle sooner than that returns an HTML error page with
HTTP 200, not GRIB. The old code hardcoded the 00z cycle and ran at 00:30 UTC,
which was always too early.

**GRIB2 decoding needs ecCodes and xarray.** `cfgrib` wraps ECMWF's ecCodes C
library. Current `eccodes` wheels (2.41+) ship that library inside the wheel via
`eccodeslib`, so `pip install -r requirements.txt` is normally sufficient — this
was verified on a container with no system `libeccodes` present. Only if pip has
no wheel for your platform do you need the system package:

```bash
sudo apt-get install -y libeccodes0 libeccodes-data   # Debian/Ubuntu
brew install eccodes                                  # macOS
conda install -c conda-forge eccodes                  # conda
```

Note that `xarray` is required too: `cfgrib` imports without it but only exposes
`open_datasets` once xarray is importable. Without ecCodes or xarray,
`fetch_gfs` raises a clear, actionable error. It does **not** fall back to synthetic data — the previous version
wrote a hardcoded 25 °C grid on any failure, and because of the
`INSERT OR IGNORE` on `UNIQUE(lat, lon, cycle, fhr)` those fake rows then
blocked the real data for the rest of the day.

The GRIB2 payload itself is archived in `data/raw/gfs/` and referenced from
`gfs_fetch_manifest`, one row per downloaded file.

---

## 4. NASA FIRMS — `raw_firms`, `cleaned_firms`

| | |
|---|---|
| **Provider** | NASA FIRMS, https://firms.modaps.eosdis.nasa.gov |
| **Endpoint** | `/api/area/csv/{MAP_KEY}/{SENSOR}/{bbox}/{days}` |
| **Auth** | Free MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/map_key/ |
| **Sensors** | `VIIRS_SNPP_NRT`, `VIIRS_NOAA20_NRT`, `MODIS_NRT` |
| **Licence** | Free and open. Attribution required — see the FIRMS citation guidance. |

### Column and scale differences between sensors

This is where the worst silent bug lived. The two sensor families do not use
the same column names or the same confidence scale:

| | MODIS | VIIRS |
|---|---|---|
| Brightness column | `brightness` | `bright_ti4` |
| Secondary channel | `bright_t31` | `bright_ti5` |
| Confidence | 0–100 percent | `l` / `n` / `h` |

Only `brightness` used to be mapped, so every VIIRS row arrived with a null
brightness — and the spatial KNN imputer then filled it from the nearest MODIS
detection, copying a fire temperature in Delhi onto a fire in Kolkata and
marking it `imputed = 1`.

Both names are mapped now, and **spatial imputation is disabled for FIRMS**.
A fire is a point event; interpolating its brightness from a neighbouring fire
is meaningless at any distance.

Confidence keeps its raw value plus a `confidence_scale`
(`percent` / `class`) and a shared `confidence_class`
(`low` / `nominal` / `high`).

**MAP_KEY safety.** FIRMS puts the key in the URL path, and the retry handler
used to log the full URL on every failure. `redact_key()` strips it now. If
your key has ever appeared in a log or a screenshot, rotate it.

### NRT vs SP, and why they are not different sensors

FIRMS names its API sources `<FAMILY>_<STREAM>`:

| Stream | Meaning | Latency |
|---|---|---|
| `NRT` | Near real time | minutes to hours |
| `SP` | Standard Processing — the reprocessed archive | months behind |

**SP is the same detections, reprocessed.** If `sensor` held the whole source
string, one physical fire would be stored twice — once as `MODIS_NRT` when the
live fetcher saw it and again as `MODIS_SP` when the archive backfill reached
that date — silently doubling the fire counts any model trains on.

So `sensor` holds the FAMILY (`MODIS`, `VIIRS_SNPP`, `VIIRS_NOAA20`) and
`processing` holds the stream. The uniqueness constraint is on the family, and
`scripts/backfill_firms_archive.py` upserts with
`WHERE excluded.processing = 'SP'`: SP supersedes an NRT row, and a later NRT
fetch can never downgrade an SP row back.

### Historical backfill

| | |
|---|---|
| **Endpoint** | `/api/area/csv/{MAP_KEY}/{SOURCE}/{bbox}/{DAY_RANGE}/{START_DATE}` |
| **DAY_RANGE** | **1–5** days per request |
| **START_DATE** | returns `START_DATE .. START_DATE + DAY_RANGE - 1` |
| **Rate limit** | 5000 requests per 10-minute window, per MAP_KEY |

> **DAY_RANGE is 5, not 10.** NASA's own API page documents `1..10`. The live
> server rejects anything above 5 with
> `HTTP 400 — Invalid day range. Expects [1..5].`
> Verified against the endpoint on 2026-09-13. The server wins.

### Coverage, as reported by the API itself

Do not hardcode these — `/api/data_availability/csv/{MAP_KEY}/ALL` returns them,
and they move as NASA reprocesses. Read on 2026-09-13:

| Source | From | To |
|---|---|---|
| `MODIS_SP` | 2000-11-01 | 2026-05-31 |
| `VIIRS_SNPP_SP` | 2012-01-20 | 2026-04-27 |
| `VIIRS_NOAA20_SP` | 2018-04-01 | 2026-05-31 |
| `VIIRS_NOAA21_NRT` | 2024-01-17 | current |
| `MODIS_NRT` | 2026-06-01 | current |
| `VIIRS_SNPP_NRT` | 2026-04-28 | current |
| `VIIRS_NOAA20_NRT` | 2026-06-01 | current |

Two things follow. The **SP archives lag the present by three to four months**,
so recent weeks exist only in NRT. And the **NRT streams reach back only a few
months**, so falling back from SP to NRT is worth a request near the present and
is guaranteed to waste one for any historical date.

`scripts/backfill_firms_archive.py` plans the chunks, skips dates before an
instrument existed, falls back from SP to NRT where the archive has not been
produced yet, and records every chunk in `firms_backfill_manifest` so an
interrupted run resumes instead of restarting.

Sources: [FIRMS Area API](https://firms.modaps.eosdis.nasa.gov/api/area/),
[FIRMS API in Python](https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html),
[FIRMS Data Availability API](https://firms.modaps.eosdis.nasa.gov/api/data_availability/)

---

## 5. Copernicus CAMS — `raw_cams`, `cleaned_cams`

| | |
|---|---|
| **Provider** | Copernicus CAMS, served via Open-Meteo Air Quality API |
| **Endpoint** | `https://air-quality-api.open-meteo.com/v1/air-quality` |
| **Auth** | None |
| **Unit** | **µg/m³ surface mass concentration** |
| **Licence** | CAMS data: Copernicus licence, attribution required. Open-Meteo: CC-BY-4.0. |

Formerly and wrongly called `sentinel5p`. It contains **no** Sentinel-5P
TROPOMI data: no TROPOMI product was ever requested, the CDSE constants were
dead code, and the docstring's claim that TROPOMI needs no authentication was
false — the Copernicus Open Access Hub it named has been retired and its
replacement, CDSE, requires registration.

Because CAMS really is in µg/m³, this is the one source from which a genuine
**India CPCB National AQI** can be computed, and `gold_layer` does exactly
that via `aqi.cpcb_aqi_from_concentrations`.

Note that CAMS is a **model reanalysis/forecast**, not a measurement. Treat it
as a spatial prior, not as ground truth.

---

## AQI scales: the one thing not to get wrong

Two different scales appear in this pipeline.

| | US EPA | India CPCB |
|---|---|---|
| Used for | WAQI values (already an index) | CAMS concentrations (computed by us) |
| 101–200 | split into "Unhealthy for Sensitive Groups" and "Unhealthy" | one band, "Moderate" |
| Averaging | as published by WAQI | 24-hour for PM/NO2/SO2, 8-hour for CO/O3 |
| Completeness | as published | ≥ 3 pollutants including PM, ≥ 16 hours |

`aqi.py` keeps both. `combine_subindices` for values that are already indices;
`cpcb_subindex` only ever for mass concentrations. Applying the second to the
output of the first is the double-conversion bug this pipeline used to ship.

---

## Attribution block for the demo and the report

> Air quality index data from the World Air Quality Index project
> (https://waqi.info), which republishes CPCB station data.
> Weather data from Open-Meteo (https://open-meteo.com), CC-BY-4.0.
> Forecast data from NOAA NCEP GFS (https://nomads.ncep.noaa.gov), public domain.
> Active fire data from NASA FIRMS (https://firms.modaps.eosdis.nasa.gov).
> Atmospheric composition from Copernicus CAMS via Open-Meteo.

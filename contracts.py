"""
contracts.py — Data contracts for every cleaned table.

Validation runs at ingestion so bad rows are dropped loudly and recorded,
instead of silently reaching the gold layer and any model trained on it.

Changes in this revision:
  * Schemas match the renamed, unit-carrying columns. The WAQI schema in
    particular now bounds AQI sub-indices to 0-500, the range the scale
    defines, rather than pretending the values were µg/m³ concentrations.
  * Failure logs are appended under data/contract_failures/ with the run
    timestamp in the filename. The old code wrote
    contract_failures_<source>.csv in the working directory and overwrote it
    every run, so yesterday's violations were gone before anyone looked, and
    *.csv is gitignored so they were invisible in review too.
  * A schema for the CAMS table was missing entirely, which meant that source
    ran with no validation at all.
"""

import os
import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger('contracts')

FAILURE_DIR = os.path.join(
    os.getenv('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')),
    'contract_failures',
)

try:
    import pandera.pandas as pa
    from pandera.pandas import Column, DataFrameSchema, Check
    HAS_PANDERA = True
except ImportError:  # pragma: no cover - exercised only without pandera
    HAS_PANDERA = False
    logger.warning("pandera not installed — data-contract validation is DISABLED. "
                   "Run: pip install pandera")

# India bounding box, shared by every geospatial contract.
LAT_RANGE = (6.0, 37.0)
LON_RANGE = (68.0, 97.0)

# AQI is a 0-500 index by construction on both the US EPA and CPCB scales.
AQI_RANGE = (0.0, 500.0)


def _build_waqi_schema():
    """WAQI serves AQI SUB-INDICES, so every pollutant column is 0-500."""
    columns = {
        "station_id":   Column(str, nullable=False),
        "city":         Column(str, nullable=False),
        "timestamp":    Column(str, nullable=False),
        "source":       Column(str, nullable=False),
        "is_synthetic": Column(int, checks=Check.isin([0, 1]), nullable=False),
    }
    for pollutant in ('pm25', 'pm10', 'no2', 'so2', 'co', 'o3'):
        columns[f"{pollutant}_aqi_raw"] = Column(
            float, checks=Check.in_range(*AQI_RANGE), nullable=True)
    return DataFrameSchema(columns=columns, coerce=True, strict=False, name="cleaned_waqi")


def _build_weather_schema():
    return DataFrameSchema(
        columns={
            "station":           Column(str,   nullable=False),
            "timestamp":         Column(str,   nullable=False),
            "lat":               Column(float, checks=Check.in_range(*LAT_RANGE), nullable=True),
            "lon":               Column(float, checks=Check.in_range(*LON_RANGE), nullable=True),
            "temperature_c_raw": Column(float, checks=Check.in_range(-60, 60),  nullable=True),
            "humidity_pct_raw":  Column(float, checks=Check.in_range(0, 100),   nullable=True),
            "rainfall_mm_raw":   Column(float, checks=Check.in_range(0, 500),   nullable=True),
            "wind_speed_ms_raw": Column(float, checks=Check.in_range(0, 100),   nullable=True),
            "wind_dir_deg_raw":  Column(float, checks=Check.in_range(0, 360),   nullable=True),
            "source":            Column(str,   nullable=False),
            "is_synthetic":      Column(int,   checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True, strict=False, name="cleaned_weather",
    )


def _build_gfs_schema():
    return DataFrameSchema(
        columns={
            "lat":                  Column(float, checks=Check.in_range(*LAT_RANGE), nullable=False),
            "lon":                  Column(float, checks=Check.in_range(*LON_RANGE), nullable=False),
            "cycle":                Column(str,   nullable=False),
            "fhr":                  Column(str,   nullable=False),
            "valid_time":           Column(str,   nullable=False),
            # -80 C is not a plausible surface temperature in India, but the
            # grid includes Himalayan gridpoints at model elevation, so the
            # bound is set by physics rather than by expected weather.
            "temperature_c_raw":    Column(float, checks=Check.in_range(-80, 60),   nullable=True),
            "precipitation_mm_raw": Column(float, checks=Check.in_range(0, 500),    nullable=True),
            "u_wind_ms_raw":        Column(float, checks=Check.in_range(-150, 150), nullable=True),
            "v_wind_ms_raw":        Column(float, checks=Check.in_range(-150, 150), nullable=True),
            "source":               Column(str,   nullable=False),
            "is_synthetic":         Column(int,   checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True, strict=False, name="cleaned_gfs",
    )


def _build_firms_schema():
    return DataFrameSchema(
        columns={
            "lat":              Column(float, checks=Check.in_range(*LAT_RANGE), nullable=False),
            "lon":              Column(float, checks=Check.in_range(*LON_RANGE), nullable=False),
            "timestamp":        Column(str,   nullable=False),
            "sensor":           Column(str,   nullable=False),
            "satellite":        Column(str,   nullable=True),
            # Brightness temperature of a fire pixel, kelvin.
            "brightness_k_raw": Column(float, checks=Check.in_range(200, 600), nullable=True),
            "frp_mw":           Column(float, checks=Check.greater_than_or_equal_to(0),
                                       nullable=True),
            "source":           Column(str,   nullable=False),
            "is_synthetic":     Column(int,   checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True, strict=False, name="cleaned_firms",
    )


def _build_cams_schema():
    """CAMS values are surface mass concentrations in µg/m³."""
    columns = {
        "lat":          Column(float, checks=Check.in_range(*LAT_RANGE), nullable=False),
        "lon":          Column(float, checks=Check.in_range(*LON_RANGE), nullable=False),
        "timestamp":    Column(str,   nullable=False),
        "source":       Column(str,   nullable=False),
        "is_synthetic": Column(int,   checks=Check.isin([0, 1]), nullable=False),
    }
    bounds = {
        'no2_ugm3': (0, 1000), 'so2_ugm3': (0, 2000), 'co_ugm3': (0, 50000),
        'o3_ugm3': (0, 1000), 'pm25_ugm3': (0, 1000), 'pm10_ugm3': (0, 2000),
    }
    for column, (lo, hi) in bounds.items():
        columns[f"{column}_raw"] = Column(
            float, checks=Check.in_range(lo, hi), nullable=True)
    return DataFrameSchema(columns=columns, coerce=True, strict=False, name="cleaned_cams")


_SCHEMAS = {
    'waqi':    _build_waqi_schema,
    'weather': _build_weather_schema,
    'gfs':     _build_gfs_schema,
    'firms':   _build_firms_schema,
    'cams':    _build_cams_schema,
}


def validate(df, source):
    """
    Validates a DataFrame against the schema declared for `source`.

    Returns (valid_df, failures_df). Rows that violate the contract are
    dropped from valid_df and written to a timestamped CSV so the history of
    violations survives.
    """
    if not HAS_PANDERA:
        return df, pd.DataFrame()

    schema_builder = _SCHEMAS.get(source)
    if schema_builder is None:
        logger.warning(f"[contracts] No schema defined for source={source!r}. Skipping validation.")
        return df, pd.DataFrame()

    try:
        validated = schema_builder().validate(df, lazy=True)
        logger.info(f"[contracts] {source}: {len(df)} rows passed all contract checks.")
        return validated, pd.DataFrame()
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases
        bad_indices = [i for i in failures['index'].dropna().unique() if i in df.index]
        valid_df = df.drop(index=bad_indices)

        log_path = _write_failures(failures, source)

        # A failure with no row index is a SCHEMA-level problem — most often a
        # required column missing from the frame entirely. Those dropped no rows,
        # so without this they were logged at the same volume as a single bad
        # reading and every row was written anyway. A source that silently stops
        # returning a pollutant is a much bigger event than one bad value.
        schema_level = failures[failures['index'].isna()]
        if not schema_level.empty:
            names = sorted({str(c) for c in schema_level['failure_case'].dropna().unique()})
            logger.error(
                f"[contracts] SCHEMA VIOLATION — source={source!r}: "
                f"{len(schema_level)} structural failure(s) affecting no specific row "
                f"(usually a missing column): {', '.join(names[:10])}. "
                f"Every row was still written; inspect {log_path}"
            )

        logger.error(
            f"[contracts] CONTRACT VIOLATION — source={source!r}: {len(failures)} check "
            f"failure(s), {len(bad_indices)} row(s) dropped. Details: {log_path}"
        )
        return valid_df, failures


def _write_failures(failures, source):
    """Appends this run's violations to a dated per-source CSV."""
    try:
        os.makedirs(FAILURE_DIR, exist_ok=True)
        now = datetime.now(timezone.utc)
        path = os.path.join(FAILURE_DIR, f"{source}_{now.strftime('%Y-%m-%d')}.csv")
        failures = failures.copy()
        failures.insert(0, 'detected_at', now.isoformat())
        failures.to_csv(path, mode='a', header=not os.path.exists(path), index=False)
        return path
    except Exception as e:
        logger.error(f"[contracts] Could not persist failure cases for {source}: {e}")
        return '<unwritten>'


class ContractViolationError(Exception):
    """Raised when incoming data violates a declared data contract."""

    def __init__(self, source, failures, log_path):
        self.source = source
        self.failures = failures
        super().__init__(
            f"Data contract violation for source={source!r}: "
            f"{len(failures)} check failure(s). Inspect {log_path!r} for details."
        )

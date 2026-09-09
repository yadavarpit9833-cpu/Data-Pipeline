"""
contracts.py  — Data Contracts (Item 11)
-----------------------------------------
Declares pandera schemas for every cleaned table.
Validation runs at ingestion time — bad rows are flagged loudly
instead of silently polluting training data.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger('contracts')

# ── Try to import pandera; fall back to no-op if not installed ──────────────
try:
    import pandera.pandas as pa
    from pandera.pandas import Column, DataFrameSchema, Check
    HAS_PANDERA = True
except ImportError:
    HAS_PANDERA = False
    logger.warning("pandera not installed — data-contract validation is DISABLED. Run: pip install pandera")


# ── Schema definitions ───────────────────────────────────────────────────────

def _build_cpcb_schema():
    return DataFrameSchema(
        columns={
            "station_id": Column(str, nullable=False),
            "city":       Column(str, nullable=False),
            "timestamp":  Column(str, nullable=False),
            "pm25_raw":   Column(float, checks=Check.in_range(0, 1000), nullable=True),
            "pm10_raw":   Column(float, checks=Check.in_range(0, 1500), nullable=True),
            "no2_raw":    Column(float, checks=Check.in_range(0, 1000), nullable=True),
            "so2_raw":    Column(float, checks=Check.in_range(0, 1000), nullable=True),
            "co_raw":     Column(float, checks=Check.in_range(0, 100),  nullable=True),
            "o3_raw":     Column(float, checks=Check.in_range(0, 1000), nullable=True),
            "source":     Column(str, nullable=False),
            "is_synthetic": Column(int, checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True,
        name="cleaned_cpcb",
    )

def _build_weather_schema():
    return DataFrameSchema(
        columns={
            "station":         Column(str, nullable=False),
            "timestamp":       Column(str, nullable=False),
            "temperature_raw": Column(float, checks=Check.in_range(-60, 60),  nullable=True),
            "humidity_raw":    Column(float, checks=Check.in_range(0, 100),   nullable=True),
            "rainfall_raw":    Column(float, checks=Check.in_range(0, 500),   nullable=True),
            "wind_speed_raw":  Column(float, checks=Check.in_range(0, 100),   nullable=True),
            "wind_dir_raw":    Column(float, checks=Check.in_range(0, 360),   nullable=True),
            "source":          Column(str, nullable=False),
            "is_synthetic":    Column(int, checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True,
        name="cleaned_imd",
    )

def _build_gfs_schema():
    return DataFrameSchema(
        columns={
            "lat":              Column(float, checks=Check.in_range(6, 37),    nullable=False),
            "lon":              Column(float, checks=Check.in_range(68, 97),   nullable=False),
            "cycle":            Column(str,   nullable=False),
            "fhr":              Column(str,   nullable=False),
            "temperature_raw":  Column(float, checks=Check.in_range(-60, 60),  nullable=True),
            "precipitation_raw":Column(float, checks=Check.in_range(0, 500),   nullable=True),
            "u_wind_raw":       Column(float, checks=Check.in_range(-150, 150),nullable=True),
            "v_wind_raw":       Column(float, checks=Check.in_range(-150, 150),nullable=True),
            "source":           Column(str,   nullable=False),
            "is_synthetic":     Column(int,   checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True,
        name="cleaned_gfs",
    )

def _build_firms_schema():
    return DataFrameSchema(
        columns={
            "lat":           Column(float, checks=Check.in_range(6, 37),    nullable=False),
            "lon":           Column(float, checks=Check.in_range(68, 97),   nullable=False),
            "timestamp":     Column(str,   nullable=False),
            "brightness_raw":Column(float, checks=Check.in_range(200, 600), nullable=True),
            "satellite":     Column(str,   nullable=True),
            "source":        Column(str,   nullable=False),
            "is_synthetic":  Column(int,   checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True,
        name="cleaned_firms",
    )


_SCHEMAS = {
    'cpcb':    _build_cpcb_schema,
    'weather': _build_weather_schema,
    'gfs':     _build_gfs_schema,
    'firms':   _build_firms_schema,
}


# ── Public API ───────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Validates a DataFrame against the declared schema for the given source.
    On failure: logs all violations, raises ContractViolationError so the
    ingestion pipeline can decide to reject or quarantine the batch.
    Returns the (possibly coerced) DataFrame on success.

    Skips validation gracefully if pandera is not installed.
    """
    if not HAS_PANDERA:
        return df

    schema_builder = _SCHEMAS.get(source)
    if schema_builder is None:
        logger.warning(f"[contracts] No schema defined for source='{source}'. Skipping validation.")
        return df

    schema = schema_builder()
    try:
        validated = schema.validate(df, lazy=True)
        logger.info(f"[contracts] {source}: {len(df)} rows passed all contract checks.")
        return validated
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases
        n_fail = len(failures)
        logger.error(
            f"[contracts] CONTRACT VIOLATION — source='{source}': "
            f"{n_fail} check failure(s) detected.\n{failures.to_string()}"
        )
        raise ContractViolationError(source, failures) from exc


class ContractViolationError(Exception):
    """Raised when incoming data violates a declared data contract."""
    def __init__(self, source: str, failures: pd.DataFrame):
        self.source = source
        self.failures = failures
        super().__init__(
            f"Data contract violation for source='{source}': "
            f"{len(failures)} check failure(s). Inspect .failures for details."
        )

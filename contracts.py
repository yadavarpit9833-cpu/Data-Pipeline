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


def _build_cams_aq_schema():
    """
    CAMS concentrations in the source's own units (ug/m3 throughout, CO
    included). Ranges are generous because this is model output over Indian
    cities during burning season, where PM2.5 genuinely reaches the high
    hundreds.
    """
    return DataFrameSchema(
        columns={
            "city":      Column(str, nullable=False),
            "timestamp": Column(str, nullable=False),
            "pm25_ugm3": Column(float, checks=Check.in_range(0, 2000),  nullable=True),
            "pm10_ugm3": Column(float, checks=Check.in_range(0, 3000),  nullable=True),
            "no2_ugm3":  Column(float, checks=Check.in_range(0, 1000),  nullable=True),
            "so2_ugm3":  Column(float, checks=Check.in_range(0, 1000),  nullable=True),
            "co_ugm3":   Column(float, checks=Check.in_range(0, 50000), nullable=True),
            "o3_ugm3":   Column(float, checks=Check.in_range(0, 1000),  nullable=True),
            "source":    Column(str, nullable=False),
            "is_synthetic": Column(int, checks=Check.isin([0, 1]), nullable=False),
        },
        coerce=True,
        name="cleaned_cams_aq",
    )


_SCHEMAS = {
    'cpcb':    _build_cpcb_schema,
    'weather': _build_weather_schema,
    'gfs':     _build_gfs_schema,
    'firms':   _build_firms_schema,
    'cams_aq': _build_cams_aq_schema,
}


# ── Public API ───────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, source: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates a DataFrame against the declared schema for the given source.
    Returns (valid_df, failures_df). If failures exist, bad rows are dropped
    from valid_df and logged.
    """
    if not HAS_PANDERA:
        return df, pd.DataFrame()

    schema_builder = _SCHEMAS.get(source)
    if schema_builder is None:
        logger.warning(f"[contracts] No schema defined for source='{source}'. Skipping validation.")
        return df, pd.DataFrame()

    schema = schema_builder()
    try:
        validated = schema.validate(df, lazy=True)
        logger.info(f"[contracts] {source}: {len(df)} rows passed all contract checks.")
        return validated, pd.DataFrame()
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases
        n_fail = len(failures)
        
        # Drop bad rows using the index provided by Pandera
        bad_indices = failures['index'].dropna().unique()
        valid_df = df.drop(index=bad_indices)
        
        # Save failures to CSV so we never lose them when terminal closes
        failure_log_path = f"contract_failures_{source}.csv"
        failures.to_csv(failure_log_path, index=False)
        
        logger.error(
            f"[contracts] CONTRACT VIOLATION — source='{source}': "
            f"{n_fail} check failure(s) detected. Dropped {len(bad_indices)} bad rows. Saved to {failure_log_path}"
        )
        return valid_df, failures


class ContractViolationError(Exception):
    """Raised when incoming data violates a declared data contract."""
    def __init__(self, source: str, failures: pd.DataFrame, log_path: str):
        self.source = source
        self.failures = failures
        super().__init__(
            f"Data contract violation for source='{source}': "
            f"{len(failures)} check failure(s). Inspect '{log_path}' for details."
        )

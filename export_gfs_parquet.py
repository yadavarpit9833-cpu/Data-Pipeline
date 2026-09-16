"""
Export cleaned_gfs as a single self-describing Parquet file.

The SQLite table is wide - one column per variable, each in a different unit -
so there is nowhere to put a unit. This writes the long form instead: one row
per (grid point, variable), with `value` and `unit` side by side, so a consumer
never has to guess whether temperature is Kelvin or Celsius. The same unit map
is also attached to the Parquet file metadata under the `units` key.

Two things are held back by default because they are not measurements:

  is_synthetic rows   When NOMADS has not published a cycle yet, fetch_gfs.py
                      falls back to a constant grid (25 degC, 1 m/s, 0 mm) and
                      tags it. It is a placeholder, not weather.
  precipitation       Every APCP value in the table is exactly 0.0, including
                      real NOAA rows. It is not a dry spell - see NOTES below.

Pass --include-synthetic / --include-precipitation to override either.
"""
import os
import json
import argparse
import sqlite3
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'env_data.db')
DEFAULT_OUT = os.path.join(BASE_DIR, 'data', 'exports', 'cleaned_gfs.parquet')

# variable name -> cleaned_gfs column prefix, unit as written to the file, GRIB field.
# `unit` uses the UDUNITS spelling the CF conventions use, so it parses; `suffix`
# is the short form appended to column names in --format wide, where the unit has
# to survive in the name itself. temperature is degC and not the Kelvin GFS ships
# because fetch_gfs.py subtracts 273.15 at parse time - hence _c, not _k.
VARIABLES = {
    'temperature':   {'prefix': 'temperature',   'unit': 'degC',  'suffix': 'c',
                      'grib': 'TMP 2 m above ground (K in GRIB, converted to degC on ingest)'},
    'u_wind':        {'prefix': 'u_wind',        'unit': 'm s-1', 'suffix': 'ms',
                      'grib': 'UGRD 10 m above ground'},
    'v_wind':        {'prefix': 'v_wind',        'unit': 'm s-1', 'suffix': 'ms',
                      'grib': 'VGRD 10 m above ground'},
    'precipitation': {'prefix': 'precipitation', 'unit': 'mm',    'suffix': 'mm',
                      'grib': 'APCP surface'},
}
HELD_BACK = ('precipitation',)

NOTES = {
    'precipitation': (
        "Always 0.0 in this table and NOT a real channel yet. Two independent causes: "
        "(1) fetch_gfs.py requests forecast hour 000, and APCP is an accumulation over an "
        "interval, so NOMADS omits the field entirely at f000 - the fetcher then defaults it "
        "to 0.0, which is indistinguishable from a dry grid point; "
        "(2) GRIB2 writes scale factors in sign-magnitude, and the parser read them as two's "
        "complement, turning APCP's binary scale of -4 into -32764 so that 2**scale underflowed "
        "to 0.0. Cause (2) is fixed as of this export; cause (1) needs the fetcher to request "
        "f003 or later, which changes the table from analysis to forecast and is not done here."
    ),
    'temperature': "Converted from Kelvin to degC in fetch_gfs.py at parse time.",
    'grid': (
        "GFS 0.25 degree global lat/lon, subset to 6.0-37.0 N, 68.0-97.0 E via the NOMADS "
        "subregion filter: 117 lon x 125 lat = 14,625 points per cycle. Scanning mode 0x40 "
        "(west-to-east, south-to-north); latitudes ascend with row index."
    ),
    'qc_flag': (
        "From the shared cleaning step: 'ok', 'flatline' (no variation across the QC window - "
        "expected on the synthetic fallback grid), 'step_fail' (change larger than the "
        "per-variable threshold). 'value' is the cleaned series, 'value_raw' is pre-QC."
    ),
}


def load(conn, include_synthetic, include_precipitation, bbox):
    where, params = ['1=1'], []
    if not include_synthetic:
        where.append('is_synthetic = 0')
    if bbox:
        where.append('lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?')
        params += [bbox[0], bbox[1], bbox[2], bbox[3]]

    wanted = [v for v in VARIABLES if v not in HELD_BACK or include_precipitation]
    cols = ['lat', 'lon', 'cycle', 'fhr', 'valid_time', 'fetched_at', 'source', 'is_synthetic']
    for v in wanted:
        p = VARIABLES[v]['prefix']
        cols += [f'{p}_raw', f'{p}_clean', f'{p}_imputed', f'{p}_qc_flag']

    df = pd.read_sql_query(
        f"SELECT {', '.join(cols)} FROM cleaned_gfs WHERE {' AND '.join(where)}",
        conn, params=params)
    return df, wanted


def to_long(df, wanted):
    frames = []
    for v in wanted:
        p = VARIABLES[v]['prefix']
        frames.append(pd.DataFrame({
            'valid_time': pd.to_datetime(df['valid_time'], utc=True, format='ISO8601'),
            'cycle': df['cycle'].astype(str),
            'fhr': df['fhr'].astype(int).astype('int16'),
            'lat': df['lat'].astype('float64'),
            'lon': df['lon'].astype('float64'),
            'variable': v,
            'value': df[f'{p}_clean'].astype('float64'),
            'value_raw': df[f'{p}_raw'].astype('float64'),
            'unit': VARIABLES[v]['unit'],
            'qc_flag': df[f'{p}_qc_flag'].fillna('ok').astype(str),
            'imputed': df[f'{p}_imputed'].fillna(0).astype(bool),
            'source': df['source'].astype(str),
            'is_synthetic': df['is_synthetic'].astype(bool),
            'fetched_at': pd.to_datetime(df['fetched_at'], utc=True, format='ISO8601'),
        }))
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(['valid_time', 'variable', 'lat', 'lon']).reset_index(drop=True)


def to_wide(df, wanted):
    """
    One row per grid point, with the unit carried in the column name:
    temperature_c, u_wind_ms, v_wind_ms. A consumer reading only the header still
    knows temperature is Celsius rather than the Kelvin the GRIB actually ships.

    valid_time and fetched_at stay ISO-8601 strings with an explicit +00:00 rather
    than Parquet timestamps, so no reader can silently localise them.
    """
    out = pd.DataFrame({
        'valid_time': iso_utc(df['valid_time']),
        'cycle': df['cycle'].astype(str),
        'fhr': df['fhr'].astype(int).astype('int16'),
        'lat': df['lat'].astype('float64'),
        'lon': df['lon'].astype('float64'),
    })
    for v in wanted:
        p, sfx = VARIABLES[v]['prefix'], VARIABLES[v]['suffix']
        out[f'{p}_{sfx}'] = df[f'{p}_clean'].astype('float64')
        out[f'{p}_{sfx}_raw'] = df[f'{p}_raw'].astype('float64')
        out[f'{p}_qc_flag'] = df[f'{p}_qc_flag'].fillna('ok').astype(str)
        out[f'{p}_imputed'] = df[f'{p}_imputed'].fillna(0).astype(bool)
    out['source'] = df['source'].astype(str)
    out['is_synthetic'] = df['is_synthetic'].astype(bool)
    out['fetched_at'] = iso_utc(df['fetched_at'])
    return out.sort_values(['valid_time', 'lat', 'lon']).reset_index(drop=True)


def iso_utc(series):
    """ISO-8601 in UTC, written as text so nothing downstream can reinterpret it."""
    ts = pd.to_datetime(series, utc=True, format='ISO8601')
    return ts.dt.strftime('%Y-%m-%dT%H:%M:%S%z').str.replace(
        r'([+-]\d{2})(\d{2})$', r'\1:\2', regex=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--include-synthetic', action='store_true',
                    help='keep the constant-grid fallback rows (default: dropped)')
    ap.add_argument('--include-precipitation', action='store_true',
                    help='keep the APCP column even though every value is 0.0')
    ap.add_argument('--bbox', nargs=4, type=float,
                    metavar=('LAT_MIN', 'LAT_MAX', 'LON_MIN', 'LON_MAX'),
                    help='clip to a bounding box, e.g. Delhi NCR: 28.2 28.9 76.8 77.6')
    ap.add_argument('--format', choices=('long', 'wide'), default='long',
                    help="long: a row per grid point per variable, with a `unit` column. "
                         "wide: a row per grid point, unit carried in the column name "
                         "(temperature_c, u_wind_ms).")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=180)
    try:
        df, wanted = load(conn, args.include_synthetic, args.include_precipitation, args.bbox)
    finally:
        conn.close()

    if df.empty:
        raise SystemExit('cleaned_gfs returned no rows for those filters - nothing exported.')

    out_df = to_wide(df, wanted) if args.format == 'wide' else to_long(df, wanted)

    if args.format == 'wide':
        units = {f"{VARIABLES[v]['prefix']}_{VARIABLES[v]['suffix']}": VARIABLES[v]['unit']
                 for v in wanted}
    else:
        units = {v: VARIABLES[v]['unit'] for v in wanted}

    table = pa.Table.from_pandas(out_df, preserve_index=False)
    meta = {
        b'units': json.dumps(units).encode(),
        b'format': args.format.encode(),
        b'grib_fields': json.dumps({v: VARIABLES[v]['grib'] for v in wanted}).encode(),
        b'coordinate_units': json.dumps(
            {'lat': 'degrees_north', 'lon': 'degrees_east',
             'valid_time': 'UTC', 'fetched_at': 'UTC'}).encode(),
        b'notes': json.dumps(NOTES).encode(),
        b'source': b'NOAA NCEP GFS 0.25 deg via NOMADS subregion filter',
        b'excluded': json.dumps({
            'synthetic_rows': not args.include_synthetic,
            'precipitation': not args.include_precipitation,
        }).encode(),
        b'exported_at': datetime.now(timezone.utc).isoformat().encode(),
        b'row_source': b'cleaned_gfs (SQLite), one row per grid point per variable',
    }
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), **meta})

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + '.tmp'
    pq.write_table(table, tmp, compression='zstd')
    os.replace(tmp, args.out)

    print(f"wrote {args.out}  ({os.path.getsize(args.out):,} bytes, {len(out_df):,} rows)")
    print(f"  format          : {args.format}")
    print(f"  grid rows in    : {len(df):,}  ({df.groupby(['lat', 'lon']).ngroups} grid points)")
    print(f"  units           : {json.dumps(units)}")
    print(f"  cycles          : {', '.join(sorted(df['cycle'].unique()))}")
    print(f"  valid_time span : {out_df['valid_time'].min()} .. {out_df['valid_time'].max()}")
    held = [v for v in HELD_BACK if v not in wanted]
    if held:
        print(f"  held back       : {', '.join(held)} (--include-precipitation to force)")
    if not args.include_synthetic:
        print('  synthetic rows  : dropped (--include-synthetic to keep)')


if __name__ == '__main__':
    main()

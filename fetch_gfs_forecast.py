"""
Pull one full GFS forecast run - f000 to f072 at 3-hourly steps - for a bounding
box, and write it as a single Parquet with units in the column names.

This is not what scheduler.py does. `fetch_gfs.py` requests forecast hour 000 only,
so `cleaned_gfs` holds the analysis hour of each cycle and nothing else; there is no
forecast in the database to export. This script fetches one.

Three things the naive read of these files gets wrong, all of them silent:

  TMP appears twice per file. Surface skin temperature (type 1) and 2 m air
  temperature (type 103, level 2). Keying on the GRIB parameter alone and letting
  the last record win picks whichever NCEP happened to write second. Records are
  selected here by parameter AND level.

  APCP appears twice per file, with different accumulation windows. One record is
  the bucket since the last 6-hour boundary (0-3, 0-6, 6-9, 6-12, ...) and the other
  is the run total (0-N). Last-record-wins takes the run total, so the series looks
  like rain that only ever increases. Verified across the whole 0-72 h run.

  APCP is never a 3-hour bucket at 6-hourly steps. At f006, f012, f018 the short
  record spans 6 hours, not 3. Differencing against f003, f009, f015 turns the run
  into a uniform 3-hourly series, which is what a per-step precipitation channel has
  to be.

Usage:
    python fetch_gfs_forecast.py --bbox 28.2 28.9 76.8 77.6 --out exports/gfs_ncr_forecast.parquet
"""
import os
import json
import time
import struct
import logging
import argparse
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from fetch_gfs import grib_signed
from cleaning import clean_and_impute

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fetch_gfs_forecast')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NOMADS = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl'
MAX_FHR, STEP = 72, 3

# Selected by parameter AND level, so a same-parameter record at another level
# cannot take its place. (category, number, surface type, level)
FIELDS = {
    'temperature_c': {'key': (0, 0, 103, 2.0),  'grib': 'TMP 2 m above ground',
                      'unit': 'degC', 'kelvin': True},
    'u_wind_ms':     {'key': (2, 2, 103, 10.0), 'grib': 'UGRD 10 m above ground',
                      'unit': 'm s-1', 'kelvin': False},
    'v_wind_ms':     {'key': (2, 3, 103, 10.0), 'grib': 'VGRD 10 m above ground',
                      'unit': 'm s-1', 'kelvin': False},
}
APCP_KEY = (1, 8, 1, 0.0)

QC = {
    'temperature_c':      dict(min_val=-60.0, max_val=60.0, max_step_change=25.0,
                               ignore_zero_flatline=False),
    'u_wind_ms':          dict(min_val=-150.0, max_val=150.0, max_step_change=50.0,
                               ignore_zero_flatline=False),
    'v_wind_ms':          dict(min_val=-150.0, max_val=150.0, max_step_change=50.0,
                               ignore_zero_flatline=False),
    'precipitation_mm_3h': dict(min_val=0.0, max_val=500.0, max_step_change=200.0,
                                ignore_zero_flatline=True),
}


# ---------------------------------------------------------------- GRIB2 reading

def iter_records(content):
    """Yield one dict per GRIB2 message, with enough of sections 3-5 to identify it."""
    pos = 0
    while pos < len(content) - 4:
        if content[pos:pos + 4] != b'GRIB':
            pos += 1
            continue
        length = int.from_bytes(content[pos + 8:pos + 16], 'big')
        if length <= 0 or pos + length > len(content):
            pos += 4
            continue
        msg = content[pos:pos + length]
        pos += length

        sp, secs = 16, {}
        while sp < len(msg) - 4:
            if msg[sp:sp + 4] == b'7777':
                break
            sl = int.from_bytes(msg[sp:sp + 4], 'big')
            if sl <= 0:
                break
            secs[msg[sp + 4]] = msg[sp:sp + sl]
            sp += sl
        if not {3, 4, 5, 7} <= secs.keys():
            continue

        s3, s4, s5, s7 = secs[3], secs[4], secs[5], secs[7]
        template = int.from_bytes(s4[7:9], 'big')
        scale = s4[23]
        level = int.from_bytes(s4[24:28], 'big') / (10 ** scale) if scale < 200 else 0.0

        rec = {
            'key': (s4[9], s4[10], s4[22], float(level)),
            'template': template,
            'forecast_time': int.from_bytes(s4[18:22], 'big'),
            # Only product template 4.8 carries a statistical time range.
            'span_h': int.from_bytes(s4[49:53], 'big') if template == 8 else None,
            'ni': int.from_bytes(s3[30:34], 'big'),
            'nj': int.from_bytes(s3[34:38], 'big'),
            'lat1': int.from_bytes(s3[46:50], 'big', signed=True) / 1e6,
            'lat2': int.from_bytes(s3[55:59], 'big', signed=True) / 1e6,
            'lon1': int.from_bytes(s3[50:54], 'big', signed=True) / 1e6,
            'lon2': int.from_bytes(s3[59:63], 'big', signed=True) / 1e6,
            'dlat': int.from_bytes(s3[63:67], 'big') / 1e6,
            'dlon': int.from_bytes(s3[67:71], 'big') / 1e6,
            'scan_mode': s3[71],
            '_s5': s5, '_s7': s7,
        }
        yield rec


def decode(rec):
    """Unpack a simple-packed (template 5.0) data section."""
    s5, s7 = rec['_s5'], rec['_s7']
    drt = int.from_bytes(s5[9:11], 'big')
    if drt != 0:
        raise ValueError(f"data representation template 5.{drt} is not simple packing")
    ref = struct.unpack('>f', s5[11:15])[0]
    # Sign-magnitude, not two's complement - see grib_signed() in fetch_gfs.py.
    bin_scale, dec_scale, nbits = grib_signed(s5[15:17]), grib_signed(s5[17:19]), s5[19]
    npoints = rec['ni'] * rec['nj']

    if nbits == 0:                      # constant field
        return [ref * 10.0 ** -dec_scale] * npoints
    raw = s7[5:]
    if len(raw) * 8 < nbits * npoints:
        raise ValueError('truncated data section')
    bits = ''.join(f'{b:08b}' for b in raw)
    return [(ref + int(bits[i:i + nbits], 2) * 2.0 ** bin_scale) * 10.0 ** -dec_scale
            for i in range(0, nbits * npoints, nbits)]


def grid_points(rec):
    """(lat, lon) per data index. Scan mode 0x40 is west-to-east, south-to-north."""
    if rec['scan_mode'] not in (0x40,):
        raise ValueError(f"unhandled scanning mode 0x{rec['scan_mode']:02x}")
    lat0, lon0 = min(rec['lat1'], rec['lat2']), min(rec['lon1'], rec['lon2'])
    lats = [round(lat0 + j * rec['dlat'], 3) for j in range(rec['nj'])]
    lons = [round(lon0 + i * rec['dlon'], 3) for i in range(rec['ni'])]
    return [(la, lo) for la in lats for lo in lons]


# ---------------------------------------------------------------- NOMADS access

def url_for(cycle_date, cycle_hour, fhr, bbox):
    lat_min, lat_max, lon_min, lon_max = bbox
    return (f"{NOMADS}?file=gfs.t{cycle_hour}z.pgrb2.0p25.f{fhr:03d}"
            "&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on"
            "&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on"
            f"&subregion=&leftlon={lon_min}&rightlon={lon_max}"
            f"&toplat={lat_max}&bottomlat={lat_min}"
            f"&dir=%2Fgfs.{cycle_date}%2F{cycle_hour}%2Fatmos")


def get(cycle_date, cycle_hour, fhr, bbox, retries=3):
    for attempt in range(retries):
        r = requests.get(url_for(cycle_date, cycle_hour, fhr, bbox), timeout=90)
        if r.status_code == 200 and r.content.startswith(b'GRIB'):
            return r.content
        # NOMADS answers 200 with an HTML error page for an unpublished cycle.
        if attempt == retries - 1:
            head = r.content[:160].decode('utf-8', 'replace').strip()
            raise ValueError(f"f{fhr:03d}: HTTP {r.status_code}, not GRIB2: {head!r}")
        time.sleep(3 * (attempt + 1))


def newest_complete_cycle(bbox):
    """Walk back through cycles until one has f072 on disk at NCEP."""
    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0, hour=now.hour // 6 * 6)
    for back in range(0, 5):
        c = start - timedelta(hours=6 * back)
        date_str, hour_str = c.strftime('%Y%m%d'), c.strftime('%H')
        try:
            get(date_str, hour_str, MAX_FHR, bbox, retries=1)
            logger.info(f"using cycle {date_str} {hour_str}z "
                        f"({(now - c).total_seconds() / 3600:.1f} h old)")
            return date_str, hour_str
        except ValueError:
            logger.info(f"cycle {date_str} {hour_str}z not complete through f{MAX_FHR}, going back")
    raise SystemExit('no cycle found with a complete 72-hour run')


# ---------------------------------------------------------------- assembly

def pick_apcp(records):
    """
    Of the two APCP records per file, take the one with the SHORTER accumulation
    window: that is the bucket since the last 6-hour boundary. The other is the run
    total from f000, which grows monotonically and is not a per-step quantity.
    """
    apcp = [r for r in records if r['key'] == APCP_KEY and r['span_h']]
    return min(apcp, key=lambda r: r['span_h']) if apcp else None


def fetch_run(cycle_date, cycle_hour, bbox):
    cycle = f'{cycle_date}_{cycle_hour}z'
    cycle_dt = datetime.strptime(cycle_date + cycle_hour, '%Y%m%d%H').replace(tzinfo=timezone.utc)
    fetched_at = datetime.now(timezone.utc)

    rows, buckets = [], {}
    for fhr in range(0, MAX_FHR + 1, STEP):
        content = get(cycle_date, cycle_hour, fhr, bbox)
        records = list(iter_records(content))
        by_key = {}
        for r in records:
            if r['key'] in FIELDS_BY_KEY and r['key'] not in by_key:
                by_key[r['key']] = r

        missing = [n for n, f in FIELDS.items() if f['key'] not in by_key]
        if missing:
            raise ValueError(f"f{fhr:03d}: missing {', '.join(missing)}")

        first = by_key[FIELDS['temperature_c']['key']]
        points = grid_points(first)

        values = {}
        for name, f in FIELDS.items():
            v = decode(by_key[f['key']])
            values[name] = [round(x - 273.15, 3) if f['kelvin'] else round(x, 3) for x in v]

        apcp = pick_apcp(records)
        if apcp is None:
            if fhr != 0:
                raise ValueError(f"f{fhr:03d}: no APCP record found")
            bucket, span = None, None          # f000 genuinely has no APCP
        else:
            bucket, span = decode(apcp), apcp['span_h']
        buckets[fhr] = {'values': bucket, 'span': span}

        # The record we deliberately do NOT use is the run total from f000. At the
        # last step it is the independent check that the differencing was right.
        if fhr == MAX_FHR:
            totals = [r for r in records
                      if r['key'] == APCP_KEY and r['span_h'] == MAX_FHR
                      and r['forecast_time'] == 0]
            buckets['_run_total'] = decode(totals[0]) if totals else None

        valid = cycle_dt + timedelta(hours=fhr)
        for i, (lat, lon) in enumerate(points):
            rows.append({
                'valid_time': valid, 'cycle': cycle, 'fhr': fhr, 'lat': lat, 'lon': lon,
                'temperature_c': values['temperature_c'][i],
                'u_wind_ms': values['u_wind_ms'][i],
                'v_wind_ms': values['v_wind_ms'][i],
                '_apcp_bucket': None if bucket is None else round(bucket[i], 4),
                '_apcp_span_h': span,
                'fetched_at': fetched_at,
            })
        logger.info(f"f{fhr:03d}  {len(points)} points  "
                    f"APCP window {'none' if span is None else f'{fhr - span}-{fhr}h'}")
        time.sleep(0.4)
    return rows, buckets


FIELDS_BY_KEY = {f['key']: n for n, f in FIELDS.items()}


def to_3h_increments(df, buckets):
    """
    Make precipitation a uniform 3-hour quantity.

    At f003, f009, f015 ... the bucket already spans 3 hours. At f006, f012, f018 ...
    it spans 6, sharing its start with the 3-hour bucket before it, so the 3-hour
    increment is the difference. f000 has no accumulation at all and is left null
    rather than zero - there is no interval to have rained in.
    """
    run_total = buckets.get('_run_total')
    out, notes = [], []
    for fhr in sorted(k for k in buckets if isinstance(k, int)):
        b = buckets[fhr]
        if b['values'] is None:
            out.append((fhr, None, None))
            notes.append(f'f{fhr:03d}: no APCP (analysis hour)')
            continue
        span = b['span']
        if span == STEP:
            vals, window = b['values'], (fhr - STEP, fhr)
        else:
            prev = buckets.get(fhr - STEP)
            if prev is None or prev['values'] is None or prev['span'] != span - STEP:
                raise ValueError(f"f{fhr:03d}: cannot difference a {span}h bucket")
            vals = [a - c for a, c in zip(b['values'], prev['values'])]
            window = (fhr - STEP, fhr)
            notes.append(f'f{fhr:03d}: {span}h bucket minus f{fhr - STEP:03d} '
                         f'{span - STEP}h bucket')
        out.append((fhr, [round(v, 4) for v in vals], window))

    lookup = {fhr: vals for fhr, vals, _ in out}
    per_fhr_index = df.groupby('fhr').cumcount()
    df = df.copy()
    df['precipitation_mm_3h'] = [
        None if lookup[f] is None else lookup[f][i]
        for f, i in zip(df['fhr'], per_fhr_index)
    ]

    neg = df['precipitation_mm_3h'].dropna()
    neg = neg[neg < -1e-6]
    if len(neg):
        raise ValueError(f"{len(neg)} negative 3-hour increments, min {neg.min()} - "
                         "the bucket differencing is wrong, refusing to write")
    df['precipitation_mm_3h'] = df['precipitation_mm_3h'].clip(lower=0)

    # Independent check: the 3-hour increments must add back up to the run-total
    # record NCEP ships alongside them. If differencing were wrong this would not
    # close, and a plausible-looking rainfall series would go out anyway.
    if run_total is not None:
        summed = df.groupby(['lat', 'lon'], sort=True)['precipitation_mm_3h'].sum().tolist()
        diffs = [a - b for a, b in zip(summed, run_total)]
        worst = max(abs(d) for d in diffs)
        for (la, lo), s, t, d in zip(sorted(df.groupby(['lat', 'lon']).groups),
                                     summed, run_total, diffs):
            logger.info(f"  reconcile {la},{lo}: increments {s:.4f} vs run total {t:.4f} "
                        f"(diff {d:+.4f}, {d / 0.0625:+.2f} quanta)")
        # APCP is packed at 1/16 mm, and 24 differenced buckets carry that rounding
        # forward, so exact closure is not available. A couple of quanta is rounding;
        # anything larger means the windows were combined wrongly.
        tolerance = max(0.25, 0.02 * max(run_total))
        if worst > tolerance:
            raise ValueError(f"3-hour increments do not sum to the f{MAX_FHR:03d} run total "
                             f"(worst grid point off by {worst:.4f} mm, tolerance "
                             f"{tolerance:.4f}) - refusing to write")
        notes.append(f'checked: increments sum to the 0-{MAX_FHR}h run total '
                     f'within {worst:.4f} mm at every grid point')
        logger.info(f"increments reconcile with the 0-{MAX_FHR}h run total "
                    f"(worst point off by {worst:.4f} mm)")
    return df, notes


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--bbox', nargs=4, type=float, required=True,
                    metavar=('LAT_MIN', 'LAT_MAX', 'LON_MIN', 'LON_MAX'))
    ap.add_argument('--out', required=True)
    ap.add_argument('--cycle', help='YYYYMMDDHH; default is the newest complete run')
    args = ap.parse_args()

    bbox = tuple(args.bbox)
    if args.cycle:
        cycle_date, cycle_hour = args.cycle[:8], args.cycle[8:10]
    else:
        cycle_date, cycle_hour = newest_complete_cycle(bbox)

    rows, buckets = fetch_run(cycle_date, cycle_hour, bbox)
    df = pd.DataFrame(rows).sort_values(['fhr', 'lat', 'lon']).reset_index(drop=True)
    df, notes = to_3h_increments(df, buckets)
    df = df.drop(columns=['_apcp_bucket', '_apcp_span_h'])

    # Same QC chain the scheduled pipeline uses. Unlike f000-only runs, this is a
    # genuine time series per grid point, so step and flatline checks mean something.
    for metric, opts in QC.items():
        df = clean_and_impute(df, metric, 'valid_time', lat_col='lat', lon_col='lon', **opts)
        df[metric] = df[f'{metric}_clean']
        df[f'{metric}_qc_flag'] = df[f'{metric}_qc_flag'].fillna('ok').astype(str)
        df[f'{metric}_imputed'] = df[f'{metric}_imputed'].fillna(0).astype(bool)
        df = df.drop(columns=[f'{metric}_clean'])

    # The imputer fills gaps, which is right for a dropout and wrong here: f000 has
    # no accumulation window, so its precipitation is undefined rather than missing.
    # Left alone it interpolates rain into the analysis hour. Put the null back.
    at_analysis = df['fhr'] == 0
    df.loc[at_analysis, 'precipitation_mm_3h'] = pd.NA
    df.loc[at_analysis, 'precipitation_mm_3h_qc_flag'] = 'no_accumulation_window'
    df.loc[at_analysis, 'precipitation_mm_3h_imputed'] = False

    df['valid_time'] = df['valid_time'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    df['fetched_at'] = df['fetched_at'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    df['fhr'] = df['fhr'].astype('int16')
    df['source'] = 'noaa'
    df['is_synthetic'] = False

    ordered = ['valid_time', 'cycle', 'fhr', 'lat', 'lon']
    for m in ('temperature_c', 'u_wind_ms', 'v_wind_ms', 'precipitation_mm_3h'):
        ordered += [m, f'{m}_qc_flag', f'{m}_imputed']
    df = df[ordered + ['source', 'is_synthetic', 'fetched_at']]

    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata({
        **(table.schema.metadata or {}),
        b'units': json.dumps({'temperature_c': 'degC', 'u_wind_ms': 'm s-1',
                              'v_wind_ms': 'm s-1', 'precipitation_mm_3h': 'mm'}).encode(),
        b'coordinate_units': json.dumps({'lat': 'degrees_north', 'lon': 'degrees_east',
                                         'valid_time': 'UTC', 'fetched_at': 'UTC'}).encode(),
        b'grib_fields': json.dumps({**{n: f['grib'] for n, f in FIELDS.items()},
                                    'precipitation_mm_3h': 'APCP surface'}).encode(),
        b'cycle': f'{cycle_date}_{cycle_hour}z'.encode(),
        b'forecast_hours': f'0 to {MAX_FHR} step {STEP}'.encode(),
        b'precipitation_definition': (
            'Accumulation over the 3 hours ENDING at valid_time, in mm. GFS APCP resets '
            'every 6 hours, so files at 6-hourly steps carry a 6-hour bucket; those are '
            'differenced against the preceding 3-hour bucket. f000 is null, not zero - '
            'the analysis hour has no interval to accumulate over.').encode(),
        b'derivation': json.dumps(notes).encode(),
        b'source': b'NOAA NCEP GFS 0.25 deg via NOMADS subregion filter',
        b'bbox': json.dumps({'lat_min': bbox[0], 'lat_max': bbox[1],
                             'lon_min': bbox[2], 'lon_max': bbox[3]}).encode(),
        b'note': (b'Fetched by fetch_gfs_forecast.py, not by the scheduled pipeline. '
                  b'These rows are NOT in cleaned_gfs, which holds f000 only.'),
        b'exported_at': datetime.now(timezone.utc).isoformat().encode(),
    })

    out = args.out if os.path.isabs(args.out) else os.path.join(BASE_DIR, args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    tmp = out + '.tmp'
    pq.write_table(table, tmp, compression='zstd')
    os.replace(tmp, out)

    rain = df['precipitation_mm_3h'].dropna()
    print(f"\nwrote {out}  ({os.path.getsize(out):,} bytes, {len(df):,} rows)")
    print(f"  cycle           : {cycle_date}_{cycle_hour}z")
    print(f"  grid points     : {df.groupby(['lat', 'lon']).ngroups}")
    print(f"  forecast hours  : {df['fhr'].min()} to {df['fhr'].max()} step {STEP} "
          f"({df['fhr'].nunique()} steps)")
    print(f"  valid_time span : {df['valid_time'].min()} .. {df['valid_time'].max()}")
    print(f"  precipitation   : {rain.min():.3f} to {rain.max():.3f} mm/3h, "
          f"{(rain > 0).sum()}/{len(rain)} wet cells, total {rain.sum():.1f} mm")
    flags = {c: df[c].value_counts().to_dict() for c in df.columns if c.endswith('_qc_flag')}
    print(f"  qc flags        : {flags}")


if __name__ == '__main__':
    main()

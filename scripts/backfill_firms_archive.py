"""
backfill_firms_archive.py — Historical NASA FIRMS active fire detections.

WHY THIS IS A SEPARATE SCRIPT FROM fetch_firms.py
-------------------------------------------------
fetch_firms.py asks for the last 1 day from the NRT sources. It cannot reach
2020: the FIRMS area API caps DAY_RANGE at 10 days, and the near-real-time
sources only carry a recent window. Historical dates need two things the live
fetcher does not do — an explicit start date, and the SP (Standard Processing)
sources, which are the reprocessed archive.

    https://firms.modaps.eosdis.nasa.gov/api/area/csv/
        {MAP_KEY}/{SOURCE}/{west,south,east,north}/{DAY_RANGE}/{START_DATE}

    DAY_RANGE  : 1..10          START_DATE : YYYY-MM-DD, returns
    MAP_KEY    : 5000 requests               START_DATE .. START_DATE+DAY_RANGE-1
                 per 10 minutes

NRT vs SP matters for correctness, not just coverage. SP is the reprocessed
version of the same detections, so the same fire appears in both streams. The
pipeline stores the instrument FAMILY in `sensor` and the stream in
`processing`, and the uniqueness constraint is on the family — so this script
UPSERTS: an SP row supersedes the NRT row for the same detection instead of
duplicating it. Without that split, a backfill overlapping the live fetcher's
window would quietly double the fire counts any model is trained on.

USAGE
-----
    # What would be fetched, and how many API calls that costs
    python scripts/backfill_firms_archive.py --dry-run

    # The default window: January, October, November, December of 2020-2025
    python scripts/backfill_firms_archive.py

    # Narrower
    python scripts/backfill_firms_archive.py --years 2023,2024 --months 10,11

The run is RESUMABLE. Every completed chunk is recorded in
firms_backfill_manifest, and a re-run skips what is already stored, so an
interrupted backfill continues where it stopped rather than starting over.
"""

import os
import sys
import time
import logging
import argparse
import hashlib
from io import StringIO
from calendar import monthrange
from datetime import date, datetime, timezone, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_db_connection, execute_many, execute_query, init_db  # noqa: E402
from cleaning import clean_and_impute                                    # noqa: E402
from storage import save_raw_data, save_cleaned_data_parquet            # noqa: E402
from contracts import validate                                           # noqa: E402
from fetch_firms import (                                                # noqa: E402
    standardise_frame, format_firms_timestamp, redact_key, split_source,
    INDIA_AREA,
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('firms_backfill')

load_dotenv()
FIRMS_MAP_KEY = os.getenv('FIRMS_MAP_KEY')

API_BASE = 'https://firms.modaps.eosdis.nasa.gov/api'
MAX_DAY_RANGE = 10          # hard API limit
DEFAULT_MONTHS = [1, 10, 11, 12]
DEFAULT_YEARS = list(range(2020, 2026))

# Archive (Standard Processing) sources, with the NRT stream to fall back to
# when SP has not been produced for a date yet — SP lags NRT by months.
DEFAULT_SOURCES = ['MODIS_SP', 'VIIRS_SNPP_SP', 'VIIRS_NOAA20_SP']
NRT_FALLBACK = {
    'MODIS_SP': 'MODIS_NRT',
    'VIIRS_SNPP_SP': 'VIIRS_SNPP_NRT',
    'VIIRS_NOAA20_SP': 'VIIRS_NOAA20_NRT',
}

# Instrument service dates. Asking for data before these returns nothing and
# burns a request, so those chunks are skipped up front.
SENSOR_FIRST_LIGHT = {
    'MODIS': date(2000, 11, 1),        # Terra; Aqua from 2002
    'VIIRS_SNPP': date(2012, 1, 20),
    'VIIRS_NOAA20': date(2018, 1, 1),
    'VIIRS_NOAA21': date(2024, 1, 1),
}

REQUEST_TIMEOUT_S = 120
MAX_RETRIES = 4
DEFAULT_SLEEP_S = 1.0


# ── Manifest ────────────────────────────────────────────────────────────────

MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS firms_backfill_manifest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    start_date TEXT NOT NULL,
    day_range INTEGER NOT NULL,
    status TEXT NOT NULL,
    rows_fetched INTEGER DEFAULT 0,
    payload_sha256 TEXT,
    lake_path TEXT,
    error_message TEXT,
    fetched_at TEXT NOT NULL,
    CONSTRAINT unq_firms_backfill UNIQUE (source, start_date, day_range)
)
"""


def ensure_manifest(conn):
    conn.cursor().execute(MANIFEST_DDL.strip())
    conn.commit()


def completed_chunks(conn, include_empty=True):
    """
    (source, start_date) pairs that a resumed run should not fetch again.

    'empty' counts as complete by default. A window with no detections is a
    real answer, and re-requesting it on every run costs two API calls each
    (the SP attempt plus the NRT fallback) to learn the same thing. Pass
    --retry-empty when SP may since have been produced for a recent window.
    """
    statuses = ("'success', 'empty'") if include_empty else ("'success'")
    cur = conn.cursor()
    try:
        cur.execute("SELECT source, start_date FROM firms_backfill_manifest "
                    f"WHERE status IN ({statuses})")
        return {(r[0], r[1]) for r in cur.fetchall()}
    except Exception:
        return set()


def record_chunk(conn, source, start, day_range, status, rows, sha, lake_path, error):
    execute_query(conn.cursor(), """
        INSERT INTO firms_backfill_manifest
            (source, start_date, day_range, status, rows_fetched,
             payload_sha256, lake_path, error_message, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, start_date, day_range) DO UPDATE SET
            status        = excluded.status,
            rows_fetched  = excluded.rows_fetched,
            payload_sha256= excluded.payload_sha256,
            lake_path     = excluded.lake_path,
            error_message = excluded.error_message,
            fetched_at    = excluded.fetched_at
    """, (source, start, day_range, status, rows, sha, lake_path, error,
          datetime.now(timezone.utc).isoformat()))
    conn.commit()


# ── Planning ────────────────────────────────────────────────────────────────

def month_chunks(year, month, max_days=MAX_DAY_RANGE):
    """Splits one calendar month into (start_date, day_range) pairs of <= 10 days."""
    days_in_month = monthrange(year, month)[1]
    chunks, day = [], 1
    while day <= days_in_month:
        span = min(max_days, days_in_month - day + 1)
        chunks.append((date(year, month, day).isoformat(), span))
        day += span
    return chunks


def build_plan(years, months, sources, today=None):
    """
    Returns the list of (source, start_date, day_range) to fetch.

    Chunks before an instrument existed, or in the future, are dropped rather
    than requested — each would cost an API call and return nothing.
    """
    today = today or datetime.now(timezone.utc).date()
    plan, skipped = [], []
    for source in sources:
        family, _ = split_source(source)
        first_light = SENSOR_FIRST_LIGHT.get(family, date(2000, 1, 1))
        for year in years:
            for month in months:
                for start, span in month_chunks(year, month):
                    start_d = date.fromisoformat(start)
                    if start_d + timedelta(days=span - 1) < first_light:
                        skipped.append((source, start, f'before {family} first light'))
                        continue
                    if start_d > today:
                        skipped.append((source, start, 'in the future'))
                        continue
                    plan.append((source, start, span))
    return plan, skipped


# ── Fetching ────────────────────────────────────────────────────────────────

def build_url(source, start, day_range):
    return (f"{API_BASE}/area/csv/{FIRMS_MAP_KEY}/{source}/"
            f"{INDIA_AREA}/{day_range}/{start}")


def looks_like_csv(text):
    return bool(text) and 'latitude' in text.lower()


def fetch_chunk(source, start, day_range):
    """
    Fetches one chunk. Returns (csv_text, source_actually_used).

    Raises on a transport failure. A valid-but-empty response (no detections in
    that window) is returned as-is — that is data, not an error.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(build_url(source, start, day_range),
                                    timeout=REQUEST_TIMEOUT_S)
            if response.status_code == 429:
                wait = min(60, 5 * (2 ** attempt))
                logger.warning(f"  rate limited; waiting {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            last_err = e
            logger.warning(f"  attempt {attempt + 1}/{MAX_RETRIES} failed: {redact_key(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise last_err


def parse_chunk(csv_text, source, start):
    """Parses a chunk's CSV into the pipeline's column vocabulary."""
    frame = pd.read_csv(StringIO(csv_text))
    if frame.empty:
        return frame

    frame = standardise_frame(frame, source)
    if frame is None:
        return pd.DataFrame()

    fallback = f"{start}T00:00:00+00:00"
    if 'acq_date' in frame.columns and 'acq_time' in frame.columns:
        frame['timestamp'] = [
            format_firms_timestamp(d, t, fallback)
            for d, t in zip(frame['acq_date'], frame['acq_time'])
        ]
    else:
        frame['timestamp'] = fallback
    return frame


# ── Storage ─────────────────────────────────────────────────────────────────

CLEANED_UPSERT = """
INSERT INTO cleaned_firms (
    lat, lon, timestamp, sensor, processing, satellite,
    brightness_k_raw, brightness_k_clean, brightness_k_imputed, brightness_k_qc_flag,
    frp_mw, confidence_raw, confidence_scale, confidence_class, daynight,
    source, is_synthetic
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (lat, lon, timestamp, satellite, sensor) DO UPDATE SET
    processing        = excluded.processing,
    brightness_k_raw  = excluded.brightness_k_raw,
    brightness_k_clean= excluded.brightness_k_clean,
    brightness_k_qc_flag = excluded.brightness_k_qc_flag,
    frp_mw            = excluded.frp_mw,
    confidence_raw    = excluded.confidence_raw,
    confidence_scale  = excluded.confidence_scale,
    confidence_class  = excluded.confidence_class,
    daynight          = excluded.daynight
WHERE excluded.processing = 'SP'
"""
# The WHERE clause is the point: only Standard Processing may overwrite an
# existing row. SP is the reprocessed, authoritative version of a detection, so
# it supersedes NRT — but a later NRT fetch must never downgrade an SP row.


def store_chunk(conn, frame, source, start):
    """QC, validate and upsert one chunk. Returns the number of rows stored."""
    if frame.empty:
        return 0

    # Range check only. Consecutive rows here are SEPARATE FIRES, not readings
    # from one instrument, so step and flatline checks are meaningless — the
    # same reasoning that applies in fetch_firms.py.
    frame = clean_and_impute(
        frame, 'brightness_k_raw', time_col='timestamp', group_cols=['sensor'],
        min_val=200.0, max_val=600.0, max_step_change=None, window_flatline=None,
    )
    frame = frame.rename(columns={
        'brightness_k_raw_clean': 'brightness_k_clean',
        'brightness_k_raw_imputed': 'brightness_k_imputed',
        'brightness_k_raw_qc_flag': 'brightness_k_qc_flag',
    })
    frame['source'] = 'firms_archive'
    frame['is_synthetic'] = 0

    frame, failures = validate(frame, 'firms')
    if frame.empty:
        logger.warning(f"  {source} {start}: all rows failed contract validation")
        return 0
    if not failures.empty:
        logger.warning(f"  {source} {start}: "
                       f"{len(failures['index'].dropna().unique())} rows dropped by contract")

    rows = [
        (float(r.lat), float(r.lon), r.timestamp, r.sensor, r.processing,
         str(r.satellite),
         _f(r.brightness_k_raw), _f(r.brightness_k_clean),
         int(bool(r.brightness_k_imputed)), r.brightness_k_qc_flag,
         _f(r.frp_mw), _s(r.confidence_raw), _s(r.confidence_scale),
         _s(r.confidence_class), _s(r.daynight), 'firms_archive', 0)
        for r in frame.itertuples(index=False)
    ]
    execute_many(conn.cursor(), CLEANED_UPSERT, rows)
    conn.commit()

    # Parquet is partitioned by observation date, so one chunk spanning ten
    # days writes into up to ten partitions.
    frame['_date'] = frame['timestamp'].str.slice(0, 10)
    for day, group in frame.groupby('_date'):
        save_cleaned_data_parquet(
            group.drop(columns=['_date']), source='firms',
            partition_key='date', partition_value=day,
            dedup_keys=['lat', 'lon', 'timestamp', 'satellite', 'sensor'],
            pure_overwrite=False,
        )
    return len(rows)


def _f(v):
    return None if pd.isna(v) else float(v)


def _s(v):
    return None if v is None or pd.isna(v) else str(v)


# ── Main ────────────────────────────────────────────────────────────────────

def parse_int_list(text, name):
    values = []
    for part in str(text).split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part and not part.startswith('-'):
            lo, hi = part.split('-', 1)
            values.extend(range(int(lo), int(hi) + 1))
        else:
            values.append(int(part))
    if not values:
        raise ValueError(f"--{name} produced no values")
    return sorted(set(values))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--years', default='2020-2025',
                        help="e.g. '2020-2025' or '2023,2024' (default: 2020-2025)")
    parser.add_argument('--months', default='1,10,11,12',
                        help='1-12, comma separated (default: 1,10,11,12 — '
                             'the stubble-burning and winter season)')
    parser.add_argument('--sources', default=','.join(DEFAULT_SOURCES),
                        help='FIRMS API sources (default: the three SP archives)')
    parser.add_argument('--sleep', type=float, default=DEFAULT_SLEEP_S,
                        help='seconds between requests (default: 1.0)')
    parser.add_argument('--dry-run', action='store_true',
                        help='show the plan and the API cost, fetch nothing')
    parser.add_argument('--no-resume', action='store_true',
                        help='re-fetch every chunk, including completed ones')
    parser.add_argument('--retry-empty', action='store_true',
                        help='also re-check windows previously recorded as empty '
                             '(useful when SP may have been produced since)')
    args = parser.parse_args(argv)

    years = parse_int_list(args.years, 'years')
    months = parse_int_list(args.months, 'months')
    if any(m < 1 or m > 12 for m in months):
        parser.error('--months must be between 1 and 12')
    sources = [s.strip() for s in args.sources.split(',') if s.strip()]

    plan, skipped = build_plan(years, months, sources)

    print()
    print('=== FIRMS archive backfill ===')
    print(f"  years   : {years[0]}-{years[-1]}  ({len(years)} years)")
    print(f"  months  : {', '.join(str(m) for m in months)}")
    print(f"  sources : {', '.join(sources)}")
    print(f"  area    : {INDIA_AREA} (west,south,east,north)")
    print()
    print(f"  API requests planned : {len(plan)}")
    print(f"  MAP_KEY budget       : 5000 per 10 minutes "
          f"({len(plan) / 5000:.1%} of one window)")
    est = len(plan) * (args.sleep + 2.0) / 60
    print(f"  rough runtime        : {est:.0f} min at {args.sleep}s between requests")
    if skipped:
        print(f"  chunks skipped       : {len(skipped)} (instrument not in service, "
              f"or future dates)")

    if args.dry_run:
        print()
        print('DRY RUN — nothing fetched.')
        for source, start, span in plan[:5]:
            print(f"    would GET {source} {start} +{span}d")
        if len(plan) > 5:
            print(f"    ... and {len(plan) - 5} more")
        return 0

    if not FIRMS_MAP_KEY or FIRMS_MAP_KEY == 'your_firms_map_key_here':
        print()
        print('ERROR: FIRMS_MAP_KEY is missing or still the placeholder in .env.')
        print('Get a free key: https://firms.modaps.eosdis.nasa.gov/api/map_key/')
        return 1

    init_db()
    conn = get_db_connection()
    ensure_manifest(conn)

    done = set() if args.no_resume else completed_chunks(
        conn, include_empty=not args.retry_empty)
    if done:
        print(f"  already fetched      : {len(done)} chunk(s) — resuming")

    print()
    stored = attempted = failed = empty = 0
    try:
        for index, (source, start, span) in enumerate(plan, 1):
            if (source, start) in done:
                continue

            attempted += 1
            label = f"[{index}/{len(plan)}] {source} {start} +{span}d"
            used_source = source
            try:
                csv_text = fetch_chunk(source, start, span)

                # SP lags NRT by months. When the archive has not been produced
                # for a window yet, fall back to the NRT stream rather than
                # leaving a hole — `processing` records which one was used.
                if not looks_like_csv(csv_text) and source in NRT_FALLBACK:
                    used_source = NRT_FALLBACK[source]
                    logger.info(f"{label}: no SP data, trying {used_source}")
                    csv_text = fetch_chunk(used_source, start, span)
                    time.sleep(args.sleep)

                if not looks_like_csv(csv_text):
                    empty += 1
                    record_chunk(conn, source, start, span, 'empty', 0, None, None,
                                 redact_key(csv_text.strip()[:200]))
                    print(f"{label}: no data")
                    continue

                sha = hashlib.sha256(csv_text.encode('utf-8')).hexdigest()
                lake_path = save_raw_data('firms_archive', start, csv_text, ext='csv')

                frame = parse_chunk(csv_text, used_source, start)
                count = store_chunk(conn, frame, used_source, start)
                stored += count

                record_chunk(conn, source, start, span, 'success', count, sha,
                             lake_path, None if used_source == source
                             else f'fell back to {used_source}')
                note = '' if used_source == source else f' (via {used_source})'
                print(f"{label}: {count} detections{note}")

            except Exception as e:
                failed += 1
                message = redact_key(str(e))[:400]
                record_chunk(conn, source, start, span, 'failure', 0, None, None, message)
                logger.error(f"{label}: {message}")

            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print()
        print('Interrupted. Progress is recorded — re-run to resume.')
    finally:
        conn.close()

    print()
    print('=== Summary ===')
    print(f"  chunks attempted : {attempted}")
    print(f"  detections stored: {stored}")
    print(f"  empty windows    : {empty}")
    print(f"  failed chunks    : {failed}")
    if failed:
        print()
        print('  Re-run to retry the failures; successful chunks are skipped.')
    print()
    print('  Next: python gold_layer.py   # rebuild the gold tables over the new data')
    return 1 if failed and not stored else 0


if __name__ == '__main__':
    sys.exit(main())

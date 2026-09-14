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

    DAY_RANGE  : 1..5           START_DATE : YYYY-MM-DD, returns
    MAP_KEY    : 5000 requests               START_DATE .. START_DATE+DAY_RANGE-1
                 per 10 minutes

NASA's API page documents DAY_RANGE as 1..10. The server rejects anything above
5 with "Invalid day range. Expects [1..5]." The server wins.

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
    standardise_frame, format_firms_timestamp, redact_key, INDIA_AREA,
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('firms_backfill')

load_dotenv()
FIRMS_MAP_KEY = os.getenv('FIRMS_MAP_KEY')

API_BASE = 'https://firms.modaps.eosdis.nasa.gov/api'
# The API enforces 1..5, NOT the 1..10 that NASA's own API page documents.
# Asking for 10 returns: HTTP 400 "Invalid day range. Expects [1..5]."
# Confirmed against the live endpoint on 2026-09-13.
MAX_DAY_RANGE = 5
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

# Fallback coverage, used only when the data_availability endpoint cannot be
# reached. The live endpoint is authoritative and is queried first — these
# dates were read from it on 2026-09-13 and will drift as NASA reprocesses.
FALLBACK_AVAILABILITY = {
    'MODIS_SP':        (date(2000, 11, 1), date(2026, 5, 31)),
    'VIIRS_SNPP_SP':   (date(2012, 1, 20), date(2026, 4, 27)),
    'VIIRS_NOAA20_SP': (date(2018, 4, 1), date(2026, 5, 31)),
    'VIIRS_NOAA21_NRT': (date(2024, 1, 17), None),
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


def fetch_availability(map_key=None):
    """
    Asks FIRMS which sources exist and what dates each one covers.

    Returns {source: (min_date, max_date)}. This replaces guessing at instrument
    service dates: the endpoint reports, for example, that MODIS_NRT only
    reaches back to 2026-06-01 while MODIS_SP starts at 2000-11-01, which is
    exactly what decides whether a historical chunk is worth requesting.

    Falls back to FALLBACK_AVAILABILITY if the endpoint cannot be reached, so a
    network hiccup degrades the plan rather than stopping it.
    """
    key = map_key or FIRMS_MAP_KEY
    if not key:
        return dict(FALLBACK_AVAILABILITY)
    try:
        response = requests.get(f"{API_BASE}/data_availability/csv/{key}/ALL",
                                timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
        out = {}
        for row in frame.itertuples(index=False):
            try:
                lo = date.fromisoformat(str(row.min_date))
                hi = date.fromisoformat(str(row.max_date))
            except (ValueError, AttributeError):
                continue
            out[str(row.data_id)] = (lo, hi)
        return out or dict(FALLBACK_AVAILABILITY)
    except Exception as e:
        logger.warning(f"Could not read data availability ({redact_key(e)}); "
                       f"using the built-in fallback dates.")
        return dict(FALLBACK_AVAILABILITY)


# ── Gap filling ─────────────────────────────────────────────────────────────

# Each instrument family, and the two streams it can be served from. SP is the
# reprocessed archive and is preferred; NRT is the only option near the present,
# because SP lags by three to four months.
SOURCE_FAMILIES = {
    'MODIS':        ('MODIS_SP', 'MODIS_NRT'),
    'VIIRS_SNPP':   ('VIIRS_SNPP_SP', 'VIIRS_SNPP_NRT'),
    'VIIRS_NOAA20': ('VIIRS_NOAA20_SP', 'VIIRS_NOAA20_NRT'),
}


def covers(availability, source, day):
    """True when `source` publishes data for `day`."""
    lo, hi = availability.get(source, (None, None))
    if lo and day < lo:
        return False
    if hi and day > hi:
        return False
    return source in availability


def select_source_for_date(family, day, availability):
    """
    Picks which stream can serve one date for one instrument family.

    SP first: it is the reprocessed, authoritative version, and the upsert lets
    it supersede an NRT row for the same detection. NRT only when SP does not
    reach that date yet. None when neither covers it, so no request is spent.
    """
    sp_source, nrt_source = SOURCE_FAMILIES.get(family, (None, None))
    if sp_source and covers(availability, sp_source, day):
        return sp_source
    if nrt_source and covers(availability, nrt_source, day):
        return nrt_source
    return None


def dates_with_data(conn, family):
    """Dates that already hold at least one detection for this family."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT substr(timestamp, 1, 10) FROM cleaned_firms "
                    "WHERE sensor = ?", (family,))
        out = set()
        for (value,) in cur.fetchall():
            try:
                out.add(date.fromisoformat(str(value)))
            except (ValueError, TypeError):
                continue
        return out
    except Exception:
        return set()


def dates_known_empty(conn, family):
    """
    Dates a previous run already established have no detections.

    Without this every genuinely fireless day would look like a gap forever and
    be re-requested on every run. The manifest records the chunk, so its whole
    window is expanded back into individual dates.
    """
    sp_source, nrt_source = SOURCE_FAMILIES.get(family, (None, None))
    cur = conn.cursor()
    try:
        cur.execute("SELECT start_date, day_range FROM firms_backfill_manifest "
                    "WHERE status = 'empty' AND source IN (?, ?)",
                    (sp_source, nrt_source))
        out = set()
        for start, span in cur.fetchall():
            try:
                first = date.fromisoformat(str(start))
            except (ValueError, TypeError):
                continue
            out.update(first + timedelta(days=i) for i in range(int(span or 1)))
        return out
    except Exception:
        return set()


def build_gap_plan(conn, since, until, families=None, availability=None, max_days=None):
    """
    Returns (plan, gap_summary) for the dates that have no data yet.

    A gap is a date in the window with no stored detections for that family and
    no record of a previous run finding it empty. Consecutive gap dates served
    by the same source are merged into chunks so a two-week outage costs three
    requests rather than fourteen.
    """
    availability = FALLBACK_AVAILABILITY if availability is None else availability
    families = families or list(SOURCE_FAMILIES)
    max_days = max_days or MAX_DAY_RANGE

    plan, summary = [], {}
    for family in families:
        have = dates_with_data(conn, family) | dates_known_empty(conn, family)

        # (date, source) for every missing day we can actually request
        missing = []
        day = since
        while day <= until:
            if day not in have:
                source = select_source_for_date(family, day, availability)
                if source:
                    missing.append((day, source))
            day += timedelta(days=1)

        summary[family] = len(missing)

        # Merge runs of consecutive days that share a source.
        run_start = run_source = None
        run_len = 0
        for day, source in missing:
            contiguous = (run_start is not None
                          and source == run_source
                          and day == run_start + timedelta(days=run_len)
                          and run_len < max_days)
            if contiguous:
                run_len += 1
                continue
            if run_start is not None:
                plan.append((run_source, run_start.isoformat(), run_len))
            run_start, run_source, run_len = day, source, 1
        if run_start is not None:
            plan.append((run_source, run_start.isoformat(), run_len))

    return plan, summary


def build_plan(years, months, sources, today=None, availability=None):
    """
    Returns the list of (source, start_date, day_range) to fetch.

    Chunks outside a source's published coverage are dropped rather than
    requested: each one would cost an API call and return an empty CSV.
    """
    today = today or datetime.now(timezone.utc).date()
    availability = FALLBACK_AVAILABILITY if availability is None else availability

    plan, skipped = [], []
    for source in sources:
        lo, hi = availability.get(source, (date(2000, 1, 1), None))
        for year in years:
            for month in months:
                for start, span in month_chunks(year, month):
                    start_d = date.fromisoformat(start)
                    end_d = start_d + timedelta(days=span - 1)
                    if lo and end_d < lo:
                        skipped.append((source, start, f'{source} coverage starts {lo}'))
                        continue
                    if hi and start_d > hi:
                        skipped.append((source, start, f'{source} coverage ends {hi}'))
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

            # FIRMS explains a rejected request in the RESPONSE BODY as plain
            # text ("Invalid MAP_KEY", "Invalid source", a date range message).
            # raise_for_status() discards that and leaves only "400 Client
            # Error: Bad Request", which says nothing about the cause. A 4xx is
            # also not worth retrying four times — the request will be just as
            # malformed the fourth time.
            if response.status_code >= 400:
                detail = redact_key((response.text or '').strip()[:300])
                message = (f"HTTP {response.status_code} for {source} {start} "
                           f"+{day_range}d — FIRMS said: {detail or '(empty body)'}")
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    raise ValueError(message)      # no retry: our request is wrong
                raise requests.exceptions.HTTPError(message)

            return response.text
        except ValueError as e:
            raise                                   # client error, stop immediately
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
    """
    QC, validate and upsert one chunk.

    Returns (rows_stored, parquet_ok). The second value matters: the database
    write and the Parquet write can succeed independently, and the gold layer
    reads Parquet. A chunk whose rows reached SQLite but not Parquet is NOT
    done, and recording it as success would hide fire data from every
    downstream table while the run reported itself healthy.
    """
    if frame.empty:
        return 0, True

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
        return 0, True
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
    parquet_ok = True
    for day, group in frame.groupby('_date'):
        written = save_cleaned_data_parquet(
            group.drop(columns=['_date']), source='firms',
            partition_key='date', partition_value=day,
            dedup_keys=['lat', 'lon', 'timestamp', 'satellite', 'sensor'],
            pure_overwrite=False,
        )
        if written is None:
            parquet_ok = False
    return len(rows), parquet_ok


def _f(v):
    return None if pd.isna(v) else float(v)


def _s(v):
    return None if v is None or pd.isna(v) else str(v)


# ── Main ────────────────────────────────────────────────────────────────────

def rebuild_parquet_from_db(conn):
    """
    Regenerates the Parquet partitions from cleaned_firms, making no API calls.

    Needed because the database write and the Parquet write are independent:
    a run can store every detection in SQLite and still fail to write Parquet,
    which is exactly what happened while two sensors' `confidence` columns had
    clashing types. The rows are already paid for; re-downloading them to fix a
    serialisation bug would waste both quota and an hour.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT lat, lon, timestamp, sensor, processing, satellite,
               brightness_k_raw, brightness_k_clean, brightness_k_imputed,
               brightness_k_qc_flag, frp_mw, confidence_raw, confidence_scale,
               confidence_class, daynight, source, is_synthetic
        FROM cleaned_firms
    """)
    columns = [d[0] for d in cur.description]
    frame = pd.DataFrame(cur.fetchall(), columns=columns)
    if frame.empty:
        print('  cleaned_firms is empty — nothing to rebuild.')
        return 0

    frame['_date'] = frame['timestamp'].astype(str).str.slice(0, 10)
    written = failed = 0
    for day, group in frame.groupby('_date'):
        path = save_cleaned_data_parquet(
            group.drop(columns=['_date']), source='firms',
            partition_key='date', partition_value=day,
            dedup_keys=['lat', 'lon', 'timestamp', 'satellite', 'sensor'],
            pure_overwrite=True,       # the database is the source of truth here
        )
        if path:
            written += 1
        else:
            failed += 1
            print(f'  FAILED: date={day}')

    print(f"  rebuilt {written} partition(s) from {len(frame)} rows"
          + (f", {failed} failed" if failed else ''))
    return failed


def execute_plan(conn, plan, args, availability, resume=True):
    """
    Fetches and stores every chunk in `plan`. Shared by the year/month backfill
    and by --fill-gaps so both get the same retry, resume, upsert and
    Parquet-failure handling.

    Returns a process exit code.
    """
    done = set()
    if resume and not args.no_resume:
        done = completed_chunks(conn, include_empty=not args.retry_empty)
        if done:
            print(f"  already fetched      : {len(done)} chunk(s) — resuming")

    print()
    stored = attempted = failed = empty = partial = 0
    try:
        for index, (source, start, span) in enumerate(plan, 1):
            if (source, start) in done:
                continue

            attempted += 1
            label = f"[{index}/{len(plan)}] {source} {start} +{span}d"
            used_source = source
            try:
                csv_text = fetch_chunk(source, start, span)

                # SP lags NRT by months, so an empty SP window near the present
                # may still exist in NRT. For older dates it cannot: NRT carries
                # only a few recent months (FIRMS reports MODIS_NRT starting
                # 2026-06-01), so falling back there would spend a request to
                # receive a header row. Only try it when NRT actually covers
                # the window.
                if not looks_like_csv(csv_text) and source in NRT_FALLBACK:
                    fallback = NRT_FALLBACK[source]
                    nrt_lo, nrt_hi = (availability or {}).get(fallback, (None, None))
                    chunk_end = date.fromisoformat(start) + timedelta(days=span - 1)
                    covered = (nrt_lo is None) or (
                        chunk_end >= nrt_lo and (nrt_hi is None or
                                                 date.fromisoformat(start) <= nrt_hi))
                    if covered:
                        used_source = fallback
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
                count, parquet_ok = store_chunk(conn, frame, used_source, start)
                stored += count

                # 'partial' is deliberately not in the resume skip set, so the
                # next run retries the chunk and repairs its Parquet partition.
                status = 'success' if parquet_ok else 'partial'
                if not parquet_ok:
                    partial += 1
                note = None if used_source == source else f'fell back to {used_source}'
                if not parquet_ok:
                    note = f"{note + '; ' if note else ''}parquet write failed"
                record_chunk(conn, source, start, span, status, count, sha,
                             lake_path, note)

                suffix = '' if used_source == source else f' (via {used_source})'
                if not parquet_ok:
                    suffix += '  [PARQUET FAILED - will retry next run]'
                print(f"{label}: {count} detections{suffix}")

            except Exception as e:
                failed += 1
                message = redact_key(str(e))[:400]
                record_chunk(conn, source, start, span, 'failure', 0, None, None, message)
                logger.error(f"{label}: {message}")

            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print()
        print('Interrupted. Progress is recorded — re-run to resume.')
    
    print()
    print('=== Summary ===')
    print(f"  chunks attempted : {attempted}")
    print(f"  detections stored: {stored}")
    print(f"  empty windows    : {empty}")
    print(f"  partial chunks   : {partial}  (database written, Parquet failed)")
    print(f"  failed chunks    : {failed}")
    if failed or partial:
        print()
        print('  Re-run to retry the failed and partial chunks; completed ones are skipped.')
    print()
    print('  Next: python gold_layer.py   # rebuild the gold tables over the new data')
    return 1 if failed and not stored else 0


def run_gap_fill(args):
    """Finds the dates with no data and fetches only those."""
    today = datetime.now(timezone.utc).date()
    since = (date.fromisoformat(args.since) if args.since
             else today - timedelta(days=90))
    # Yesterday by default: today is still being observed, so treating it as a
    # gap would re-request a partial day on every run.
    until = date.fromisoformat(args.until) if args.until else today - timedelta(days=1)
    if since > until:
        print(f"--since {since} is after --until {until}; nothing to check.")
        return 1

    init_db()
    conn = get_db_connection()
    ensure_manifest(conn)
    try:
        availability = (None if args.no_availability_check else fetch_availability())
        plan, summary = build_gap_plan(conn, since, until, availability=availability)

        print()
        print('=== FIRMS gap fill ===')
        print(f"  window  : {since} .. {until}  ({(until - since).days + 1} days)")
        print()
        for family, missing in summary.items():
            total = (until - since).days + 1
            print(f"    {family:<14} {missing:>4} of {total} day(s) missing")
        print()
        print(f"  API requests planned : {len(plan)}")

        if not plan:
            print()
            print('  No gaps. Every day in the window is either stored or known empty.')
            return 0

        # Show which stream each gap will come from: SP for older dates, NRT
        # near the present, since SP lags by months.
        streams = {}
        for source, _, _ in plan:
            streams[source] = streams.get(source, 0) + 1
        print('  by source:')
        for source, count in sorted(streams.items()):
            print(f"    {source:<20} {count}")

        if args.dry_run:
            print()
            print('DRY RUN — nothing fetched.')
            for source, start, span in plan[:8]:
                print(f"    would GET {source} {start} +{span}d")
            if len(plan) > 8:
                print(f"    ... and {len(plan) - 8} more")
            return 0

        if not FIRMS_MAP_KEY or FIRMS_MAP_KEY == 'your_firms_map_key_here':
            print('\nERROR: FIRMS_MAP_KEY is missing or still the placeholder in .env.')
            return 1

        print()
        return execute_plan(conn, plan, args, availability, resume=False)
    finally:
        conn.close()


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
    parser.add_argument('--fill-gaps', action='store_true',
                        help='fetch only the dates that have no data yet, instead '
                             'of a fixed year/month window. Use this after the '
                             'scheduler has been down.')
    parser.add_argument('--since', default=None,
                        help='with --fill-gaps: earliest date to check '
                             '(YYYY-MM-DD, default: 90 days ago)')
    parser.add_argument('--until', default=None,
                        help='with --fill-gaps: latest date to check '
                             '(YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--rebuild-parquet', action='store_true',
                        help='regenerate the Parquet lake from cleaned_firms and '
                             'exit. Makes no API calls — use this when the rows '
                             'reached the database but Parquet writes failed.')
    parser.add_argument('--no-resume', action='store_true',
                        help='re-fetch every chunk, including completed ones')
    parser.add_argument('--no-availability-check', action='store_true',
                        help='skip the data_availability lookup and use the '
                             'built-in fallback coverage dates')
    parser.add_argument('--retry-empty', action='store_true',
                        help='also re-check windows previously recorded as empty '
                             '(useful when SP may have been produced since)')
    args = parser.parse_args(argv)

    if args.rebuild_parquet:
        init_db()
        conn = get_db_connection()
        print()
        print('=== Rebuilding the FIRMS Parquet lake from the database ===')
        print('    (no API calls)')
        try:
            return 1 if rebuild_parquet_from_db(conn) else 0
        finally:
            conn.close()

    if args.fill_gaps:
        return run_gap_fill(args)

    years = parse_int_list(args.years, 'years')
    months = parse_int_list(args.months, 'months')
    if any(m < 1 or m > 12 for m in months):
        parser.error('--months must be between 1 and 12')
    sources = [s.strip() for s in args.sources.split(',') if s.strip()]

    # Ask FIRMS what it actually has before deciding what to request.
    availability = fetch_availability() if not args.no_availability_check else None
    plan, skipped = build_plan(years, months, sources, availability=availability)

    print()
    print('=== FIRMS archive backfill ===')
    print(f"  years   : {years[0]}-{years[-1]}  ({len(years)} years)")
    print(f"  months  : {', '.join(str(m) for m in months)}")
    print(f"  sources : {', '.join(sources)}")
    print(f"  area    : {INDIA_AREA} (west,south,east,north)")
    if availability:
        print()
        print('  Coverage reported by FIRMS:')
        for source in sources:
            lo, hi = availability.get(source, (None, None))
            mark = '' if source in availability else '   (not listed by FIRMS!)'
            print(f"    {source:<18} {lo} .. {hi}{mark}")
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
    print()
    try:
        return execute_plan(conn, plan, args, availability)
    finally:
        conn.close()



if __name__ == '__main__':
    sys.exit(main())

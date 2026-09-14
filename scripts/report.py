"""
report.py — One command that prints the whole state of the pipeline.

Run this instead of piecing the picture together from query_db, directory
listings and ad-hoc DuckDB queries. It writes the same text to
pipeline_report.txt so it can be attached or pasted in one go.

    python scripts/report.py

It reads only. Nothing is fetched, written or migrated.
"""

import os
import sys
import glob
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_db_connection, DB_ENGINE, SQLITE_DB_PATH   # noqa: E402
from storage import DATA_DIR                                   # noqa: E402

OUT_PATH = 'pipeline_report.txt'
_lines = []


def say(text=''):
    print(text)
    _lines.append(text)


def rule(title):
    say()
    say('=' * 78)
    say(f' {title}')
    say('=' * 78)


def table_names(cur):
    if DB_ENGINE == 'postgres':
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' "
                    "ORDER BY tablename")
    else:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name")
    return [r[0] for r in cur.fetchall()]


def scalar(cur, sql, default=None):
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else default
    except Exception:
        return default


# ── Database ────────────────────────────────────────────────────────────────

def report_database():
    rule('DATABASE')
    try:
        conn = get_db_connection()
    except Exception as e:
        say(f'  Could not open the database: {e}')
        return None

    cur = conn.cursor()
    say(f'  engine : {DB_ENGINE}')
    if DB_ENGINE != 'postgres':
        size = os.path.getsize(SQLITE_DB_PATH) / 1024 / 1024 if os.path.exists(SQLITE_DB_PATH) else 0
        say(f'  file   : {SQLITE_DB_PATH}  ({size:.1f} MB)')
    say()
    say(f"  {'table':<28} {'rows':>10}   {'earliest':<12} {'latest':<12}")
    say(f"  {'-' * 28} {'-' * 10}   {'-' * 12} {'-' * 12}")

    # The column each table is dated by, so a range can be shown alongside counts.
    date_column = {
        'cleaned_firms': 'timestamp', 'cleaned_waqi': 'timestamp',
        'cleaned_weather': 'timestamp', 'cleaned_cams': 'timestamp',
        'cleaned_gfs': 'valid_time', 'raw_firms': 'timestamp',
        'raw_waqi': 'timestamp', 'raw_weather': 'timestamp',
        'raw_cams': 'timestamp', 'raw_gfs': 'valid_time',
        'pipeline_run_log': 'run_started_at',
        'firms_backfill_manifest': 'start_date',
    }

    for table in table_names(cur):
        count = scalar(cur, f'SELECT COUNT(*) FROM "{table}"', 0) or 0
        column = date_column.get(table)
        lo = hi = ''
        if column and count:
            lo = str(scalar(cur, f'SELECT MIN(substr({column},1,10)) FROM "{table}"') or '')
            hi = str(scalar(cur, f'SELECT MAX(substr({column},1,10)) FROM "{table}"') or '')
        say(f'  {table:<28} {count:>10}   {lo:<12} {hi:<12}')
    return conn


def report_firms(conn):
    rule('FIRE DETECTIONS (cleaned_firms)')
    cur = conn.cursor()
    if not scalar(cur, 'SELECT COUNT(*) FROM cleaned_firms', 0):
        say('  Empty — the backfill has not stored anything yet.')
        return

    say('  By sensor and processing stream:')
    say(f"    {'sensor':<16} {'stream':<8} {'rows':>10}   {'from':<12} {'to':<12}")
    cur.execute("""
        SELECT sensor, processing, COUNT(*),
               MIN(substr(timestamp,1,10)), MAX(substr(timestamp,1,10))
        FROM cleaned_firms GROUP BY sensor, processing ORDER BY sensor, processing
    """)
    for sensor, stream, n, lo, hi in cur.fetchall():
        say(f'    {str(sensor):<16} {str(stream):<8} {n:>10}   {str(lo):<12} {str(hi):<12}')

    say()
    say('  Detections per year and month (the seasonal signal):')
    cur.execute("""
        SELECT substr(timestamp,1,4) AS y, substr(timestamp,6,2) AS m, COUNT(*)
        FROM cleaned_firms GROUP BY y, m ORDER BY y, m
    """)
    rows = cur.fetchall()
    years = sorted({r[0] for r in rows})
    months = sorted({r[1] for r in rows})
    lookup = {(r[0], r[1]): r[2] for r in rows}
    say('    year  ' + ''.join(f'{m:>10}' for m in months))
    for year in years:
        say(f'    {year}  ' + ''.join(f'{lookup.get((year, m), 0):>10}' for m in months))

    say()
    say('  Quality control flags:')
    cur.execute("SELECT brightness_k_qc_flag, COUNT(*) FROM cleaned_firms "
                "GROUP BY brightness_k_qc_flag ORDER BY COUNT(*) DESC")
    for flag, n in cur.fetchall():
        say(f'    {str(flag):<28} {n:>10}')

    imputed = scalar(cur, 'SELECT COUNT(*) FROM cleaned_firms WHERE brightness_k_imputed = 1', 0)
    say(f'    {"imputed brightness":<28} {imputed:>10}   '
        f'{"<- must be 0 for fire data" if imputed else "(correct)"}')


def report_manifest(conn):
    rule('BACKFILL MANIFEST')
    cur = conn.cursor()
    try:
        cur.execute('SELECT status, COUNT(*), SUM(rows_fetched) '
                    'FROM firms_backfill_manifest GROUP BY status ORDER BY status')
        rows = cur.fetchall()
    except Exception:
        say('  No manifest table — the backfill has not been run.')
        return
    if not rows:
        say('  Empty.')
        return

    say(f"    {'status':<12} {'chunks':>8} {'detections':>12}")
    for status, chunks, detections in rows:
        say(f'    {str(status):<12} {chunks:>8} {int(detections or 0):>12}')

    cur.execute("SELECT source, start_date, error_message FROM firms_backfill_manifest "
                "WHERE status IN ('failure','partial') ORDER BY start_date LIMIT 10")
    bad = cur.fetchall()
    if bad:
        say()
        say('  Chunks needing another run (re-run the backfill to repair):')
        for source, start, message in bad:
            say(f'    {source} {start}: {str(message)[:80]}')


def report_runs(conn):
    rule('PIPELINE RUN LOG')
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT source, status, COUNT(*), MAX(substr(run_started_at,1,16))
            FROM pipeline_run_log GROUP BY source, status ORDER BY source, status
        """)
        rows = cur.fetchall()
    except Exception:
        rows = []
    if not rows:
        say('  No runs recorded yet.')
        return
    say(f"    {'source':<12} {'status':<10} {'runs':>6}   last")
    for source, status, n, last in rows:
        say(f'    {str(source):<12} {str(status):<10} {n:>6}   {last}')


# ── Parquet lake ────────────────────────────────────────────────────────────

def report_lake():
    rule('PARQUET LAKE')
    if not os.path.isdir(DATA_DIR):
        say(f'  {DATA_DIR} does not exist.')
        return
    say(f'  root: {DATA_DIR}')
    say()
    say(f"    {'dataset':<34} {'files':>6} {'MB':>8}")
    for directory in sorted(glob.glob(os.path.join(DATA_DIR, 'cleaned_*'))
                            + glob.glob(os.path.join(DATA_DIR, 'gold', '*'))):
        files = glob.glob(os.path.join(directory, '*.parquet'))
        if not files:
            continue
        size = sum(os.path.getsize(f) for f in files) / 1024 / 1024
        label = os.path.relpath(directory, DATA_DIR)
        say(f'    {label:<34} {len(files):>6} {size:>8.1f}')


def read_gold(table):
    files = sorted(glob.glob(os.path.join(DATA_DIR, 'gold', table, '*.parquet')))
    if not files:
        return pd.DataFrame()
    frames = []
    for path in files:
        try:
            frame = pd.read_parquet(path)
            # The partition key lives in the filename, not the file.
            key, _, value = os.path.basename(path).partition('=')
            frame[key] = value.replace('.parquet', '')
            frames.append(frame)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def report_gold():
    rule('GOLD LAYER')

    fires = read_gold('fire_activity_daily')
    if fires.empty:
        say('  fire_activity_daily: EMPTY')
        say('    If the backfill stored rows, the gold layer was probably run')
        say('    without --all, so only the last few days were processed:')
        say('        python gold_layer.py --all')
    else:
        say(f'  fire_activity_daily: {len(fires)} rows, '
            f'{fires["date"].nunique()} dates '
            f'({fires["date"].min()} .. {fires["date"].max()})')
        say()
        say('    Ten most intense fire days by total radiative power:')
        top = (fires.groupby('date', as_index=False)
               .agg(detections=('n_detections', 'sum'),
                    frp_total_mw=('frp_total_mw', 'sum'))
               .sort_values('frp_total_mw', ascending=False).head(10))
        say(f"      {'date':<12} {'detections':>11} {'FRP (MW)':>12}")
        for row in top.itertuples(index=False):
            say(f'      {row.date:<12} {int(row.detections):>11} {row.frp_total_mw:>12,.0f}')

    say()
    exposure = read_gold('city_fire_exposure_daily')
    if exposure.empty:
        say('  city_fire_exposure_daily: EMPTY')
    else:
        say(f'  city_fire_exposure_daily: {len(exposure)} city-day rows, '
            f'{exposure["date"].nunique()} dates')
        say()
        say('    Mean fire radiative power within 300 km, by city and month.')
        say('    This is the feature that should track winter particulate load.')
        exposure['month'] = exposure['date'].str.slice(0, 7)
        pivot = (exposure.groupby(['city', 'month'])['frp_within_300km']
                 .mean().unstack(fill_value=0))
        recent = pivot[sorted(pivot.columns)[-8:]] if len(pivot.columns) > 8 else pivot
        say('      ' + f"{'city':<12}" + ''.join(f'{c:>11}' for c in recent.columns))
        for city, row in recent.iterrows():
            say(f'      {city:<12}' + ''.join(f'{v:>11,.0f}' for v in row.values))

    for table in ('city_aqi_hourly', 'city_daily_summary', 'gfs_grid_hourly'):
        frame = read_gold(table)
        say()
        say(f'  {table}: {len(frame)} rows'
            + (f", {frame.iloc[:, -1].nunique()} partition(s)" if not frame.empty else ' (EMPTY)'))


def main():
    say('=' * 78)
    say(f' PIPELINE REPORT — {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}')
    say('=' * 78)

    conn = report_database()
    if conn is not None:
        try:
            report_firms(conn)
            report_manifest(conn)
            report_runs(conn)
        finally:
            conn.close()

    report_lake()
    report_gold()

    rule('END')
    try:
        with open(OUT_PATH, 'w', encoding='utf-8') as fh:
            fh.write('\n'.join(_lines) + '\n')
        print(f'\nAlso written to {OUT_PATH} — attach or paste that file.')
    except Exception as e:
        print(f'\nCould not write {OUT_PATH}: {e}')
    return 0


if __name__ == '__main__':
    sys.exit(main())

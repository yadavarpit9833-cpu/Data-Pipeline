"""
migrate_to_v2_schema.py — one-off migration from the pre-v2 table names.

Run this ONCE against an existing env_data.db created before the renaming.
It is safe to run twice: every step checks whether it already happened.

What it does
------------
1. Backs the database up first.

2. Renames the mislabelled tables and their columns:

     raw_cpcb        -> raw_waqi         (WAQI, never CPCB)
     cleaned_cpcb    -> cleaned_waqi     pm25_raw -> pm25_aqi_raw, etc.
     raw_imd         -> raw_weather      (Open-Meteo, never IMD)
     cleaned_imd     -> cleaned_weather  temperature_raw -> temperature_c_raw
     raw_sentinel5p  -> raw_cams         (CAMS, never Sentinel-5P)
     cleaned_sentinel5p -> cleaned_cams  no2_ppb -> no2_ugm3 (it was never ppb)

3. Quarantines the synthetic GFS rows into gfs_synthetic_quarantine and
   deletes them from raw_gfs / cleaned_gfs.

   This matters. The old fetcher wrote a hardcoded constant grid
   (25.0 C, 1.0 m/s, 0.0 mm) whenever NOAA was unreachable, and because the
   cycle label was always "<today>_00z" with fhr "000", INSERT OR IGNORE on
   UNIQUE(lat, lon, cycle, fhr) meant those fake rows then BLOCKED the real
   data fetched later the same day. Any such row in your database is fake and
   is also the reason real data for that day is missing. Removing them lets
   the new fetcher backfill the cycle properly.

4. Reports what changed, and never drops a row without printing the count.

Usage:
    python scripts/migrate_to_v2_schema.py [--db path/to/env_data.db] [--yes]
"""

import os
import sys
import shutil
import sqlite3
import argparse
from datetime import datetime, timezone

TABLE_RENAMES = [
    ('raw_cpcb', 'raw_waqi'),
    ('cleaned_cpcb', 'cleaned_waqi'),
    ('raw_imd', 'raw_weather'),
    ('cleaned_imd', 'cleaned_weather'),
    ('raw_sentinel5p', 'raw_cams'),
    ('cleaned_sentinel5p', 'cleaned_cams'),
]

# table -> {old column: new column}
COLUMN_RENAMES = {
    'cleaned_waqi': {
        f'{p}{suffix}': f'{p}_aqi{suffix}'
        for p in ('pm25', 'pm10', 'no2', 'so2', 'co', 'o3')
        for suffix in ('_raw', '_clean', '_imputed', '_qc_flag')
    },
    'cleaned_weather': {
        'temperature_raw': 'temperature_c_raw', 'temperature_clean': 'temperature_c_clean',
        'temperature_imputed': 'temperature_c_imputed',
        'temperature_qc_flag': 'temperature_c_qc_flag',
        'humidity_raw': 'humidity_pct_raw', 'humidity_clean': 'humidity_pct_clean',
        'humidity_imputed': 'humidity_pct_imputed', 'humidity_qc_flag': 'humidity_pct_qc_flag',
        'rainfall_raw': 'rainfall_mm_raw', 'rainfall_clean': 'rainfall_mm_clean',
        'rainfall_imputed': 'rainfall_mm_imputed', 'rainfall_qc_flag': 'rainfall_mm_qc_flag',
        'wind_speed_raw': 'wind_speed_ms_raw', 'wind_speed_clean': 'wind_speed_ms_clean',
        'wind_speed_imputed': 'wind_speed_ms_imputed',
        'wind_speed_qc_flag': 'wind_speed_ms_qc_flag',
        'wind_dir_raw': 'wind_dir_deg_raw', 'wind_dir_clean': 'wind_dir_deg_clean',
        'wind_dir_imputed': 'wind_dir_deg_imputed', 'wind_dir_qc_flag': 'wind_dir_deg_qc_flag',
    },
    'cleaned_gfs': {
        'temperature_raw': 'temperature_c_raw', 'temperature_clean': 'temperature_c_clean',
        'temperature_imputed': 'temperature_c_imputed',
        'temperature_qc_flag': 'temperature_c_qc_flag',
        'precipitation_raw': 'precipitation_mm_raw',
        'precipitation_clean': 'precipitation_mm_clean',
        'precipitation_imputed': 'precipitation_mm_imputed',
        'precipitation_qc_flag': 'precipitation_mm_qc_flag',
        'u_wind_raw': 'u_wind_ms_raw', 'u_wind_clean': 'u_wind_ms_clean',
        'u_wind_imputed': 'u_wind_ms_imputed', 'u_wind_qc_flag': 'u_wind_ms_qc_flag',
        'v_wind_raw': 'v_wind_ms_raw', 'v_wind_clean': 'v_wind_ms_clean',
        'v_wind_imputed': 'v_wind_ms_imputed', 'v_wind_qc_flag': 'v_wind_ms_qc_flag',
    },
    'raw_gfs': {
        'temperature_raw': 'temperature_c', 'precipitation_raw': 'precipitation_mm',
        'u_wind_raw': 'u_wind_ms', 'v_wind_raw': 'v_wind_ms',
    },
    'cleaned_cams': {
        # These were never ppb. Open-Meteo's air quality API returns µg/m³;
        # the file's own QC comment said "µg/m³" next to a column named _ppb.
        'no2_ppb': 'no2_ugm3_raw', 'so2_ppb': 'so2_ugm3_raw',
        'co_ppb': 'co_ugm3_raw', 'o3_ppb': 'o3_ugm3_raw',
        'no2_clean': 'no2_ugm3_clean', 'so2_clean': 'so2_ugm3_clean',
        'co_clean': 'co_ugm3_clean', 'o3_clean': 'o3_ugm3_clean',
        'no2_qc_flag': 'no2_ugm3_qc_flag', 'so2_qc_flag': 'so2_ugm3_qc_flag',
        'co_qc_flag': 'co_ugm3_qc_flag', 'o3_qc_flag': 'o3_ugm3_qc_flag',
    },
    'raw_cams': {
        'no2_ppb': 'no2_ugm3', 'so2_ppb': 'so2_ugm3',
        'co_ppb': 'co_ugm3', 'o3_ppb': 'o3_ugm3',
    },
}


def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def columns_of(cur, table):
    cur.execute(f"PRAGMA table_info([{table}])")
    return [r[1] for r in cur.fetchall()]


def count_rows(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM [{table}]")
    return cur.fetchone()[0]


def migrate(db_path, assume_yes=False):
    if not os.path.exists(db_path):
        print(f"No database at {db_path}. Nothing to migrate — "
              f"run `python db.py` to create a fresh v2 schema.")
        return 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    pending_tables = [(old, new) for old, new in TABLE_RENAMES
                      if table_exists(cur, old) and not table_exists(cur, new)]
    synthetic_counts = {}
    for table in ('raw_gfs', 'cleaned_gfs'):
        if table_exists(cur, table) and 'is_synthetic' in columns_of(cur, table):
            cur.execute(f"SELECT COUNT(*) FROM [{table}] WHERE is_synthetic = 1")
            synthetic_counts[table] = cur.fetchone()[0]

    print("=== v2 schema migration — dry run ===\n")
    if pending_tables:
        for old, new in pending_tables:
            print(f"  RENAME  {old:<22} -> {new:<22} ({count_rows(cur, old)} rows)")
    else:
        print("  No table renames pending.")

    total_synthetic = sum(synthetic_counts.values())
    if total_synthetic:
        print(f"\n  QUARANTINE {total_synthetic} synthetic GFS rows "
              f"({', '.join(f'{t}: {n}' for t, n in synthetic_counts.items())})")
        print("    These are the hardcoded 25 C fallback grid. They are not")
        print("    measurements, and they blocked the real data for their cycle.")
    else:
        print("\n  No synthetic GFS rows found.")

    if not pending_tables and not total_synthetic:
        print("\nDatabase already migrated. Nothing to do.")
        conn.close()
        return 0

    if not assume_yes:
        if input("\nType 'yes' to apply: ").strip().lower() != 'yes':
            print("Aborted. No changes made.")
            conn.close()
            return 1

    backup = f"{os.path.splitext(db_path)[0]}_backup_" \
             f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(db_path, backup)
    print(f"\nBackup written to {backup}")

    for old, new in pending_tables:
        cur.execute(f"ALTER TABLE [{old}] RENAME TO [{new}]")
        print(f"  renamed {old} -> {new}")

    for table, renames in COLUMN_RENAMES.items():
        if not table_exists(cur, table):
            continue
        existing = columns_of(cur, table)
        for old_col, new_col in renames.items():
            if old_col in existing and new_col not in existing:
                cur.execute(f"ALTER TABLE [{table}] RENAME COLUMN [{old_col}] TO [{new_col}]")
                existing.append(new_col)

    if total_synthetic:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gfs_synthetic_quarantine (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_table TEXT,
                lat REAL, lon REAL, cycle TEXT, fhr TEXT,
                valid_time TEXT, fetched_at TEXT,
                quarantined_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for table, count in synthetic_counts.items():
            if not count:
                continue
            cur.execute(f"""
                INSERT INTO gfs_synthetic_quarantine
                    (origin_table, lat, lon, cycle, fhr, valid_time, fetched_at)
                SELECT '{table}', lat, lon, cycle, fhr, valid_time, fetched_at
                FROM [{table}] WHERE is_synthetic = 1
            """)
            cur.execute(f"DELETE FROM [{table}] WHERE is_synthetic = 1")
            print(f"  quarantined and removed {count} synthetic rows from {table}")

    conn.commit()
    print("\nDone. Now run `python db.py` to create any tables the v2 schema adds.")
    print("Re-run fetch_gfs.py to backfill the cycles the synthetic rows were blocking.")
    conn.close()
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--db', default=os.getenv('SQLITE_DB_PATH', 'env_data.db'))
    parser.add_argument('--yes', action='store_true', help='skip the confirmation prompt')
    args = parser.parse_args()
    sys.exit(migrate(args.db, args.yes))

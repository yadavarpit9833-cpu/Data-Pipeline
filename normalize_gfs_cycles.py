"""
normalize_gfs_cycles.py
-----------------------
One-time fix to normalize the malformed cycle='00' rows in raw_gfs and
cleaned_gfs to the correct '20260905_00z' format.

The fetched_at timestamp is 2026-09-05T15:14:04+00:00, which means the
GFS cycle being fetched was the 00z cycle for 2026-09-05.
Correct format: '20260905_00z'

Safety:
 - Prints a dry-run summary BEFORE making any changes.
 - Requires explicit user confirmation (type 'yes') to proceed.
 - After the update, verifies the row counts haven't changed.
 - Re-migrates Parquet lake for gfs_raw partition to reflect corrected cycles.
"""

import sqlite3
import pandas as pd
import os
import shutil

DB_PATH = 'env_data.db'
OLD_CYCLE = '00'
NEW_CYCLE = '20260905_00z'   # Derived from fetched_at 2026-09-05T15:14:04+00:00, GFS 00z cycle

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("=== GFS Cycle Normalization — DRY RUN ===\n")

    # Count rows that will be updated
    cur.execute("SELECT COUNT(*) FROM raw_gfs WHERE cycle = ?", (OLD_CYCLE,))
    raw_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM cleaned_gfs WHERE cycle = ?", (OLD_CYCLE,))
    cleaned_count = cur.fetchone()[0]

    print(f"Rows to UPDATE in raw_gfs:     {raw_count}  (cycle '{OLD_CYCLE}' -> '{NEW_CYCLE}')")
    print(f"Rows to UPDATE in cleaned_gfs: {cleaned_count}  (cycle '{OLD_CYCLE}' -> '{NEW_CYCLE}')")
    print(f"\nTotal rows affected: {raw_count + cleaned_count}")
    print("\nNo rows will be created or deleted — only the cycle TEXT column value changes.")

    confirm = input("\nType 'yes' to proceed with UPDATE, anything else to abort: ").strip().lower()
    if confirm != 'yes':
        print("Aborted. No changes made.")
        conn.close()
        return

    # --- Execute UPDATEs ---
    print("\nApplying UPDATEs...")
    cur.execute("UPDATE raw_gfs SET cycle = ? WHERE cycle = ?", (NEW_CYCLE, OLD_CYCLE))
    raw_updated = cur.rowcount
    cur.execute("UPDATE cleaned_gfs SET cycle = ? WHERE cycle = ?", (NEW_CYCLE, OLD_CYCLE))
    cleaned_updated = cur.rowcount
    conn.commit()

    # --- Verify counts unchanged ---
    cur.execute("SELECT cycle, COUNT(*) FROM raw_gfs GROUP BY cycle")
    print("\nraw_gfs cycles after update:")
    for row in cur.fetchall():
        print(f"  cycle={repr(row[0]):<25} rows={row[1]}")

    cur.execute("SELECT cycle, COUNT(*) FROM cleaned_gfs GROUP BY cycle")
    print("\ncleaned_gfs cycles after update:")
    for row in cur.fetchall():
        print(f"  cycle={repr(row[0]):<25} rows={row[1]}")

    # Verify no cycle='00' remain
    cur.execute("SELECT COUNT(*) FROM raw_gfs WHERE cycle = ?", (OLD_CYCLE,))
    remaining_raw = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM cleaned_gfs WHERE cycle = ?", (OLD_CYCLE,))
    remaining_cleaned = cur.fetchone()[0]

    if remaining_raw == 0 and remaining_cleaned == 0:
        print(f"\nSUCCESS: {raw_updated} raw_gfs rows and {cleaned_updated} cleaned_gfs rows updated.")
        print(f"No malformed cycle='{OLD_CYCLE}' rows remain.")
    else:
        print(f"\nWARNING: {remaining_raw} raw_gfs and {remaining_cleaned} cleaned_gfs rows still have cycle='{OLD_CYCLE}'!")

    conn.close()

    # --- Re-migrate Parquet lake ---
    print("\nRe-migrating Parquet lake to reflect corrected cycle values...")
    from storage import save_cleaned_data_parquet, DATA_DIR

    # Remove stale Parquet partitions for old malformed cycle
    for source_dir in ['cleaned_gfs', 'cleaned_gfs_raw']:
        old_parquet = os.path.join(DATA_DIR, source_dir, f"cycle={OLD_CYCLE}.parquet")
        if os.path.exists(old_parquet):
            os.remove(old_parquet)
            print(f"  Removed stale Parquet: {old_parquet}")

    conn2 = sqlite3.connect(DB_PATH)
    # Re-export raw_gfs (structured)
    raw_df = pd.read_sql_query(
        "SELECT id, lat, lon, cycle, fhr, valid_time, fetched_at, "
        "temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, "
        "raw_data_hash, source, is_synthetic, created_at FROM raw_gfs",
        conn2
    )
    for cycle_val, group_df in raw_df.groupby('cycle'):
        save_cleaned_data_parquet(
            group_df, source='gfs_raw', partition_key='cycle',
            partition_value=cycle_val, dedup_keys=['lat', 'lon', 'cycle', 'fhr'],
            pure_overwrite=True
        )
    print(f"  Re-wrote {len(raw_df)} raw_gfs rows to Parquet across {raw_df['cycle'].nunique()} cycle(s).")

    # Re-export cleaned_gfs
    clean_df = pd.read_sql_query("SELECT * FROM cleaned_gfs", conn2)
    for cycle_val, group_df in clean_df.groupby('cycle'):
        save_cleaned_data_parquet(
            group_df, source='gfs', partition_key='cycle',
            partition_value=cycle_val, dedup_keys=['lat', 'lon', 'cycle', 'fhr'],
            pure_overwrite=True
        )
    print(f"  Re-wrote {len(clean_df)} cleaned_gfs rows to Parquet across {clean_df['cycle'].nunique()} cycle(s).")
    conn2.close()

    print("\nNormalization complete. Parquet lake is now consistent with SQLite.")
    print("Dual-write mode is active. Let it run for 24+ hours before SQLite removal decision.")

if __name__ == '__main__':
    main()

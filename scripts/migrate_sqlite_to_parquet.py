import os
import pandas as pd
from db import get_db_connection
from storage import save_raw_data, save_cleaned_data_parquet, DATA_DIR

def get_row_count(table):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0

def migrate_cleaned(source, table, partition_col, partition_key, dedup_keys):
    print(f"Migrating cleaned data for {source} from {table}...")
    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()
    
    if df.empty:
        print(f"  No data found in {table}.")
        return 0

    # Group by partition column and save
    if partition_key == 'date':
        # Create a date string column for partitioning
        df['__part_date'] = df[partition_col].astype(str).str[:10]
        groups = df.groupby('__part_date')
    else:
        groups = df.groupby(partition_col)
        
    for part_val, group_df in groups:
        save_cleaned_data_parquet(
            group_df.drop(columns=['__part_date'] if partition_key == 'date' else []),
            source=source,
            partition_key=partition_key,
            partition_value=part_val,
            dedup_keys=dedup_keys,
            pure_overwrite=False # RMW to handle any overlaps
        )
    return len(df)

def migrate_raw(source, table, ext):
    """For CPCB/IMD/FIRMS: saves the raw blob per row as a file (hash-deduped)."""
    print(f"Migrating raw blob data for {source} from {table}...")
    conn = get_db_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()
    
    count = 0
    for _, r in df.iterrows():
        ts = r.get('timestamp') if 'timestamp' in df.columns else r.get('fetched_at')
        save_raw_data(source, ts, r['raw_data'], ext=ext)
        count += 1
    return count

def migrate_raw_gfs_structured():
    """
    raw_gfs is NOT a blob-per-row table — it contains structured per-gridpoint
    numeric columns (lat, lon, temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw).
    Migrate these rows to Parquet partitioned by cycle, excluding the raw_data BLOB column.
    """
    print("Migrating raw_gfs structured per-gridpoint data to Parquet (by cycle)...")
    conn = get_db_connection()
    # Deliberately exclude the raw_data BLOB column — it's a tiny per-cycle header snippet,
    # not useful per-row data and would bloat the Parquet unnecessarily.
    df = pd.read_sql_query(
        "SELECT id, lat, lon, cycle, fhr, valid_time, fetched_at, "
        "temperature_raw, precipitation_raw, u_wind_raw, v_wind_raw, "
        "raw_data_hash, source, is_synthetic, created_at FROM raw_gfs",
        conn
    )
    conn.close()
    
    if df.empty:
        print("  No data in raw_gfs.")
        return 0
    
    print(f"  Found {len(df)} rows across {df['cycle'].nunique()} cycle(s): {list(df['cycle'].unique())}")
    
    for cycle_val, group_df in df.groupby('cycle'):
        save_cleaned_data_parquet(
            group_df,
            source='gfs_raw',
            partition_key='cycle',
            partition_value=cycle_val,
            dedup_keys=['lat', 'lon', 'cycle', 'fhr'],
            pure_overwrite=True  # Each cycle is a complete, distinct unit
        )
    
    print(f"  Migrated {len(df)} rows to data/cleaned_gfs_raw/*.parquet")
    return len(df)

def count_parquet_rows(source):
    target_dir = os.path.join(DATA_DIR, f"cleaned_{source}")
    if not os.path.exists(target_dir):
        return 0
    total = 0
    for file in os.listdir(target_dir):
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(target_dir, file))
            total += len(df)
    return total

def count_raw_files(source):
    target_dir = os.path.join(DATA_DIR, 'raw', source)
    if not os.path.exists(target_dir):
        return 0
    total = 0
    for root, _, files in os.walk(target_dir):
        total += len([f for f in files if f.endswith('.json') or f.endswith('.csv') or f.endswith('.bin')])
    return total

def main():
    print("Starting Migration from SQLite to Parquet Lake...\n")
    
    # 1. Migrate Cleaned Data
    migrate_cleaned('cpcb', 'cleaned_cpcb', 'timestamp', 'date', ['station_id', 'timestamp'])
    migrate_cleaned('weather', 'cleaned_imd', 'timestamp', 'date', ['station', 'timestamp'])
    migrate_cleaned('firms', 'cleaned_firms', 'timestamp', 'date', ['lat', 'lon', 'timestamp', 'satellite'])
    migrate_cleaned('gfs', 'cleaned_gfs', 'cycle', 'cycle', ['lat', 'lon', 'valid_time'])
    
    # 2. Migrate Raw Data
    migrate_raw('cpcb', 'raw_cpcb', 'json')
    migrate_raw('weather', 'raw_imd', 'json')
    migrate_raw('firms', 'raw_firms', 'csv')
    # raw_gfs is structured per-gridpoint data, NOT a blob-per-row table.
    # Migrate it separately to Parquet, not as binary blob files.
    migrate_raw_gfs_structured()
    
    # 3. Verification Report
    print("\n" + "="*60)
    print("MIGRATION ROW COUNT VERIFICATION REPORT")
    print("="*60)
    print(f"{'Source Layer':<30} | {'SQLite Count':<15} | {'Parquet/Lake Count':<15}")
    print("-" * 60)
    
    # Cleaned checks
    sources = [
        ('cleaned_cpcb (Cleaned CPCB)', 'cleaned_cpcb', 'cpcb', count_parquet_rows),
        ('cleaned_imd (Cleaned Weather)', 'cleaned_imd', 'weather', count_parquet_rows),
        ('cleaned_firms (Cleaned FIRMS)', 'cleaned_firms', 'firms', count_parquet_rows),
        ('cleaned_gfs (Cleaned GFS)', 'cleaned_gfs', 'gfs', count_parquet_rows),
        # raw_gfs: compare Parquet row count (structured data) vs SQLite row count
        ('raw_gfs (Raw GFS gridpoints)', 'raw_gfs', 'gfs_raw', count_parquet_rows),
        # For CPCB/IMD/FIRMS raw: file count vs SQLite blob row count (hash-deduped, so mismatch is expected and intentional)
        ('raw_cpcb (blob files, hash-deduped)', 'raw_cpcb', 'cpcb', count_raw_files),
        ('raw_imd (blob files, hash-deduped)', 'raw_imd', 'weather', count_raw_files),
        ('raw_firms (blob files, hash-deduped)', 'raw_firms', 'firms', count_raw_files),
    ]
    
    for label, sqlite_table, source_name, count_func in sources:
        sqlite_count = get_row_count(sqlite_table)
        lake_count = count_func(source_name)
        match = "OK" if sqlite_count == lake_count else "MISMATCH"
        print(f"{label:<30} | {sqlite_count:<15} | {lake_count:<15} {match}")
        
    print("\nMigration complete. `env_data.db` has been preserved as a legacy backup.")
    print("Dual-write mode is active. You can now use DuckDB to query the Parquet files.")

if __name__ == "__main__":
    main()

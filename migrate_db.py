import os
import shutil
import sqlite3
import hashlib
from datetime import datetime, timezone

DB_PATH = 'env_data.db'

def py_sha256(data):
    if data is None:
        return None
    if isinstance(data, str):
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    elif isinstance(data, bytes):
        return hashlib.sha256(data).hexdigest()
    return hashlib.sha256(str(data).encode('utf-8')).hexdigest()

def get_table_counts(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    counts = {}
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM [{t}]")
        counts[t] = cur.fetchone()[0]
    return counts

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Database {DB_PATH} does not exist yet. No migration needed.")
        return

    # 1. Create safety backup
    now_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
    backup_path = f"env_data_backup_{now_str}.db"
    shutil.copy2(DB_PATH, backup_path)
    print(f"Safety backup created at: {backup_path}")

    conn = sqlite3.connect(DB_PATH)
    conn.create_function("sha256_hash", 1, py_sha256)
    conn.row_factory = sqlite3.Row
    before_counts = get_table_counts(conn)

    # 2. Read new schema
    with open('schema.sql', 'r') as f:
        schema_sql = f.read()

    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys=OFF")

    # Temp migration strategy
    tables = ['raw_cpcb', 'raw_imd', 'raw_gfs', 'raw_firms',
              'cleaned_cpcb', 'cleaned_imd', 'cleaned_gfs', 'cleaned_firms', 'pipeline_run_log']

    for t in tables:
        # Check if table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
        if not cur.fetchone():
            continue

        temp_t = f"{t}_new"
        cur.execute(f"DROP TABLE IF EXISTS [{temp_t}]")

    # Execute schema.sql on temp tables by replacing table names
    temp_schema_sql = schema_sql
    for t in tables:
        temp_schema_sql = temp_schema_sql.replace(f"CREATE TABLE IF NOT EXISTS {t} (", f"CREATE TABLE IF NOT EXISTS {t}_new (")
        temp_schema_sql = temp_schema_sql.replace(f"CONSTRAINT unq_{t} UNIQUE", f"CONSTRAINT unq_{t}_new UNIQUE")

    cur.executescript(temp_schema_sql)

    # Copy existing columns from old tables to new temp tables
    for t in tables:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
        if not cur.fetchone():
            continue

        cur.execute(f"PRAGMA table_info([{t}])")
        old_cols = [r['name'] for r in cur.fetchall() if r['name'] != 'id']

        cur.execute(f"PRAGMA table_info([{t}_new])")
        new_cols = [r['name'] for r in cur.fetchall() if r['name'] != 'id']

        dest_cols = []
        src_cols = []
        for c in new_cols:
            if c in old_cols:
                dest_cols.append(c)
                src_cols.append(c)
            elif c == 'raw_data_hash' and 'raw_data' in old_cols:
                dest_cols.append('raw_data_hash')
                src_cols.append('sha256_hash(raw_data)')
            elif c == 'fetched_at' and 'timestamp' in old_cols:
                dest_cols.append('fetched_at')
                src_cols.append('timestamp')

        if dest_cols:
            dest_str = ', '.join(dest_cols)
            src_str = ', '.join(src_cols)
            cur.execute(f"INSERT OR IGNORE INTO [{t}_new] ({dest_str}) SELECT {src_str} FROM [{t}]")

        cur.execute(f"DROP TABLE [{t}]")
        cur.execute(f"ALTER TABLE [{t}_new] RENAME TO [{t}]")

    # Backfill valid_time in raw_gfs and cleaned_gfs where valid_time is NULL
    for gfs_tbl in ['raw_gfs', 'cleaned_gfs']:
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (gfs_tbl,))
        if not cur.fetchone():
            continue
        cur.execute(f"SELECT id, cycle, fhr, fetched_at FROM [{gfs_tbl}] WHERE valid_time IS NULL")
        rows = cur.fetchall()
        for r in rows:
            fetched_ts = r['fetched_at'] or '2026-09-05T00:00:00'
            date_prefix = fetched_ts[:10].replace('-', '')
            cycle_str = str(r['cycle'] or '00')
            if '_' in cycle_str:
                full_cycle = cycle_str
            else:
                clean_c = cycle_str.lower().replace('z', '').zfill(2)
                full_cycle = f"{date_prefix}_{clean_c}z"
            fhr_str = str(r['fhr'] or '000')
            try:
                hour_part = full_cycle.split('_')[1].lower().replace('z', '')
                dt = datetime.strptime(f"{date_prefix}{hour_part}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
                from datetime import timedelta
                vt = (dt + timedelta(hours=int(fhr_str))).isoformat()
            except Exception:
                vt = fetched_ts
            cur.execute(f"UPDATE [{gfs_tbl}] SET valid_time = ? WHERE id = ?", (vt, r['id']))

    # Backfill source and is_synthetic for all tables
    print("\nBackfilling source and is_synthetic tags...")
    tables_sources = {
        'raw_cpcb': 'cpcb',
        'cleaned_cpcb': 'cpcb',
        'raw_firms': 'firms',
        'cleaned_firms': 'firms',
        'raw_imd': 'open-meteo',
        'cleaned_imd': 'open-meteo',
        'raw_gfs': 'noaa',
        'cleaned_gfs': 'noaa'
    }

    gfs_fallback_retag_count = 0

    for tbl, default_source in tables_sources.items():
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
        if not cur.fetchone():
            continue
        
        # Default all rows
        cur.execute(f"UPDATE [{tbl}] SET source = ?, is_synthetic = 0 WHERE source IS NULL", (default_source,))
        
        # Retroactive tagging for GFS fallback constants
        if tbl in ['raw_gfs', 'cleaned_gfs']:
            cur.execute(f"""
                UPDATE [{tbl}]
                SET source = 'fallback_constant', is_synthetic = 1
                WHERE temperature_raw = 25.0
                  AND precipitation_raw = 0.0
                  AND u_wind_raw = 1.0
                  AND v_wind_raw = 1.0
                  AND is_synthetic = 0
            """)
            retagged = cur.rowcount
            gfs_fallback_retag_count += retagged
            print(f"Retagged {retagged} rows in {tbl} as fallback_constant.")

    print(f"Total historical GFS rows retagged as fallback_constant: {gfs_fallback_retag_count}")

    conn.commit()
    after_counts = get_table_counts(conn)
    conn.close()

    print("\n" + "="*70)
    print(f"{'Table Name':<25} | {'Before Migration':<18} | {'After Migration':<18} | {'Difference'}")
    print("-" * 70)
    for t in sorted(set(list(before_counts.keys()) + list(after_counts.keys()))):
        b = before_counts.get(t, 0)
        a = after_counts.get(t, 0)
        diff = a - b
        diff_str = f"{diff} (duplicates dropped)" if diff < 0 else "0"
        print(f"{t:<25} | {b:<18} | {a:<18} | {diff_str}")
    print("="*70 + "\n")

if __name__ == '__main__':
    migrate()


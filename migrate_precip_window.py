"""
Add precipitation_window_h to raw_gfs and cleaned_gfs, and clear the precipitation
values that were never measurements.

Additive only, so this uses ALTER TABLE rather than migrate_db.py's rebuild: the
database is ~1.8 GB and a rebuild would copy it twice for one nullable column.
Safe to run more than once - it checks what is already there.

What the column means
---------------------
GFS APCP is an accumulation bucket, not a rate, and the bucket length varies with
forecast hour: 3 h at f003/f009/f015, 6 h at f006/f012/f018. Without a window, a
precipitation number cannot be compared across rows. NULL means there is no window
and the precipitation columns hold no measurement.

Why existing precipitation values are cleared
---------------------------------------------
Every row in the table is f000, where APCP does not exist: NOMADS omits the field
and fetch_gfs.py filled `[0.0] * len(grid)`. Those zeros were never observed. A NULL
window alongside a 0.0 value still reads as "it did not rain" to anything that does
not check the window, which is the failure this column exists to prevent, so the
zeros go too. Nothing is lost - the column held exactly one distinct value.
"""
import os
import glob
import sqlite3
import argparse
import tempfile

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'env_data.db')
PARQUET_DIR = os.path.join(BASE_DIR, 'data', 'cleaned_gfs')

TABLES = ('raw_gfs', 'cleaned_gfs')
# Only the f000 rows are being cleared, and every row is f000; the predicate is
# spelled out anyway so a later run cannot touch forecast rows.
NO_WINDOW = "CAST(fhr AS INTEGER) = 0"


def columns(cur, table):
    cur.execute(f"PRAGMA table_info([{table}])")
    return {r[1] for r in cur.fetchall()}


def migrate_parquet(dry_run):
    import numpy as np
    import pandas as pd

    files = sorted(glob.glob(os.path.join(PARQUET_DIR, '*.parquet')))
    if not files:
        print("\ndata/cleaned_gfs: no Parquet partitions found")
        return

    print()
    for path in files:
        df = pd.read_parquet(path)
        name = os.path.basename(path)
        rows = (df['fhr'].astype(str).str.lstrip('0').replace('', '0').astype(int) == 0
                if 'fhr' in df.columns else pd.Series(True, index=df.index))
        touched = int((rows & df.get('precipitation_raw', pd.Series(np.nan, index=df.index))
                       .notna()).sum())
        has_window = 'precipitation_window_h' in df.columns

        if not touched and has_window:
            print(f"data/cleaned_gfs/{name}: already migrated")
            continue
        if dry_run:
            print(f"data/cleaned_gfs/{name}: would clear {touched:,} f000 rows"
                  f"{'' if has_window else ' and add precipitation_window_h'}")
            continue

        if not has_window:
            df['precipitation_window_h'] = np.nan
        for col in ('precipitation_raw', 'precipitation_clean'):
            if col in df.columns:
                df.loc[rows, col] = np.nan
        df.loc[rows, 'precipitation_window_h'] = np.nan
        if 'precipitation_qc_flag' in df.columns:
            df.loc[rows, 'precipitation_qc_flag'] = 'no_accumulation_window'
        if 'precipitation_imputed' in df.columns:
            # bool in Parquet, integer in SQLite - assigning 0 to a bool column
            # raises rather than coercing.
            df.loc[rows, 'precipitation_imputed'] = (
                False if df['precipitation_imputed'].dtype == bool else 0)

        fd, tmp = tempfile.mkstemp(suffix='.parquet', dir=PARQUET_DIR)
        os.close(fd)
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
        print(f"data/cleaned_gfs/{name}: cleared {touched:,} f000 rows, window column present")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--db', default=DB_PATH)
    ap.add_argument('--dry-run', action='store_true', help='report, change nothing')
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=180)
    conn.execute('PRAGMA busy_timeout = 120000')
    cur = conn.cursor()

    for table in TABLES:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            print(f"{table}: not present, skipped")
            continue

        if 'precipitation_window_h' in columns(cur, table):
            print(f"{table}: precipitation_window_h already present")
        elif args.dry_run:
            print(f"{table}: would add precipitation_window_h INTEGER")
        else:
            cur.execute(f"ALTER TABLE [{table}] ADD COLUMN precipitation_window_h INTEGER")
            print(f"{table}: added precipitation_window_h INTEGER")

        cur.execute(f"SELECT COUNT(*) FROM [{table}] WHERE {NO_WINDOW} "
                    "AND precipitation_raw IS NOT NULL")
        n = cur.fetchone()[0]
        clean_col = 'precipitation_clean' in columns(cur, table)

        if not n:
            print(f"{table}: no f000 rows with a precipitation value to clear")
            continue
        if args.dry_run:
            print(f"{table}: would clear precipitation on {n:,} f000 rows")
            continue

        sets = ['precipitation_raw = NULL', 'precipitation_window_h = NULL']
        if clean_col:
            sets += ['precipitation_clean = NULL',
                     "precipitation_qc_flag = 'no_accumulation_window'",
                     'precipitation_imputed = 0']
        cur.execute(f"UPDATE [{table}] SET {', '.join(sets)} WHERE {NO_WINDOW}")
        print(f"{table}: cleared precipitation on {cur.rowcount:,} f000 rows")

    if not args.dry_run:
        conn.commit()

    # The Parquet lake is the other half of the silver layer, and it is what the
    # gold layer actually reads. Leaving the zeros here would keep precip_total at
    # a measured-looking 0.0 while the table says NULL - the two disagreeing is
    # worse than either alone.
    migrate_parquet(args.dry_run)

    print()
    for table in TABLES:
        if 'precipitation_window_h' not in columns(cur, table):
            print(f"{table}: column not present (dry run)")
            continue
        cur.execute(f"SELECT COUNT(*) total, "
                    f"SUM(precipitation_window_h IS NULL) null_window, "
                    f"SUM(precipitation_raw IS NULL) null_precip FROM [{table}]")
        total, nw, npv = cur.fetchone()
        print(f"{table}: {total:,} rows, {nw:,} with NULL window, {npv:,} with NULL precipitation")
    conn.close()


if __name__ == '__main__':
    main()

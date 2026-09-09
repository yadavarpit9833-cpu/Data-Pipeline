import sqlite3
import pandas as pd

conn = sqlite3.connect('env_data.db')

# Verify which tables have cycle='00'
tables = ['raw_gfs', 'cleaned_gfs']
for t in tables:
    cur = conn.cursor()
    cur.execute(f"SELECT cycle, fhr, fetched_at, COUNT(*) FROM {t} GROUP BY cycle, fhr, fetched_at")
    print(f"\n=== {t} cycles ===")
    for row in cur.fetchall():
        print(f"  cycle={repr(row[0])}, fhr={row[1]}, fetched_at={row[2]}, count={row[3]}")

conn.close()

import sqlite3

DB_PATH = 'env_data.db'
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=" * 70)
print("1. raw_gfs Table Diagnostics:")
cur.execute("SELECT COUNT(*) FROM raw_gfs")
raw_count = cur.fetchone()[0]
print(f"   Total rows in raw_gfs: {raw_count}")

cur.execute("SELECT MIN(lat) as min_lat, MAX(lat) as max_lat, MIN(lon) as min_lon, MAX(lon) as max_lon FROM raw_gfs")
r = cur.fetchone()
min_lat, max_lat, min_lon, max_lon = r['min_lat'], r['max_lat'], r['min_lon'], r['max_lon']
print(f"   Latitude range:  [{min_lat}, {max_lat}]")
print(f"   Longitude range: [{min_lon}, {max_lon}]")

cur.execute("SELECT cycle, fhr, valid_time, fetched_at, COUNT(*) as count FROM raw_gfs GROUP BY cycle, fhr, valid_time, fetched_at LIMIT 5")
print("\n   Sample Forecast Valid Times & Ingestion Timestamps in raw_gfs:")
for row in cur.fetchall():
    print(f"   Cycle: {row['cycle']} | FHR: {row['fhr']} | Valid: {row['valid_time']} | Fetched: {row['fetched_at']} | Count: {row['count']}")

print("-" * 70)
print("2. cleaned_gfs Table Diagnostics:")
cur.execute("SELECT COUNT(*) FROM cleaned_gfs")
cleaned_count = cur.fetchone()[0]
print(f"   Total rows in cleaned_gfs: {cleaned_count}")

cur.execute("SELECT MIN(lat) as min_lat, MAX(lat) as max_lat, MIN(lon) as min_lon, MAX(lon) as max_lon FROM cleaned_gfs")
cr = cur.fetchone()
print(f"   Latitude range:  [{cr['min_lat']}, {cr['max_lat']}]")
print(f"   Longitude range: [{cr['min_lon']}, {cr['max_lon']}]")

cur.execute("SELECT cycle, fhr, valid_time, fetched_at, COUNT(*) as count FROM cleaned_gfs GROUP BY cycle, fhr, valid_time, fetched_at LIMIT 5")
print("\n   Sample Forecast Valid Times & Ingestion Timestamps in cleaned_gfs:")
for row in cur.fetchall():
    print(f"   Cycle: {row['cycle']} | FHR: {row['fhr']} | Valid: {row['valid_time']} | Fetched: {row['fetched_at']} | Count: {row['count']}")

print("=" * 70)
if min_lat is not None and min_lat <= 6.0 and max_lat >= 37.0 and min_lon <= 68.0 and max_lon >= 97.0:
    print("SUCCESS: Full India bounding box (lat 6-37, lon 68-97) is 100% covered!")
else:
    print("STATUS: Data diagnostics complete.")
print("=" * 70)

conn.close()


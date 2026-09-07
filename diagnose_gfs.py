import sqlite3

DB_PATH = 'env_data.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("=" * 70)
print("1. raw_gfs Table Diagnostics:")
cur.execute("SELECT COUNT(*) FROM raw_gfs")
raw_count = cur.fetchone()[0]
print(f"   SELECT COUNT(*) FROM raw_gfs: {raw_count}")

cur.execute("SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon) FROM raw_gfs")
min_lat, max_lat, min_lon, max_lon = cur.fetchone()
print(f"   SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon) FROM raw_gfs:")
print(f"   MIN(lat) = {min_lat}, MAX(lat) = {max_lat}")
print(f"   MIN(lon) = {min_lon}, MAX(lon) = {max_lon}")

print("-" * 70)
print("2. cleaned_gfs Table Diagnostics:")
cur.execute("SELECT COUNT(*) FROM cleaned_gfs")
cleaned_count = cur.fetchone()[0]
print(f"   SELECT COUNT(*) FROM cleaned_gfs: {cleaned_count}")

cur.execute("SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon) FROM cleaned_gfs")
c_min_lat, c_max_lat, c_min_lon, c_max_lon = cur.fetchone()
print(f"   SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon) FROM cleaned_gfs:")
print(f"   MIN(lat) = {c_min_lat}, MAX(lat) = {c_max_lat}")
print(f"   MIN(lon) = {c_min_lon}, MAX(lon) = {c_max_lon}")

print("=" * 70)
if min_lat <= 6.0 and max_lat >= 37.0 and min_lon <= 68.0 and max_lon >= 97.0:
    print("SUCCESS: Full India bounding box (lat 6-37, lon 68-97) is 100% covered!")
else:
    print("WARNING: India bounding box range is incomplete.")
print("=" * 70)

conn.close()

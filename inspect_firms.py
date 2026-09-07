"""
inspect_firms.py — Inspection script to analyze FIRMS raw payloads and cleaned rows.
"""
import sqlite3

DB_PATH = 'env_data.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print("=" * 80)
    print("1. raw_firms TABLE ENTRIES:")
    print("=" * 80)
    cur.execute("SELECT id, timestamp, length(raw_data) as len_bytes, substr(raw_data, 1, 120) as preview FROM raw_firms ORDER BY id DESC LIMIT 6")
    raw_rows = cur.fetchall()
    for r in raw_rows:
        print(f"ID: {r['id']} | Timestamp: {r['timestamp']} | Payload Size: {r['len_bytes']} bytes")
        print(f"   Preview: {r['preview'].strip()[:100]}...\n")

    print("=" * 80)
    print("2. cleaned_firms RUN BREAKDOWN (Rows per Timestamp):")
    print("=" * 80)
    cur.execute("SELECT timestamp, satellite, COUNT(*) as count FROM cleaned_firms GROUP BY timestamp, satellite ORDER BY timestamp DESC")
    cleaned_breakdown = cur.fetchall()
    for cb in cleaned_breakdown:
        print(f"Timestamp: {cb['timestamp']} | Satellite/Sensor: {cb['satellite']:<15} | Count: {cb['count']}")

    print("\n" + "=" * 80)
    print("3. DUPLICATE CHECK WITHIN SAME RUN (same lat, lon, satellite, timestamp):")
    print("=" * 80)
    cur.execute("""
        SELECT lat, lon, satellite, timestamp, COUNT(*) as dup_count
        FROM cleaned_firms
        GROUP BY lat, lon, satellite, timestamp
        HAVING COUNT(*) > 1
        LIMIT 5
    """)
    dups = cur.fetchall()
    if dups:
        print(f"Found {len(dups)} duplicate clusters within same run:")
        for d in dups:
            print(f"   Lat: {d['lat']}, Lon: {d['lon']}, Sat: {d['satellite']}, Timestamp: {d['timestamp']} -> Count: {d['dup_count']}")
    else:
        print("[OK] NO DUPLICATES WITHIN SAME RUN: Every fire detection record is a distinct satellite observation!")

    conn.close()

if __name__ == '__main__':
    main()

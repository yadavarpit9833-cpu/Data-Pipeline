"""
test_idempotency.py — Deterministic test suite for database idempotency.
Verifies that repeating identical insertions or scheduler runs does not duplicate records in any raw or cleaned table.
"""
import os
import sqlite3
import unittest
import hashlib
import json

TEST_DB_PATH = 'test_idempotency.db'
SCHEMA_FILE = 'schema.sql'

def compute_hash(data):
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = str(data).encode('utf-8')
    return hashlib.sha256(b).hexdigest()

class TestDatabaseIdempotency(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        
        with open(SCHEMA_FILE, 'r') as f:
            schema_sql = f.read()
            
        conn = sqlite3.connect(TEST_DB_PATH)
        conn.executescript(schema_sql)
        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TEST_DB_PATH):
            try:
                os.remove(TEST_DB_PATH)
            except Exception:
                pass

    def setUp(self):
        self.conn = sqlite3.connect(TEST_DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_raw_cpcb_idempotency(self):
        payload = json.dumps({'idx': 123, 'city': 'delhi', 'pm25': 150})
        p_hash = compute_hash(payload)
        ts = "2026-09-09T10:00:00+00:00"

        # First insert
        self.cur.execute(
            "INSERT OR IGNORE INTO raw_cpcb (timestamp, raw_data, raw_data_hash) VALUES (?, ?, ?)",
            (ts, payload, p_hash)
        )
        self.conn.commit()

        # Second duplicate insert
        self.cur.execute(
            "INSERT OR IGNORE INTO raw_cpcb (timestamp, raw_data, raw_data_hash) VALUES (?, ?, ?)",
            (ts, payload, p_hash)
        )
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM raw_cpcb WHERE timestamp = ? AND raw_data_hash = ?", (ts, p_hash))
        self.assertEqual(self.cur.fetchone()[0], 1, "raw_cpcb duplicated identical timestamp + payload hash!")

    def test_raw_imd_idempotency(self):
        payload = json.dumps({'station': 'Lucknow', 'temp': 32.5})
        p_hash = compute_hash(payload)
        ts = "2026-09-09T10:00:00+00:00"

        self.cur.execute(
            "INSERT OR IGNORE INTO raw_imd (timestamp, raw_data, raw_data_hash) VALUES (?, ?, ?)",
            (ts, payload, p_hash)
        )
        self.cur.execute(
            "INSERT OR IGNORE INTO raw_imd (timestamp, raw_data, raw_data_hash) VALUES (?, ?, ?)",
            (ts, payload, p_hash)
        )
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM raw_imd WHERE timestamp = ? AND raw_data_hash = ?", (ts, p_hash))
        self.assertEqual(self.cur.fetchone()[0], 1, "raw_imd duplicated identical timestamp + payload hash!")

    def test_raw_gfs_idempotency(self):
        lat, lon = 28.5, 77.2
        cycle = "20260909_00z"
        fhr = "003"
        valid_time = "2026-09-09T03:00:00+00:00"
        fetched_at = "2026-09-09T06:15:00+00:00"

        for _ in range(3):
            self.cur.execute("""
                INSERT OR IGNORE INTO raw_gfs (lat, lon, cycle, fhr, valid_time, fetched_at, temperature_raw)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (lat, lon, cycle, fhr, valid_time, fetched_at, 29.5))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM raw_gfs WHERE lat=? AND lon=? AND cycle=? AND fhr=?", (lat, lon, cycle, fhr))
        self.assertEqual(self.cur.fetchone()[0], 1, "raw_gfs duplicated identical (lat, lon, cycle, fhr)!")

    def test_raw_firms_idempotency(self):
        payload = "lat,lon,brightness,acq_date,acq_time\n28.5,77.2,320.5,2026-09-09,0530"
        p_hash = compute_hash(payload)
        ts = "2026-09-09T05:30:00+00:00"

        for _ in range(2):
            self.cur.execute(
                "INSERT OR IGNORE INTO raw_firms (timestamp, raw_data, raw_data_hash) VALUES (?, ?, ?)",
                (ts, payload, p_hash)
            )
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM raw_firms WHERE timestamp = ? AND raw_data_hash = ?", (ts, p_hash))
        self.assertEqual(self.cur.fetchone()[0], 1, "raw_firms duplicated identical timestamp + payload hash!")

    def test_cleaned_cpcb_idempotency(self):
        station_id = "site_delhi_01"
        ts = "2026-09-09T10:00:00+00:00"

        for _ in range(2):
            self.cur.execute("""
                INSERT OR IGNORE INTO cleaned_cpcb (station_id, city, timestamp, pm25_raw, pm25_clean, pm25_qc_flag)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (station_id, "delhi", ts, 120.0, 120.0, "ok"))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM cleaned_cpcb WHERE station_id = ? AND timestamp = ?", (station_id, ts))
        self.assertEqual(self.cur.fetchone()[0], 1, "cleaned_cpcb duplicated (station_id, timestamp)!")

    def test_cleaned_imd_idempotency(self):
        station = "Lucknow"
        ts = "2026-09-09T10:00:00+00:00"

        for _ in range(2):
            self.cur.execute("""
                INSERT OR IGNORE INTO cleaned_imd (station, timestamp, temperature_raw, temperature_clean, temperature_qc_flag)
                VALUES (?, ?, ?, ?, ?)
            """, (station, ts, 32.0, 32.0, "ok"))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM cleaned_imd WHERE station = ? AND timestamp = ?", (station, ts))
        self.assertEqual(self.cur.fetchone()[0], 1, "cleaned_imd duplicated (station, timestamp)!")

    def test_cleaned_gfs_idempotency(self):
        lat, lon = 28.5, 77.2
        cycle = "20260909_00z"
        fhr = "003"
        valid_time = "2026-09-09T03:00:00+00:00"
        fetched_at = "2026-09-09T06:15:00+00:00"

        for _ in range(2):
            self.cur.execute("""
                INSERT OR IGNORE INTO cleaned_gfs (lat, lon, cycle, fhr, valid_time, fetched_at, temperature_raw, temperature_clean)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (lat, lon, cycle, fhr, valid_time, fetched_at, 28.0, 28.0))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM cleaned_gfs WHERE lat=? AND lon=? AND cycle=? AND fhr=?", (lat, lon, cycle, fhr))
        self.assertEqual(self.cur.fetchone()[0], 1, "cleaned_gfs duplicated (lat, lon, cycle, fhr)!")

    def test_cleaned_firms_idempotency(self):
        lat, lon = 28.5, 77.2
        ts = "2026-09-09T05:30:00+00:00"
        sat = "VIIRS_SNPP"

        for _ in range(2):
            self.cur.execute("""
                INSERT OR IGNORE INTO cleaned_firms (lat, lon, timestamp, satellite, brightness_raw, brightness_clean)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (lat, lon, ts, sat, 340.5, 340.5))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM cleaned_firms WHERE lat=? AND lon=? AND timestamp=? AND satellite=?", (lat, lon, ts, sat))
        self.assertEqual(self.cur.fetchone()[0], 1, "cleaned_firms duplicated (lat, lon, timestamp, satellite)!")

    def test_raw_sentinel5p_idempotency(self):
        lat, lon = 28.5, 77.2
        ts = "2026-09-09T00:00:00+00:00"

        for _ in range(3):
            self.cur.execute("""
                INSERT OR IGNORE INTO raw_sentinel5p (lat, lon, timestamp, no2_ppb, so2_ppb, co_ppb, o3_ppb, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (lat, lon, ts, 12.5, 3.1, 180.0, 45.0, "2026-09-09T06:15:00+00:00"))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM raw_sentinel5p WHERE lat=? AND lon=? AND timestamp=?", (lat, lon, ts))
        self.assertEqual(self.cur.fetchone()[0], 1, "raw_sentinel5p duplicated identical (lat, lon, timestamp)!")

    def test_cleaned_sentinel5p_idempotency(self):
        lat, lon = 28.5, 77.2
        ts = "2026-09-09T00:00:00+00:00"

        for _ in range(2):
            self.cur.execute("""
                INSERT OR IGNORE INTO cleaned_sentinel5p (lat, lon, timestamp, no2_ppb, no2_clean, no2_qc_flag)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (lat, lon, ts, 12.5, 12.5, "ok"))
        self.conn.commit()

        self.cur.execute("SELECT COUNT(*) FROM cleaned_sentinel5p WHERE lat=? AND lon=? AND timestamp=?", (lat, lon, ts))
        self.assertEqual(self.cur.fetchone()[0], 1, "cleaned_sentinel5p duplicated (lat, lon, timestamp)!")

    def test_gfs_compute_valid_time(self):
        from fetch_gfs import compute_valid_time
        # Test 00Z cycle + 003 fhr
        vt = compute_valid_time("20260909_00z", "003")
        self.assertEqual(vt, "2026-09-09T03:00:00+00:00")

        # Test 12Z cycle + 024 fhr (rolls over to next day)
        vt2 = compute_valid_time("20260909_12z", "024")
        self.assertEqual(vt2, "2026-09-10T12:00:00+00:00")

if __name__ == '__main__':
    unittest.main()

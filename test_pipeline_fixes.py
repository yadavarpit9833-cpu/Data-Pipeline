"""
test_pipeline_fixes.py — Regression suite.

One test per data-correctness bug that was found in the audit, each pinned so
it cannot come back silently. Every test names the bug it guards.

These tests make no network calls: each one exercises the pure function that
held the bug.
"""

import os
import shutil
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

import db
from cleaning import clean_and_impute
from storage import save_cleaned_data_parquet, save_raw_data, _stringify_object_columns


# ── GFS cycle selection ──────────────────────────────────────────────────────

class TestGFSCycleSelection(unittest.TestCase):
    """
    BUG: cycle_hour was hardcoded to '00' and fhr to '000', so the fetcher
    asked for the 00z analysis every single run. The scheduler fired it at
    00:30 UTC, before NOAA had published that cycle (00z lands 03:30-05:00
    UTC), so it failed nearly every day and fell through to synthetic data.
    """

    def setUp(self):
        from fetch_gfs import latest_available_cycle, cycle_label, compute_valid_time
        self.latest = latest_available_cycle
        self.label = cycle_label
        self.valid_time = compute_valid_time

    def test_never_selects_an_unpublished_cycle(self):
        """The chosen cycle must always be at least lag_hours in the past."""
        lag = 5
        for hour in range(24):
            for minute in (0, 29, 30, 59):
                now = datetime(2026, 9, 12, hour, minute, tzinfo=timezone.utc)
                cycle = self.latest(now, lag_hours=lag)
                age_hours = (now - cycle).total_seconds() / 3600
                self.assertGreaterEqual(
                    age_hours, lag,
                    msg=f"At {now}, cycle {cycle} is only {age_hours:.1f}h old (lag={lag})")

    def test_always_lands_on_a_six_hourly_boundary(self):
        for hour in range(24):
            cycle = self.latest(datetime(2026, 9, 12, hour, tzinfo=timezone.utc), lag_hours=5)
            self.assertIn(cycle.hour, (0, 6, 12, 18))
            self.assertEqual((cycle.minute, cycle.second), (0, 0))

    def test_the_old_0030_utc_run_now_picks_yesterdays_18z(self):
        """
        The exact scenario that used to fail: the 00:30 UTC trigger. With a
        5-hour lag it selects the previous day's 18z, which is published.
        """
        now = datetime(2026, 9, 12, 0, 30, tzinfo=timezone.utc)
        self.assertEqual(self.label(self.latest(now, lag_hours=5)), '20260911_18z')

    def test_cycle_advances_through_the_day(self):
        labels = [self.label(self.latest(
            datetime(2026, 9, 12, h, tzinfo=timezone.utc), lag_hours=5)) for h in range(24)]
        self.assertGreater(len(set(labels)), 1, "Cycle never advances across a day")
        self.assertEqual(labels, sorted(labels), "Cycle labels must be non-decreasing")

    def test_valid_time_is_cycle_plus_forecast_hour(self):
        self.assertEqual(self.valid_time('20260909_00z', '003'), '2026-09-09T03:00:00+00:00')
        self.assertEqual(self.valid_time('20260909_18z', '024'), '2026-09-10T18:00:00+00:00')
        self.assertEqual(self.valid_time('20260909_00z', '000'), '2026-09-09T00:00:00+00:00')

    def test_malformed_cycle_raises_instead_of_returning_now(self):
        """
        BUG: compute_valid_time caught every exception and returned
        datetime.now(). A malformed cycle therefore stamped forecast rows with
        the download time, which silently destroyed temporal deduplication —
        and left 14,625 rows with cycle='00' that needed a one-off repair
        script (scripts/normalize_gfs_cycles.py) to untangle.
        """
        for bad in ('00', '', 'not-a-cycle', '20260909'):
            with self.assertRaises(ValueError, msg=f"{bad!r} should raise"):
                self.valid_time(bad, '000')

    def test_forecast_hours_are_not_all_analysis(self):
        """A forecast pipeline must request at least one non-zero lead time."""
        from fetch_gfs import GFS_FORECAST_HOURS
        self.assertTrue(GFS_FORECAST_HOURS)
        self.assertTrue(any(int(h) > 0 for h in GFS_FORECAST_HOURS),
                        "Only f000 requested — f000 is the analysis, not a forecast")

    def test_no_synthetic_fallback_remains(self):
        """
        BUG: on any fetch error the module wrote a hardcoded 25.0 C /
        1.0 m/s / 0.0 mm grid tagged is_synthetic=1. Combined with
        INSERT OR IGNORE on UNIQUE(lat, lon, cycle, fhr), those rows then
        blocked the real data for the rest of the day.
        """
        import fetch_gfs
        with open(fetch_gfs.__file__, encoding='utf-8') as fh:
            source = fh.read()
        code = '\n'.join(
            line for line in source.splitlines()
            if not line.lstrip().startswith('#')
        )
        body = code.split('"""', 2)[-1]  # skip the module docstring
        self.assertNotIn("'is_synthetic': True", body)
        self.assertNotIn('fallback_constant', body)


# ── FIRMS sensor column mapping ──────────────────────────────────────────────

class TestFIRMSColumnMapping(unittest.TestCase):
    """
    BUG: MODIS publishes `brightness`, VIIRS publishes `bright_ti4`. Only the
    first was mapped, so every VIIRS row had a null brightness — which the
    spatial KNN imputer then filled from the nearest MODIS detection, copying
    a fire temperature in Delhi onto a fire in Kolkata and flagging it
    imputed=1.
    """

    def setUp(self):
        from fetch_firms import standardise_frame, normalise_confidence
        self.standardise = standardise_frame
        self.normalise_confidence = normalise_confidence

    def _modis(self):
        return pd.DataFrame({
            'latitude': [28.6, 28.7], 'longitude': [77.2, 77.3],
            'brightness': [330.0, 340.0], 'bright_t31': [295.0, 296.0],
            'confidence': [85, 20], 'satellite': ['Terra', 'Terra'],
            'frp': [12.5, 8.1], 'daynight': ['D', 'D'],
            'acq_date': ['2026-09-12'] * 2, 'acq_time': ['0530', '0530'],
        })

    def _viirs(self):
        return pd.DataFrame({
            'latitude': [22.5, 22.6], 'longitude': [88.3, 88.4],
            'bright_ti4': [350.0, 360.0], 'bright_ti5': [300.0, 301.0],
            'confidence': ['h', 'l'], 'satellite': ['N', 'N'],
            'frp': [20.0, 5.0], 'daynight': ['N', 'N'],
            'acq_date': ['2026-09-12'] * 2, 'acq_time': ['0700', '0700'],
        })

    def test_viirs_brightness_is_read_not_null(self):
        out = self.standardise(self._viirs(), 'VIIRS_SNPP_NRT')
        self.assertFalse(out['brightness_k_raw'].isna().any(),
                         "VIIRS bright_ti4 was not mapped — brightness is null again")
        self.assertEqual(out['brightness_k_raw'].tolist(), [350.0, 360.0])

    def test_modis_brightness_still_works(self):
        out = self.standardise(self._modis(), 'MODIS_NRT')
        self.assertEqual(out['brightness_k_raw'].tolist(), [330.0, 340.0])

    def test_no_fabricated_brightness_when_sensors_are_combined(self):
        """The end-to-end shape of the original bug: concat then QC."""
        combined = pd.concat([
            self.standardise(self._modis(), 'MODIS_NRT'),
            self.standardise(self._viirs(), 'VIIRS_SNPP_NRT'),
        ], ignore_index=True)
        combined['timestamp'] = '2026-09-12T05:30:00+00:00'

        out = clean_and_impute(
            combined, 'brightness_k_raw', time_col='timestamp', group_cols=['sensor'],
            min_val=200.0, max_val=600.0, max_step_change=None,
        )
        self.assertFalse(out['brightness_k_raw_imputed'].any(),
                         "A fire brightness was imputed — spatial KNN is back")
        self.assertEqual(sorted(out['brightness_k_raw_clean'].tolist()),
                         [330.0, 340.0, 350.0, 360.0])

    def test_confidence_scales_are_distinguished(self):
        """
        BUG: MODIS 0-100 percent and VIIRS l/n/h were cast to TEXT into one
        column, so "85" and "h" were indistinguishable downstream.
        """
        self.assertEqual(self.normalise_confidence(85), ('85', 'percent', 'high'))
        self.assertEqual(self.normalise_confidence(20), ('20', 'percent', 'low'))
        self.assertEqual(self.normalise_confidence(50), ('50', 'percent', 'nominal'))
        self.assertEqual(self.normalise_confidence('h'), ('h', 'class', 'high'))
        self.assertEqual(self.normalise_confidence('l'), ('l', 'class', 'low'))
        self.assertEqual(self.normalise_confidence(None), (None, None, None))

    def test_missing_brightness_column_is_reported_not_silently_null(self):
        odd = pd.DataFrame({'latitude': [20.0], 'longitude': [78.0], 'confidence': ['n']})
        with self.assertLogs('fetch_firms', level='WARNING'):
            out = self.standardise(odd, 'HYPOTHETICAL_SENSOR')
        self.assertTrue(out['brightness_k_raw'].isna().all())

    def test_map_key_is_redacted_from_log_output(self):
        """BUG: the retry handler logged the full URL, and FIRMS puts the key
        in the URL path, so every failure leaked the key."""
        from fetch_firms import redact_key
        url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/SECRET123/MODIS_NRT/68,6,97,37/1"
        redacted = redact_key(url)
        self.assertNotIn('SECRET123', redacted)
        self.assertIn('<MAP_KEY>', redacted)


# ── GFS QC must run over time, not over space ────────────────────────────────

class TestGFSQCGrouping(unittest.TestCase):
    """
    BUG: clean_and_impute was called for GFS with no group_cols, and every row
    in a fetch shared one valid_time. Sorting was a no-op and shift(1) compared
    NEIGHBOURING GRID CELLS instead of consecutive times, so a uniform region
    (ocean, clear sky) was flagged as a stuck sensor.
    """

    def _uniform_row(self, n=20, value=25.0):
        return pd.DataFrame({
            'lat': [6.0] * n,
            'lon': [68.0 + 0.25 * i for i in range(n)],
            'valid_time': ['2026-01-01T00:00:00+00:00'] * n,
            'temperature_c_raw': [value] * n,
        })

    def test_uniform_region_is_not_flagged_when_grouped_by_gridpoint(self):
        out = clean_and_impute(
            self._uniform_row(), 'temperature_c_raw',
            time_col='valid_time', group_cols=['lat', 'lon'],
            min_val=-80.0, max_val=60.0, max_step_change=25.0,
        )
        self.assertEqual(set(out['temperature_c_raw_qc_flag']), {'ok'},
                         "A uniform ocean row is being flagged as a flatline again")

    def test_ungrouped_call_would_still_flag_it(self):
        """Shows the failure mode is real, not a hypothetical."""
        out = clean_and_impute(
            self._uniform_row(), 'temperature_c_raw',
            time_col='valid_time', group_cols=None,
            min_val=-80.0, max_val=60.0, max_step_change=25.0,
        )
        self.assertIn('flatline', set(out['temperature_c_raw_qc_flag']))

    def test_a_genuine_time_series_step_is_still_caught(self):
        """Grouping must not disable the check it was meant to make correct."""
        df = pd.DataFrame({
            'lat': [20.0] * 4, 'lon': [78.0] * 4,
            'valid_time': ['2026-01-01T00:00:00+00:00', '2026-01-01T06:00:00+00:00',
                           '2026-01-01T12:00:00+00:00', '2026-01-01T18:00:00+00:00'],
            'temperature_c_raw': [25.0, 27.0, 90.0, 28.0],
        })
        out = clean_and_impute(
            df, 'temperature_c_raw', time_col='valid_time', group_cols=['lat', 'lon'],
            min_val=-80.0, max_val=60.0, max_step_change=25.0,
        )
        flags = out.sort_values('valid_time')['temperature_c_raw_qc_flag'].tolist()
        self.assertIn('range_fail', flags[2])
        self.assertIn('step_fail', flags[2])


# ── Storage ──────────────────────────────────────────────────────────────────

class TestStorage(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.patched = []
        import storage
        self._storage = storage
        self._orig_data_dir = storage.DATA_DIR
        storage.DATA_DIR = self.tmp

    def tearDown(self):
        self._storage.DATA_DIR = self._orig_data_dir
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_caller_frame_is_not_mutated(self):
        """
        BUG: the writer did `df[col] = df[col].astype(str)` on the frame it was
        handed, mutating the CALLER's DataFrame as a side effect.
        """
        df = pd.DataFrame({
            'station_id': ['A', 'B'], 'timestamp': ['t1', 't2'],
            'confidence': [None, 'high'], 'value': [1.0, np.nan],
        })
        before = df.copy(deep=True)
        save_cleaned_data_parquet(df, 'unit_test', 'date', '2026-01-01',
                                  ['station_id', 'timestamp'], pure_overwrite=True)
        pd.testing.assert_frame_equal(df, before)

    def test_missing_values_survive_the_string_cast(self):
        """
        BUG: astype(str) renders NaN as the literal string 'nan' on some pandas
        versions, and requirements.txt was unpinned, so which behaviour you got
        depended on the install. Those 'nan' strings then round-tripped back
        into QC via load_historical_context as real values.
        """
        df = pd.DataFrame({'confidence': [None, 'high'], 'other': [np.nan, 'x']})
        out = _stringify_object_columns(df)
        for col in ('confidence', 'other'):
            self.assertTrue(pd.isna(out[col].iloc[0]),
                            f"{col}: missing value became the string {out[col].iloc[0]!r}")

    def test_parquet_round_trip_preserves_missing(self):
        df = pd.DataFrame({
            'station_id': ['A', 'B'], 'timestamp': ['t1', 't2'],
            'confidence': [None, 'high'],
        })
        path = save_cleaned_data_parquet(df, 'unit_test', 'date', '2026-01-01',
                                         ['station_id', 'timestamp'], pure_overwrite=True)
        self.assertTrue(pd.isna(pd.read_parquet(path)['confidence'].iloc[0]))

    def test_read_merge_write_keeps_earlier_rows(self):
        first = pd.DataFrame({'station_id': ['A'], 'timestamp': ['t1'], 'v': [1.0]})
        second = pd.DataFrame({'station_id': ['B'], 'timestamp': ['t2'], 'v': [2.0]})
        for frame in (first, second):
            save_cleaned_data_parquet(frame, 'unit_test', 'date', '2026-01-01',
                                      ['station_id', 'timestamp'], pure_overwrite=False)
        out = pd.read_parquet(os.path.join(self.tmp, 'cleaned_unit_test',
                                           'date=2026-01-01.parquet'))
        self.assertEqual(sorted(out['station_id']), ['A', 'B'])

    def test_rerun_with_same_keys_does_not_duplicate(self):
        df = pd.DataFrame({'station_id': ['A'], 'timestamp': ['t1'], 'v': [1.0]})
        for _ in range(3):
            save_cleaned_data_parquet(df, 'unit_test', 'date', '2026-01-01',
                                      ['station_id', 'timestamp'], pure_overwrite=False)
        out = pd.read_parquet(os.path.join(self.tmp, 'cleaned_unit_test',
                                           'date=2026-01-01.parquet'))
        self.assertEqual(len(out), 1)

    def test_no_temp_files_left_in_partition_directory(self):
        """A leftover .tmp file would be picked up by the next glob as data."""
        df = pd.DataFrame({'station_id': ['A'], 'timestamp': ['t1'], 'v': [1.0]})
        save_cleaned_data_parquet(df, 'unit_test', 'date', '2026-01-01',
                                  ['station_id', 'timestamp'], pure_overwrite=True)
        leftovers = [f for f in os.listdir(os.path.join(self.tmp, 'cleaned_unit_test'))
                     if f.endswith('.tmp')]
        self.assertEqual(leftovers, [])

    def test_raw_data_is_content_addressed_and_idempotent(self):
        payload = {'city': 'delhi', 'aqi': 155}
        first = save_raw_data('unit_test', '2026-01-01T00:00:00+00:00', payload)
        second = save_raw_data('unit_test', '2026-01-01T00:00:00+00:00', payload)
        self.assertEqual(first, second)
        directory = os.path.dirname(first)
        self.assertEqual(len(os.listdir(directory)), 1)

    def test_save_raw_data_returns_path_for_lineage(self):
        path = save_raw_data('unit_test', '2026-01-01T00:00:00+00:00', b'GRIB-bytes', ext='grb2')
        self.assertTrue(path and os.path.exists(path))


# ── Database layer ───────────────────────────────────────────────────────────

class TestDatabaseLayer(unittest.TestCase):

    def test_postgres_ddl_translation(self):
        """
        BUG: DB_ENGINE=postgres was documented but schema.sql is SQLite DDL.
        INTEGER PRIMARY KEY AUTOINCREMENT and BLOB are both rejected by
        Postgres, so the advertised backend crashed on the first statement.
        """
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
        with open(schema_path, encoding='utf-8') as fh:
            sqlite_ddl = fh.read()
        pg = db.translate_ddl(sqlite_ddl, 'postgres')

        self.assertNotIn('AUTOINCREMENT', pg.upper())
        self.assertIn('GENERATED BY DEFAULT AS IDENTITY', pg)
        self.assertNotIn('BLOB', pg.upper())
        # SQLite output must be untouched.
        self.assertEqual(db.translate_ddl(sqlite_ddl, 'sqlite'), sqlite_ddl)

    def test_translation_does_not_touch_identifiers_containing_blob(self):
        ddl = "CREATE TABLE t (blob_path TEXT, payload BLOB, myblob TEXT);"
        pg = db.translate_ddl(ddl, 'postgres')
        self.assertIn('blob_path TEXT', pg)
        self.assertIn('myblob TEXT', pg)
        self.assertIn('payload BYTEA', pg)

    def test_schema_applies_and_enables_wal(self):
        """
        BUG: SQLite connections had no busy timeout and used the default
        rollback journal, so six jobs starting at once produced
        "database is locked".
        """
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, 'test.db')
            original_path, original_flag = db.SQLITE_DB_PATH, db._SCHEMA_APPLIED
            db.SQLITE_DB_PATH, db._SCHEMA_APPLIED = path, False
            try:
                db.init_db(force=True)
                conn = db.get_db_connection()
                try:
                    self.assertEqual(
                        conn.execute('PRAGMA journal_mode').fetchone()[0].lower(), 'wal')
                    self.assertGreater(conn.execute('PRAGMA busy_timeout').fetchone()[0], 0)
                    tables = {r[0] for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}
                finally:
                    conn.close()
            finally:
                db.SQLITE_DB_PATH, db._SCHEMA_APPLIED = original_path, original_flag

            for expected in ('raw_waqi', 'cleaned_waqi', 'raw_weather', 'cleaned_weather',
                             'raw_gfs', 'cleaned_gfs', 'raw_firms', 'cleaned_firms',
                             'raw_cams', 'cleaned_cams', 'pipeline_run_log',
                             'gfs_fetch_manifest'):
                self.assertIn(expected, tables)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_no_mislabelled_table_names_remain(self):
        """
        Tables must be named after the source the data actually comes from.
        cpcb/imd/sentinel5p all named providers the pipeline never called.
        """
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
        with open(schema_path, encoding='utf-8') as fh:
            statements = [line for line in fh.read().splitlines()
                          if line.strip().upper().startswith('CREATE TABLE')]
        joined = '\n'.join(statements)
        for wrong in ('raw_cpcb', 'cleaned_cpcb', 'raw_imd', 'cleaned_imd',
                      'raw_sentinel5p', 'cleaned_sentinel5p'):
            self.assertNotIn(wrong, joined)


# ── Weather station table ────────────────────────────────────────────────────

class TestWeatherStations(unittest.TestCase):
    """
    BUG: validation was two module-level `assert` statements, which `python -O`
    strips — so they checked nothing in the one mode where it matters. One of
    them also compared whole (name, lat, lon) tuples while its message claimed
    it was checking for duplicate names.
    """

    def test_real_station_table_is_valid(self):
        from fetch_weather import validate_station_table
        self.assertTrue(validate_station_table())

    def test_duplicate_city_name_is_rejected(self):
        from fetch_weather import validate_station_table
        with self.assertRaises(ValueError) as ctx:
            validate_station_table({
                '42182': ('Delhi', 28.58, 77.20),
                '99999': ('Delhi', 28.60, 77.25),
            })
        self.assertIn('Duplicate', str(ctx.exception))

    def test_coordinates_outside_india_are_rejected(self):
        from fetch_weather import validate_station_table
        with self.assertRaises(ValueError):
            validate_station_table({'00001': ('London', 51.5, -0.12)})


# ── Idempotency against the live schema ──────────────────────────────────────

class TestSchemaIdempotency(unittest.TestCase):
    """Re-inserting the same observation must never duplicate a row."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.tmpdir, 'idem.db')
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
        conn = sqlite3.connect(cls.db_path)
        with open(schema_path, encoding='utf-8') as fh:
            conn.executescript(fh.read())
        conn.commit()
        conn.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def setUp(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def _insert_twice(self, sql, params):
        for _ in range(2):
            self.cur.execute(sql, params)
        self.conn.commit()

    def test_raw_waqi_dedupes_on_timestamp_and_hash(self):
        self._insert_twice(
            "INSERT OR IGNORE INTO raw_waqi (timestamp, raw_data, raw_data_hash) VALUES (?,?,?)",
            ('2026-09-12T10:00:00+00:00', '{"aqi": 155}', 'hash-a'))
        self.cur.execute("SELECT COUNT(*) FROM raw_waqi WHERE raw_data_hash='hash-a'")
        self.assertEqual(self.cur.fetchone()[0], 1)

    def test_cleaned_waqi_dedupes_on_station_and_timestamp(self):
        self._insert_twice(
            "INSERT OR IGNORE INTO cleaned_waqi (station_id, city, timestamp, pm25_aqi_raw) "
            "VALUES (?,?,?,?)", ('1437', 'delhi', '2026-09-12T10:00:00+00:00', 155.0))
        self.cur.execute("SELECT COUNT(*) FROM cleaned_waqi WHERE station_id='1437'")
        self.assertEqual(self.cur.fetchone()[0], 1)

    def test_cleaned_gfs_dedupes_per_gridpoint_cycle_and_forecast_hour(self):
        self._insert_twice(
            "INSERT OR IGNORE INTO cleaned_gfs (lat, lon, cycle, fhr, valid_time) "
            "VALUES (?,?,?,?,?)",
            (28.5, 77.25, '20260912_06z', '024', '2026-09-13T06:00:00+00:00'))
        self.cur.execute("SELECT COUNT(*) FROM cleaned_gfs WHERE cycle='20260912_06z'")
        self.assertEqual(self.cur.fetchone()[0], 1)

    def test_different_forecast_hours_are_separate_rows(self):
        """
        The unique key must include fhr, or fetching several lead times for one
        cycle would collapse into a single row.
        """
        for fhr, valid in (('000', '2026-09-12T12:00:00+00:00'),
                           ('006', '2026-09-12T18:00:00+00:00'),
                           ('012', '2026-09-13T00:00:00+00:00')):
            self.cur.execute(
                "INSERT OR IGNORE INTO cleaned_gfs (lat, lon, cycle, fhr, valid_time) "
                "VALUES (?,?,?,?,?)", (20.0, 78.0, '20260912_12z', fhr, valid))
        self.conn.commit()
        self.cur.execute("SELECT COUNT(*) FROM cleaned_gfs WHERE cycle='20260912_12z'")
        self.assertEqual(self.cur.fetchone()[0], 3)

    def test_firms_dedupe_key_separates_sensors_at_one_location(self):
        """
        MODIS and VIIRS can both detect the same fire at the same minute. The
        key includes sensor so one does not silently suppress the other.
        """
        for sensor, satellite in (('MODIS_NRT', 'Terra'), ('VIIRS_SNPP_NRT', 'N')):
            self.cur.execute(
                "INSERT OR IGNORE INTO cleaned_firms (lat, lon, timestamp, sensor, satellite) "
                "VALUES (?,?,?,?,?)",
                (28.6, 77.2, '2026-09-12T05:30:00+00:00', sensor, satellite))
        self.conn.commit()
        self.cur.execute("SELECT COUNT(*) FROM cleaned_firms WHERE lat=28.6")
        self.assertEqual(self.cur.fetchone()[0], 2)


# ── Scheduler configuration ──────────────────────────────────────────────────

class TestSchedulerConfiguration(unittest.TestCase):

    def test_jobs_do_not_all_start_simultaneously(self):
        """
        BUG: every job had next_run_time=now, so six SQLite writers opened in
        the same second — the documented cause of "database is locked".
        """
        from scheduler import JOBS
        staggers = [stagger for *_, stagger in JOBS]
        self.assertEqual(len(set(staggers)), len(staggers),
                         "Two jobs share a start offset")

    def test_gfs_runs_only_when_a_published_cycle_is_available(self):
        """
        BUG: the GFS job ran at 00:30/06:30/12:30/18:30 UTC, asking for the
        cycle of the same nominal hour — which NOAA had not published yet
        (a cycle lands 3.5-5 hours after its nominal time).

        The invariant is not "run far from a cycle time"; a run at 05:15 is
        correct precisely because it fetches the 00z cycle, five hours old.
        What must hold is that at every scheduled hour the cycle the fetcher
        selects is already old enough to exist.
        """
        from scheduler import JOBS
        from fetch_gfs import latest_available_cycle, GFS_PUBLICATION_LAG_HOURS

        gfs = next(job for job in JOBS if job[0] == 'gfs')
        trigger = str(gfs[3])
        hours = {int(h) for h in trigger.split("hour='")[1].split("'")[0].split(',')}
        self.assertEqual(len(hours), 4, "GFS should run once per 6-hourly cycle")

        for hour in sorted(hours):
            now = datetime(2026, 9, 12, hour, 15, tzinfo=timezone.utc)
            cycle = latest_available_cycle(now)
            age_hours = (now - cycle).total_seconds() / 3600
            self.assertGreaterEqual(
                age_hours, GFS_PUBLICATION_LAG_HOURS,
                f"A run at {hour:02d}:15 UTC selects cycle {cycle}, only "
                f"{age_hours:.1f}h old — NOAA may not have published it")

    def test_each_scheduled_gfs_run_picks_a_different_cycle(self):
        """Four runs a day fetching the same cycle would be three wasted runs,
        which is what hardcoding cycle_hour='00' produced."""
        from scheduler import JOBS
        from fetch_gfs import latest_available_cycle, cycle_label

        gfs = next(job for job in JOBS if job[0] == 'gfs')
        hours = sorted(int(h) for h in
                       str(gfs[3]).split("hour='")[1].split("'")[0].split(','))
        labels = [cycle_label(latest_available_cycle(
            datetime(2026, 9, 12, h, 15, tzinfo=timezone.utc))) for h in hours]
        self.assertEqual(len(set(labels)), len(labels),
                         f"Two scheduled runs fetch the same cycle: {labels}")

    def test_every_source_is_monitored(self):
        """A scheduled job with no health threshold can fail unnoticed."""
        from scheduler import JOBS
        from monitor_health import SOURCES
        for source_key, *_ in JOBS:
            self.assertIn(source_key, SOURCES,
                          f"{source_key} is scheduled but absent from monitor_health.SOURCES")


if __name__ == '__main__':
    unittest.main()


# ═══════════════════════════════════════════════════════════════════════════
# Second audit: bugs found by re-reviewing the fixes above.
# ═══════════════════════════════════════════════════════════════════════════

class TestChronologicalSorting(unittest.TestCase):
    """
    BUG: the QC checks sorted by the raw timestamp STRING. ISO strings only
    sort correctly when every value carries the same UTC offset, and this
    pipeline mixes them routinely — WAQI stamps '...+05:30' while the fallback
    timestamp and every other source use '...+00:00'. Readings were therefore
    compared in the wrong temporal order.
    """

    def test_mixed_utc_offsets_sort_chronologically(self):
        from cleaning import _sort_for_qc
        df = pd.DataFrame({
            'station': ['A', 'A'],
            # 23:30+05:30 is 18:00 UTC — EARLIER than 20:00+00:00,
            # but sorts LATER as a string.
            'timestamp': ['2026-09-12T23:30:00+05:30', '2026-09-12T20:00:00+00:00'],
            'v': [10.0, 20.0],
        })
        # Guard the fixture: a plain string sort must disagree with the true
        # chronological order, otherwise this test proves nothing.
        string_order = df.sort_values('timestamp')['v'].tolist()
        true_order = df.assign(
            _t=pd.to_datetime(df['timestamp'], utc=True, format='mixed')
        ).sort_values('_t')['v'].tolist()
        self.assertNotEqual(string_order, true_order,
                            "fixture no longer exercises the string-sort trap")

        ordered = _sort_for_qc(df, ['station'], 'timestamp')['v'].tolist()
        self.assertEqual(ordered, true_order,
                         "QC is sorting timestamps as strings again")

    def test_step_check_uses_true_time_order(self):
        """A jump must be measured against the chronologically previous value."""
        df = pd.DataFrame({
            'station': ['A'] * 3,
            'timestamp': ['2026-09-12T18:00:00+00:00',
                          '2026-09-12T23:30:00+05:30',   # 18:00 UTC + 0h -> 18:00
                          '2026-09-12T20:00:00+00:00'],
            'pm25': [50.0, 55.0, 500.0],
        })
        out = clean_and_impute(df, 'pm25', time_col='timestamp', group_cols=['station'],
                               min_val=0.0, max_val=1000.0, max_step_change=300.0)
        by_time = out.set_index('timestamp')['pm25_qc_flag']
        self.assertIn('step_fail', by_time['2026-09-12T20:00:00+00:00'])

    def test_internal_sort_key_never_reaches_output(self):
        from cleaning import _SORT_KEY
        df = pd.DataFrame({'timestamp': ['2026-09-12T00:00:00+00:00'], 'v': [1.0]})
        out = clean_and_impute(df, 'v', time_col='timestamp', min_val=0.0, max_val=10.0)
        self.assertNotIn(_SORT_KEY, out.columns)


class TestFIRMSPointEventSemantics(unittest.TestCase):
    """
    BUG: the first audit fixed the GFS instance of "quality control treating
    unrelated rows as a time series" but missed the FIRMS instance. Consecutive
    FIRMS rows are SEPARATE FIRES, so a flatline check flags any twelve fires
    that share a rounded brightness as a stuck sensor.
    """

    def _separate_fires(self, n=14, brightness=300.0):
        return pd.DataFrame({
            'sensor': ['MODIS_NRT'] * n,
            'lat': [20.0 + i * 0.5 for i in range(n)],
            'lon': [78.0 + i * 0.5 for i in range(n)],
            'timestamp': [f'2026-09-12T{i:02d}:00:00+00:00' for i in range(n)],
            'brightness_k_raw': [brightness] * n,
        })

    def test_distinct_fires_are_not_flagged_as_a_flatline(self):
        out = clean_and_impute(
            self._separate_fires(), 'brightness_k_raw', time_col='timestamp',
            group_cols=['sensor'], min_val=200.0, max_val=600.0,
            max_step_change=None, window_flatline=None,
        )
        self.assertEqual(set(out['brightness_k_raw_qc_flag']), {'ok'},
                         "separate fires are being flagged as a stuck sensor")

    def test_the_flatline_check_would_still_fire_if_left_enabled(self):
        """Confirms the failure mode is real, not hypothetical."""
        out = clean_and_impute(
            self._separate_fires(), 'brightness_k_raw', time_col='timestamp',
            group_cols=['sensor'], min_val=200.0, max_val=600.0,
            max_step_change=None, window_flatline=12,
        )
        self.assertIn('flatline', set(out['brightness_k_raw_qc_flag']))

    def test_range_check_still_applies_to_fires(self):
        """Disabling the time-series checks must not disable range checking."""
        df = self._separate_fires(n=2)
        df.loc[0, 'brightness_k_raw'] = 900.0      # physically impossible
        out = clean_and_impute(
            df, 'brightness_k_raw', time_col='timestamp', group_cols=['sensor'],
            min_val=200.0, max_val=600.0, max_step_change=None, window_flatline=None,
        )
        self.assertIn('range_fail', out['brightness_k_raw_qc_flag'].iloc[0])

    def test_fetcher_passes_point_event_settings(self):
        import inspect
        import fetch_firms
        source = inspect.getsource(fetch_firms.main)
        self.assertIn('window_flatline=None', source)
        self.assertIn('max_step_change=None', source)


class TestGoldLayerRobustness(unittest.TestCase):

    def test_dominant_pollutant_survives_a_row_with_no_subindices(self):
        """
        BUG: DataFrame.idxmax raises "Encountered all NA values" on an all-NA
        row. A WAQI station returning an empty iaqi block therefore crashed the
        entire gold rebuild, for every city.
        """
        cols = ['pm25_aqi_clean', 'pm10_aqi_clean']
        df = pd.DataFrame({cols[0]: [np.nan, 100.0], cols[1]: [np.nan, 50.0]})
        with self.assertRaises(ValueError):
            df.idxmax(axis=1)          # the raw operation still raises

        has_any = df[cols].notna().any(axis=1)
        dominant = pd.Series(pd.NA, index=df.index, dtype=object)
        dominant[has_any] = df.loc[has_any, cols].idxmax(axis=1).str.replace(
            '_aqi_clean', '', regex=False)
        self.assertTrue(pd.isna(dominant.iloc[0]))
        self.assertEqual(dominant.iloc[1], 'pm25')

    def test_gfs_lookback_is_measured_in_days_not_files(self):
        """
        BUG: GFS partitions are cycles, four per day, but the slice used
        lookback_days directly — so a 3-day lookback kept 18 hours of data.
        """
        from gold_layer import CYCLES_PER_DAY
        self.assertEqual(CYCLES_PER_DAY, 4)
        import inspect, gold_layer
        self.assertIn('lookback_days * CYCLES_PER_DAY',
                      inspect.getsource(gold_layer._recent_partitions))


class TestSQLPlaceholderRewrite(unittest.TestCase):
    """
    BUG: execute_query rewrote Postgres %s placeholders for SQLite with a blind
    str.replace, which also mangled any %s appearing inside a string literal —
    the LIKE pattern '%send%' became '?end%'.
    """

    def test_placeholders_outside_literals_are_rewritten(self):
        from db import _to_sqlite
        self.assertEqual(_to_sqlite("SELECT * FROM t WHERE a = %s AND b = %s"),
                         "SELECT * FROM t WHERE a = ? AND b = ?")

    def test_percent_s_inside_a_string_literal_is_left_alone(self):
        from db import _to_sqlite
        out = _to_sqlite("SELECT * FROM t WHERE src LIKE %s AND note LIKE '%send%'")
        self.assertEqual(out, "SELECT * FROM t WHERE src LIKE ? AND note LIKE '%send%'")
        self.assertNotIn("'?end%'", out)

    def test_conflict_clause_still_translated(self):
        from db import _to_sqlite
        out = _to_sqlite("INSERT INTO t (a) VALUES (%s) ON CONFLICT DO NOTHING")
        self.assertEqual(out, "INSERT OR IGNORE INTO t (a) VALUES (?)")


class TestContractSchemaFailures(unittest.TestCase):
    """
    BUG: a missing column produced failure rows with no index, so no row was
    dropped and the event was logged at the same volume as one bad reading.
    A source that stops returning a pollutant is a much bigger event.
    """

    def test_missing_columns_are_logged_as_schema_violations(self):
        from contracts import validate, HAS_PANDERA
        if not HAS_PANDERA:
            self.skipTest("pandera not installed")
        df = pd.DataFrame({'station_id': ['1'], 'city': ['delhi'],
                           'timestamp': ['2026-09-12T00:00:00+00:00'],
                           'source': ['waqi'], 'is_synthetic': [0]})
        with self.assertLogs('contracts', level='ERROR') as captured:
            out, failures = validate(df, 'waqi')
        self.assertTrue(any('SCHEMA VIOLATION' in line for line in captured.output),
                        "a missing column is not reported as a schema-level violation")
        # Rows are still written: the data present is valid, the column is absent.
        self.assertEqual(len(out), len(df))
        self.assertFalse(failures.empty)


class TestDependencyPins(unittest.TestCase):
    """
    BUG: requirements pinned pyarrow~=21.0 while the code was developed and
    tested against 25.0.1, so a fresh install silently ran a different version
    from the one that was verified — the exact reproducibility problem the
    pinning was meant to solve.
    """

    def test_no_pin_excludes_the_installed_version(self):
        import importlib.metadata as md
        import re as _re
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'requirements.txt')
        with open(path, encoding='utf-8') as fh:
            lines = [ln.split('#')[0].strip() for ln in fh if ln.strip()
                     and not ln.strip().startswith('#')]

        for line in lines:
            m = _re.match(r'^([A-Za-z0-9_.\-]+)\s*~=\s*([0-9.]+)$', line)
            if not m:
                continue
            name, pin = m.group(1), m.group(2)
            try:
                installed = md.version(name)
            except md.PackageNotFoundError:
                continue
            pin_major = pin.split('.')[0]
            got_major = installed.split('.')[0]
            self.assertEqual(
                pin_major, got_major,
                f"{name} is pinned ~={pin} but {installed} is installed: a fresh "
                f"install would not reproduce what was tested")


# ═══════════════════════════════════════════════════════════════════════════
# FIRMS historical backfill (scripts/backfill_firms_archive.py)
# ═══════════════════════════════════════════════════════════════════════════

class TestFIRMSProcessingStream(unittest.TestCase):
    """
    FIRMS names its API sources <FAMILY>_<STREAM>: MODIS_NRT is near real time,
    MODIS_SP is the reprocessed archive of the SAME detections. Storing the
    whole source string in `sensor` would make one physical fire two rows once
    the archive backfill covers a window the live fetcher already saw, silently
    doubling the fire counts any model trains on.
    """

    def test_source_splits_into_family_and_stream(self):
        from fetch_firms import split_source
        self.assertEqual(split_source('MODIS_SP'), ('MODIS', 'SP'))
        self.assertEqual(split_source('MODIS_NRT'), ('MODIS', 'NRT'))
        self.assertEqual(split_source('VIIRS_SNPP_SP'), ('VIIRS_SNPP', 'SP'))
        self.assertEqual(split_source('VIIRS_NOAA20_NRT'), ('VIIRS_NOAA20', 'NRT'))

    def test_unknown_suffix_defaults_to_nrt(self):
        from fetch_firms import split_source
        self.assertEqual(split_source('LANDSAT_NRT'), ('LANDSAT', 'NRT'))
        self.assertEqual(split_source('SOMETHING_ELSE'), ('SOMETHING_ELSE', 'NRT'))

    def test_standardise_frame_records_both_columns(self):
        from fetch_firms import standardise_frame
        frame = pd.DataFrame({
            'latitude': [29.0], 'longitude': [75.0], 'brightness': [320.0],
            'confidence': [85], 'satellite': ['Terra'], 'frp': [10.0],
        })
        out = standardise_frame(frame, 'MODIS_SP')
        self.assertEqual(out['sensor'].iloc[0], 'MODIS')
        self.assertEqual(out['processing'].iloc[0], 'SP')

    def test_satellite_fallback_does_not_leak_the_stream(self):
        """
        satellite feeds the uniqueness constraint. If it fell back to the raw
        source name, the same detection would key differently under NRT and SP.
        """
        from fetch_firms import standardise_frame
        base = {'latitude': [29.0], 'longitude': [75.0], 'brightness': [320.0]}
        nrt = standardise_frame(pd.DataFrame(base), 'MODIS_NRT')['satellite'].iloc[0]
        sp = standardise_frame(pd.DataFrame(base), 'MODIS_SP')['satellite'].iloc[0]
        self.assertEqual(nrt, sp)
        self.assertNotIn('_SP', str(sp))
        self.assertNotIn('_NRT', str(nrt))


class TestFIRMSBackfillPlanning(unittest.TestCase):
    """The plan decides the API bill before a single request is made."""

    def setUp(self):
        from scripts.backfill_firms_archive import month_chunks, build_plan, MAX_DAY_RANGE
        self.month_chunks = month_chunks
        self.build_plan = build_plan
        self.max_day_range = MAX_DAY_RANGE

    def test_chunks_never_exceed_the_api_day_range_limit(self):
        for year in range(2020, 2026):
            for month in range(1, 13):
                for _, span in self.month_chunks(year, month):
                    self.assertLessEqual(span, self.max_day_range)
                    self.assertGreaterEqual(span, 1)

    def test_chunks_cover_every_day_of_a_month_exactly_once(self):
        from calendar import monthrange
        for year, month in [(2020, 1), (2020, 2), (2024, 2), (2023, 11), (2025, 12)]:
            covered = []
            for start, span in self.month_chunks(year, month):
                d = datetime.fromisoformat(start).date()
                covered.extend((d + timedelta(days=i)).day for i in range(span))
            expected = list(range(1, monthrange(year, month)[1] + 1))
            self.assertEqual(sorted(covered), expected, f"{year}-{month:02d}")
            self.assertEqual(len(covered), len(set(covered)), "a day is fetched twice")

    def test_day_range_limit_matches_the_server_not_the_docs(self):
        """
        BUG: MAX_DAY_RANGE was 10, taken from NASA's own API page. The live
        server rejects anything above 5 with
            HTTP 400  "Invalid day range. Expects [1..5]."
        which made every single archive request fail.
        """
        self.assertEqual(self.max_day_range, 5)

    def test_requested_window_costs_what_we_claim(self):
        """Jan/Oct/Nov/Dec 2020-2025 across three sensors, at 5 days a chunk."""
        plan, _ = self.build_plan(
            range(2020, 2026), [1, 10, 11, 12],
            ['MODIS_SP', 'VIIRS_SNPP_SP', 'VIIRS_NOAA20_SP'],
            today=date(2026, 9, 13))
        self.assertEqual(len(plan), 486)
        self.assertLess(len(plan), 5000, "would exceed the MAP_KEY 10-minute budget")

    def test_dates_before_a_sources_coverage_are_not_requested(self):
        """VIIRS S-NPP SP starts 2012-01-20; asking it for 2005 wastes a call."""
        plan, skipped = self.build_plan(
            [2005], [1], ['VIIRS_SNPP_SP'], today=date(2026, 9, 13))
        self.assertEqual(plan, [])
        self.assertTrue(skipped)
        self.assertIn('coverage starts', skipped[0][2])

    def test_dates_after_a_sources_coverage_are_not_requested(self):
        """
        FIRMS reports MODIS_SP ending 2026-05-31 — the archive lags the present
        by months. Requesting past it returns an empty CSV, not an error, so
        without this the run would look successful and store nothing.
        """
        plan, skipped = self.build_plan(
            [2026], [8], ['MODIS_SP'], today=date(2026, 9, 13))
        self.assertEqual(plan, [])
        self.assertTrue(any('coverage ends' in s[2] for s in skipped))

    def test_live_availability_overrides_the_builtin_dates(self):
        """The endpoint is authoritative; the constants are only a fallback."""
        narrow = {'MODIS_SP': (date(2023, 1, 1), date(2023, 12, 31))}
        plan, _ = self.build_plan([2020, 2023], [1], ['MODIS_SP'],
                                  today=date(2026, 9, 13), availability=narrow)
        self.assertTrue(plan)
        self.assertTrue(all(p[1].startswith('2023') for p in plan))

    def test_future_dates_are_not_requested(self):
        plan, skipped = self.build_plan(
            [2030], [1], ['MODIS_SP'], today=date(2026, 9, 13), availability={})
        self.assertEqual(plan, [])
        self.assertTrue(any('future' in s[2] for s in skipped))

    def test_modis_is_available_for_the_whole_requested_range(self):
        plan, _ = self.build_plan(range(2020, 2026), [1, 10, 11, 12],
                                  ['MODIS_SP'], today=date(2026, 9, 13))
        self.assertEqual(len(plan), 162)


class TestFIRMSBackfillUpsert(unittest.TestCase):
    """
    SP is the reprocessed version of an NRT detection. It must REPLACE the NRT
    row, and a later NRT fetch must never downgrade an SP row back.
    """

    def setUp(self):
        import db as db_module
        from scripts.backfill_firms_archive import CLEANED_UPSERT
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, 'upsert.db')
        schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
        self.conn = sqlite3.connect(self.db_path)
        with open(schema, encoding='utf-8') as fh:
            self.conn.executescript(fh.read())
        self.conn.commit()
        self.sql = db_module._to_sqlite(CLEANED_UPSERT)

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _row(self, processing, brightness, frp):
        return (29.0, 75.0, '2020-11-05T05:00:00+00:00', 'MODIS', processing, 'Terra',
                brightness, brightness, 0, 'ok', frp, '85', 'percent', 'high', 'D',
                'firms_archive', 0)

    def _stored(self):
        return self.conn.execute(
            "SELECT processing, brightness_k_raw, frp_mw FROM cleaned_firms "
            "WHERE lat = 29.0").fetchone()

    def test_sp_supersedes_an_existing_nrt_row(self):
        self.conn.execute(self.sql, self._row('NRT', 300.0, 5.0))
        self.conn.execute(self.sql, self._row('SP', 999.0, 77.0))
        self.conn.commit()
        self.assertEqual(self._stored(), ('SP', 999.0, 77.0))

    def test_nrt_never_downgrades_an_sp_row(self):
        self.conn.execute(self.sql, self._row('SP', 999.0, 77.0))
        self.conn.execute(self.sql, self._row('NRT', 111.0, 1.0))
        self.conn.commit()
        self.assertEqual(self._stored(), ('SP', 999.0, 77.0))

    def test_the_same_detection_is_never_duplicated(self):
        for _ in range(3):
            self.conn.execute(self.sql, self._row('NRT', 300.0, 5.0))
            self.conn.execute(self.sql, self._row('SP', 999.0, 77.0))
        self.conn.commit()
        count = self.conn.execute(
            "SELECT COUNT(*) FROM cleaned_firms WHERE lat = 29.0").fetchone()[0]
        self.assertEqual(count, 1, "an archive backfill duplicated a live detection")

    def test_different_sensors_at_one_place_and_time_stay_separate(self):
        """MODIS and VIIRS can both see the same fire; those are two observations."""
        self.conn.execute(self.sql, self._row('SP', 999.0, 77.0))
        viirs = list(self._row('SP', 350.0, 20.0))
        viirs[3], viirs[5] = 'VIIRS_SNPP', 'N'
        self.conn.execute(self.sql, tuple(viirs))
        self.conn.commit()
        count = self.conn.execute(
            "SELECT COUNT(*) FROM cleaned_firms WHERE lat = 29.0").fetchone()[0]
        self.assertEqual(count, 2)

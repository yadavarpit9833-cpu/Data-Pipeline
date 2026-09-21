import unittest
import pandas as pd
import numpy as np
from fetch_gfs import grib_signed
from cleaning import (
    check_range,
    check_step,
    check_flatline,
    detect_outliers_log_mad,
    clean_and_impute
)

class TestQCChain(unittest.TestCase):

    def test_range_check(self):
        df = pd.DataFrame({'pm25': [10.0, 450.0, -5.0, 1200.0, np.nan]})
        res = check_range(df, 'pm25', min_val=0.0, max_val=1000.0)
        expected = [False, False, True, True, False]
        self.assertEqual(res.tolist(), expected)

    def test_step_check_plain(self):
        df = pd.DataFrame({
            'station': ['A', 'A', 'A', 'A'],
            'timestamp': ['2026-01-01T00:00', '2026-01-01T01:00', '2026-01-01T02:00', '2026-01-01T03:00'],
            'pm25': [50.0, 60.0, 400.0, 410.0]
        })
        res = check_step(df, 'pm25', group_cols=['station'], time_col='timestamp', max_step_change=300.0)
        # Jump from 60 to 400 is 340 > 300 -> step_fail
        expected = [False, False, True, False]
        self.assertEqual(res.tolist(), expected)

    def test_step_check_circular_wind_dir(self):
        df = pd.DataFrame({
            'station': ['A', 'A', 'A', 'A'],
            'timestamp': ['2026-01-01T00:00', '2026-01-01T01:00', '2026-01-01T02:00', '2026-01-01T03:00'],
            'wind_dir': [350.0, 10.0, 20.0, 200.0]
        })
        # 350 to 10 is circular diff 20 deg (<= 45) -> NOT step_fail
        # 20 to 200 is circular diff 180 deg (> 45) -> step_fail
        res = check_step(df, 'wind_dir', group_cols=['station'], time_col='timestamp', max_step_change=45.0, is_circular=True)
        expected = [False, False, False, True]
        self.assertEqual(res.tolist(), expected)

    def test_flatline_rainfall_zero_persistence(self):
        # 14 consecutive zero readings for rainfall -> should NOT flag flatline (ignore_zero=True)
        df_rain = pd.DataFrame({
            'station': ['A'] * 14,
            'rainfall': [0.0] * 14
        })
        res_rain = check_flatline(df_rain, 'rainfall', group_cols=['station'], window=12, ignore_zero=True)
        self.assertFalse(res_rain.any(), "Rainfall zero flatlines should not be flagged")

        # 14 consecutive non-zero readings for rainfall -> SHOULD flag flatline
        df_rain_nonzero = pd.DataFrame({
            'station': ['A'] * 14,
            'rainfall': [5.2] * 14
        })
        res_rain_nonzero = check_flatline(df_rain_nonzero, 'rainfall', group_cols=['station'], window=12, ignore_zero=True)
        self.assertTrue(res_rain_nonzero.all(), "Non-zero rainfall flatlines should be flagged")

        # 14 consecutive zero readings for non-rainfall column -> SHOULD flag flatline (ignore_zero=False)
        df_pm25 = pd.DataFrame({
            'station': ['A'] * 14,
            'pm25': [0.0] * 14
        })
        res_pm25 = check_flatline(df_pm25, 'pm25', group_cols=['station'], window=12, ignore_zero=False)
        self.assertTrue(res_pm25.all(), "Non-rainfall zero flatlines should be flagged")

    def test_multi_failure_qc_flag_comma_separated(self):
        df = pd.DataFrame({
            'station': ['A', 'A'],
            'timestamp': ['2026-01-01T00:00', '2026-01-01T01:00'],
            'pm25': [50.0, 1500.0]  # 1500 fails range (> 1000) and step (jump 1450 > 300)
        })
        df_clean = clean_and_impute(
            df, 'pm25', time_col='timestamp', group_cols=['station'],
            min_val=0.0, max_val=1000.0, max_step_change=300.0
        )
        self.assertEqual(df_clean['pm25_qc_flag'].iloc[0], 'ok')
        self.assertEqual(df_clean['pm25_qc_flag'].iloc[1], 'range_fail,step_fail')
        # Value must be preserved, not deleted or set to NaN
        self.assertEqual(df_clean['pm25_clean'].iloc[1], 1500.0)
        self.assertFalse(df_clean['pm25_imputed'].iloc[1])

    def test_pollution_spike_preservation(self):
        # Severe real pollution reading 450 ug/m3
        df = pd.DataFrame({'pm25': [50.0, 100.0, 450.0, 420.0]})
        df_clean = clean_and_impute(df, 'pm25', min_val=0.0, max_val=1000.0)
        self.assertEqual(df_clean['pm25_clean'].iloc[2], 450.0)
        self.assertEqual(df_clean['pm25_qc_flag'].iloc[2], 'ok')
        self.assertFalse(df_clean['pm25_imputed'].iloc[2])

    def test_log_mad_outlier(self):
        # Right skewed values with one massive outlier
        vals = [10, 12, 11, 13, 10, 12, 11, 14, 12, 10000]
        df = pd.DataFrame({'pm25': vals})
        res = detect_outliers_log_mad(df, 'pm25', threshold=3.5)
        self.assertTrue(res.iloc[-1])
        self.assertFalse(res.iloc[0])

    def test_nan_only_imputation(self):
        df = pd.DataFrame({
            'timestamp': ['2026-01-01T00:00', '2026-01-01T02:00', '2026-01-01T04:00'],
            'pm25': [10.0, np.nan, 30.0]
        })
        df_clean = clean_and_impute(df, 'pm25', time_col='timestamp', min_val=0.0, max_val=1000.0)
        self.assertEqual(df_clean['pm25_clean'].iloc[1], 20.0)
        self.assertTrue(df_clean['pm25_imputed'].iloc[1])
        self.assertFalse(df_clean['pm25_imputed'].iloc[0])
        self.assertFalse(df_clean['pm25_imputed'].iloc[2])

class TestGoldAqiScale(unittest.TestCase):
    """
    WAQI's iaqi values are AQI sub-indices; CAMS reports ug/m3. gold_layer must
    not push a sub-index through the concentration breakpoints a second time.
    """

    def test_subindex_source_is_not_reconverted(self):
        import gold_layer
        self.assertEqual(gold_layer._aqi_from(112.0, 60.0, 'cpcb'), 112.0)
        self.assertEqual(gold_layer.aqi_category(112.0), 'Moderate')

    def test_concentration_source_uses_breakpoints(self):
        import gold_layer
        aqi = gold_layer._aqi_from(112.0, 60.0, 'cams-openmeteo-archive')
        self.assertAlmostEqual(aqi, 273.6, places=1)
        self.assertEqual(gold_layer.aqi_category(aqi), 'Poor')

    def test_missing_source_defaults_to_subindex(self):
        import gold_layer
        # cleaned_cpcb rows predating the source column must not be inflated.
        self.assertEqual(gold_layer._aqi_from(112.0, 60.0, None), 112.0)

    def test_all_nan_yields_nan(self):
        import gold_layer
        self.assertTrue(np.isnan(gold_layer._aqi_from(np.nan, np.nan, 'cpcb')))


class TestGribScaleFactors(unittest.TestCase):
    """
    GRIB2 signed integers are sign-magnitude, not two's complement. Reading them
    the wrong way silently zeroed every APCP value in the GFS grid.
    """

    def test_positive_scale_reads_the_same_either_way(self):
        # Temperature and wind carry positive scale factors, where both readings
        # agree - which is why the bug never surfaced in those fields.
        for value in (0, 1, 2, 300):
            raw = bytes([value >> 8, value & 0xFF])
            self.assertEqual(grib_signed(raw), value)
            self.assertEqual(grib_signed(raw), int.from_bytes(raw, 'big', signed=True))

    def test_negative_scale_is_sign_magnitude(self):
        # 0x8004 is -4 in sign-magnitude; two's complement would read -32764.
        self.assertEqual(grib_signed(bytes([0x80, 0x04])), -4)
        self.assertEqual(grib_signed(bytes([0x80, 0x01])), -1)

    def test_apcp_scale_does_not_underflow(self):
        # The actual failure: 2 ** -32764 is 0.0, so every packed APCP value
        # collapsed onto the reference value of 0.0 regardless of its bits.
        apcp_binary_scale = bytes([0x80, 0x04])
        good = grib_signed(apcp_binary_scale)
        bad = int.from_bytes(apcp_binary_scale, 'big', signed=True)
        self.assertEqual(2.0 ** bad, 0.0)
        self.assertAlmostEqual(0.0 + 655 * 2.0 ** good, 40.9375)


class TestApcpIncrements(unittest.TestCase):
    """
    GFS ships APCP as a bucket that resets every 6 hours, so a 3-hourly series has
    to be differenced out of alternating 3-hour and 6-hour windows. Getting this
    wrong produces a rainfall series that looks entirely plausible.
    """

    def _frame(self, fhrs, points=2):
        import pandas as pd
        return pd.DataFrame([{'fhr': f, 'lat': 28.0 + i, 'lon': 77.0}
                             for f in fhrs for i in range(points)])

    def test_three_hour_bucket_passes_through(self):
        from fetch_gfs_forecast import to_3h_increments
        df, _ = to_3h_increments(self._frame([3]), {3: {'values': [1.0, 2.0], 'span': 3}})
        self.assertEqual(df['precipitation_mm_3h'].tolist(), [1.0, 2.0])

    def test_six_hour_bucket_is_differenced(self):
        from fetch_gfs_forecast import to_3h_increments
        buckets = {3: {'values': [1.0, 2.0], 'span': 3},
                   6: {'values': [4.0, 2.5], 'span': 6}}
        df, notes = to_3h_increments(self._frame([3, 6]), buckets)
        # f006 carries 0-6h; the 3-6h increment is that minus the 0-3h bucket.
        self.assertEqual(df['precipitation_mm_3h'].tolist(), [1.0, 2.0, 3.0, 0.5])
        self.assertTrue(any('minus' in n for n in notes))

    def test_analysis_hour_stays_null(self):
        from fetch_gfs_forecast import to_3h_increments
        df, _ = to_3h_increments(self._frame([0, 3]),
                                 {0: {'values': None, 'span': None},
                                  3: {'values': [1.0, 2.0], 'span': 3}})
        self.assertTrue(df[df.fhr == 0]['precipitation_mm_3h'].isna().all())

    def test_dry_run_is_accepted_when_the_field_was_packed_as_constant_zero(self):
        from fetch_gfs_forecast import to_3h_increments
        # NCEP packs nbits=0 with a zero reference when nothing fell. That is a
        # forecast of no rain, and must not be mistaken for a broken decode.
        buckets = {3: {'values': [0.0, 0.0], 'span': 3, 'constant_zero': True}}
        df, notes = to_3h_increments(self._frame([3]), buckets)
        self.assertEqual(df['precipitation_mm_3h'].tolist(), [0.0, 0.0])
        self.assertTrue(any('constant zero' in n for n in notes))

    def test_all_zero_with_packed_data_refuses_to_write(self):
        from fetch_gfs_forecast import to_3h_increments
        # Bits present but every decoded value zero is what the sign-magnitude bug
        # looked like. The sum-to-run-total check cannot catch it: 0 sums to 0.
        buckets = {3: {'values': [0.0, 0.0], 'span': 3, 'constant_zero': False}}
        with self.assertRaises(ValueError):
            to_3h_increments(self._frame([3]), buckets)

    def test_negative_increment_refuses_to_write(self):
        from fetch_gfs_forecast import to_3h_increments
        # A 6-hour bucket smaller than the 3-hour bucket inside it is impossible;
        # it means the two windows were mismatched.
        buckets = {3: {'values': [5.0, 5.0], 'span': 3},
                   6: {'values': [1.0, 1.0], 'span': 6}}
        with self.assertRaises(ValueError):
            to_3h_increments(self._frame([3, 6]), buckets)


if __name__ == '__main__':
    unittest.main()

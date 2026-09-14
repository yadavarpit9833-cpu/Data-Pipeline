import unittest
import pandas as pd
import numpy as np
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


if __name__ == '__main__':
    unittest.main()

"""
test_aqi.py — AQI index maths.

The headline test here is test_no_double_conversion: it pins the bug where an
AQI value was pushed through the concentration-to-AQI breakpoints a second
time, which is what the whole of aqi.py is structured to prevent.
"""

import unittest

from aqi import (
    cpcb_subindex,
    combine_subindices,
    categorise,
    cpcb_aqi_from_concentrations,
    CPCB_BREAKPOINTS,
)


class TestCPCBSubIndex(unittest.TestCase):

    def test_band_upper_edges_map_exactly(self):
        """
        Every band's upper concentration must land exactly on its upper index.

        The LOWER edge is deliberately not asserted: CPCB publishes adjacent
        bands as 0-30 / 31-60, so consecutive bands share a concentration
        boundary but leave a one-unit gap in the index (50 then 51). At a
        shared boundary the lower band wins, which is why 30 µg/m³ of PM2.5 is
        AQI 50 and not 51.
        """
        for pollutant, bands in CPCB_BREAKPOINTS.items():
            for _, c_high, _, i_high in bands:
                self.assertAlmostEqual(cpcb_subindex(c_high, pollutant), i_high, places=6,
                                       msg=f"{pollutant} upper edge {c_high}")

    def test_shared_band_boundary_resolves_to_lower_band(self):
        self.assertEqual(cpcb_subindex(30.0, 'pm25'), 50.0)
        self.assertEqual(cpcb_subindex(60.0, 'pm25'), 100.0)

    def test_bands_are_contiguous(self):
        """No concentration between 0 and the top band may be unclassifiable."""
        for pollutant, bands in CPCB_BREAKPOINTS.items():
            for (_, c_high, _, _), (next_low, _, _, _) in zip(bands, bands[1:]):
                self.assertEqual(c_high, next_low,
                                 msg=f"{pollutant} has a gap at {c_high}")

    def test_linear_interpolation_midpoint(self):
        # PM2.5 45 µg/m³ sits halfway through the 30-60 band (51-100).
        self.assertAlmostEqual(cpcb_subindex(45.0, 'pm25'), 75.5, places=1)

    def test_monotonic_in_concentration(self):
        previous = -1.0
        for concentration in range(0, 400, 5):
            value = cpcb_subindex(float(concentration), 'pm25')
            self.assertGreaterEqual(value, previous)
            previous = value

    def test_missing_and_negative_return_none(self):
        self.assertIsNone(cpcb_subindex(None, 'pm25'))
        self.assertIsNone(cpcb_subindex(float('nan'), 'pm25'))
        self.assertIsNone(cpcb_subindex(-1.0, 'pm25'))

    def test_above_top_band_clamps_to_500(self):
        self.assertEqual(cpcb_subindex(5000.0, 'pm25'), 500.0)

    def test_unknown_pollutant_raises(self):
        with self.assertRaises(ValueError):
            cpcb_subindex(10.0, 'radon')


class TestCombineSubIndices(unittest.TestCase):

    def test_overall_aqi_is_the_worst_subindex(self):
        self.assertEqual(combine_subindices([55.0, 180.0, 90.0]), 180.0)

    def test_missing_values_are_skipped(self):
        self.assertEqual(combine_subindices([None, float('nan'), 42.0]), 42.0)

    def test_all_missing_returns_none(self):
        self.assertIsNone(combine_subindices([None, float('nan')]))
        self.assertIsNone(combine_subindices([]))

    def test_no_double_conversion(self):
        """
        REGRESSION: gold_layer used to feed WAQI's AQI sub-indices through
        _pm25_aqi(), the concentration-to-AQI function. An input of 155 —
        already 'unhealthy' on the US EPA scale — came back out as roughly 370,
        landing it in a completely different health category.

        Combining indices must be identity-preserving for a single value.
        """
        waqi_subindex = 155.0
        correct = combine_subindices([waqi_subindex])
        self.assertEqual(correct, waqi_subindex)

        # What the old code did, shown explicitly so the size of the error is
        # on the record rather than a claim in a comment.
        double_converted = cpcb_subindex(waqi_subindex, 'pm25')
        self.assertGreater(double_converted, 300)
        self.assertNotAlmostEqual(double_converted, waqi_subindex, delta=50)


class TestCategories(unittest.TestCase):

    def test_cpcb_categories(self):
        cases = [(25, 'Good'), (75, 'Satisfactory'), (150, 'Moderate'),
                 (250, 'Poor'), (350, 'Very Poor'), (450, 'Severe')]
        for value, expected in cases:
            self.assertEqual(categorise(value, 'cpcb'), expected, msg=f"AQI {value}")

    def test_us_epa_categories_differ_from_cpcb(self):
        """
        REGRESSION: WAQI values were labelled with CPCB band names. The scales
        genuinely differ between 101 and 200 — CPCB calls all of it 'Moderate',
        the EPA splits it — so the health advice attached to a WAQI reading of
        175 was wrong.
        """
        self.assertEqual(categorise(125, 'us_epa'), 'Unhealthy for Sensitive Groups')
        self.assertEqual(categorise(125, 'cpcb'), 'Moderate')
        self.assertEqual(categorise(175, 'us_epa'), 'Unhealthy')
        self.assertEqual(categorise(175, 'cpcb'), 'Moderate')

    def test_missing_is_unknown(self):
        self.assertEqual(categorise(None), 'unknown')
        self.assertEqual(categorise(float('nan')), 'unknown')


class TestCPCBAQIFromConcentrations(unittest.TestCase):

    def test_requires_three_pollutants_including_pm(self):
        """CPCB's own rule: fewer than three pollutants means no AQI."""
        aqi, category, dominant = cpcb_aqi_from_concentrations({'pm25': 80.0})
        self.assertIsNone(aqi)
        self.assertEqual(category, 'insufficient_data')
        self.assertIsNone(dominant)

    def test_requires_a_pm_pollutant(self):
        aqi, category, _ = cpcb_aqi_from_concentrations(
            {'no2': 50.0, 'so2': 30.0, 'o3': 60.0})
        self.assertIsNone(aqi)
        self.assertEqual(category, 'insufficient_data')

    def test_computes_from_three_pollutants_with_pm(self):
        aqi, category, dominant = cpcb_aqi_from_concentrations(
            {'pm25': 95.0, 'no2': 50.0, 'so2': 30.0})
        self.assertIsNotNone(aqi)
        # PM2.5 at 95 is in the 91-120 'Poor' band.
        self.assertEqual(dominant, 'pm25')
        self.assertEqual(category, 'Poor')

    def test_co_is_in_mg_not_ug(self):
        """
        CPCB's CO breakpoints are mg/m³. Passing CAMS µg/m³ straight in would
        put every reading far above the top band. 2 mg/m³ must be AQI 100.
        """
        self.assertAlmostEqual(cpcb_subindex(2.0, 'co'), 100.0, places=6)
        self.assertEqual(cpcb_subindex(2000.0, 'co'), 500.0)


if __name__ == '__main__':
    unittest.main()

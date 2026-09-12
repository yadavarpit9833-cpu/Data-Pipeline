"""
aqi.py — Air Quality Index calculation, kept in one place.

Two scales appear in this pipeline and they are NOT interchangeable:

  * US EPA AQI  — what waqi.info serves in its `iaqi` block. Already an index.
  * India CPCB National AQI — what we compute ourselves from CAMS mass
    concentrations in micrograms per cubic metre.

The bug this module exists to prevent: gold_layer.py used to take WAQI's
already-computed AQI sub-indices and run them through the CPCB
concentration-to-AQI breakpoints a second time, producing a number that
represented nothing. Anything that is already an index goes through
`combine_subindices`; only mass concentrations go through `cpcb_subindex`.

CPCB breakpoints follow the National Air Quality Index report (CPCB, 2014),
Table 2. Note that CPCB publishes the top band of PM2.5 as an open-ended
"250+" and of PM10 as "430+"; the 401-500 sub-range has to be closed somehow
to interpolate, and this module follows the convention used by CPCB's own AQI
calculator (250-380 and 430-510). Concentrations above those upper bounds are
clamped to 500, which is the maximum the scale defines.
"""

import math

# (C_low, C_high, I_low, I_high) per pollutant, in the averaging period and
# unit CPCB specifies for that pollutant.
CPCB_BREAKPOINTS = {
    # PM2.5, 24-hour average, µg/m³
    'pm25': [(0, 30, 0, 50), (30, 60, 51, 100), (60, 90, 101, 200),
             (90, 120, 201, 300), (120, 250, 301, 400), (250, 380, 401, 500)],
    # PM10, 24-hour average, µg/m³
    'pm10': [(0, 50, 0, 50), (50, 100, 51, 100), (100, 250, 101, 200),
             (250, 350, 201, 300), (350, 430, 301, 400), (430, 510, 401, 500)],
    # NO2, 24-hour average, µg/m³
    'no2':  [(0, 40, 0, 50), (40, 80, 51, 100), (80, 180, 101, 200),
             (180, 280, 201, 300), (280, 400, 301, 400), (400, 520, 401, 500)],
    # SO2, 24-hour average, µg/m³
    'so2':  [(0, 40, 0, 50), (40, 80, 51, 100), (80, 380, 101, 200),
             (380, 800, 201, 300), (800, 1600, 301, 400), (1600, 2400, 401, 500)],
    # O3, 8-hour average, µg/m³
    'o3':   [(0, 50, 0, 50), (50, 100, 51, 100), (100, 168, 101, 200),
             (168, 208, 201, 300), (208, 748, 301, 400), (748, 1000, 401, 500)],
    # CO, 8-hour average, mg/m³ (NOT µg/m³ — divide CAMS values by 1000)
    'co':   [(0, 1, 0, 50), (1, 2, 51, 100), (2, 10, 101, 200),
             (10, 17, 201, 300), (17, 34, 301, 400), (34, 50, 401, 500)],
}

# CPCB's six categories.
CPCB_CATEGORIES = [
    (50, 'Good'), (100, 'Satisfactory'), (200, 'Moderate'),
    (300, 'Poor'), (400, 'Very Poor'), (math.inf, 'Severe'),
]

# US EPA's six categories. WAQI values must be labelled with these, not the
# CPCB ones — the band edges differ (EPA splits 101-150 and 151-200, CPCB
# treats 101-200 as a single "Moderate").
US_EPA_CATEGORIES = [
    (50, 'Good'), (100, 'Moderate'), (150, 'Unhealthy for Sensitive Groups'),
    (200, 'Unhealthy'), (300, 'Very Unhealthy'), (math.inf, 'Hazardous'),
]


def _is_missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def cpcb_subindex(concentration, pollutant):
    """
    Converts a mass concentration to its CPCB AQI sub-index by linear
    interpolation within the band it falls in.

    Only ever call this on a concentration. Passing an AQI value in is the
    double-conversion bug this module was written to make obvious.

    Returns None for missing or negative input, and 500 above the top band.
    """
    if _is_missing(concentration) or concentration < 0:
        return None
    breakpoints = CPCB_BREAKPOINTS.get(pollutant)
    if breakpoints is None:
        raise ValueError(f"No CPCB breakpoints defined for pollutant {pollutant!r}")

    for c_low, c_high, i_low, i_high in breakpoints:
        if c_low <= concentration <= c_high:
            return ((i_high - i_low) / (c_high - c_low)) * (concentration - c_low) + i_low
    return 500.0


def combine_subindices(subindices):
    """
    The overall AQI is the worst of its sub-indices. Use this for values that
    are ALREADY indices, such as everything WAQI returns.

    Returns None when no sub-index is available.
    """
    usable = [float(v) for v in subindices if not _is_missing(v)]
    return max(usable) if usable else None


def categorise(aqi_value, scale='cpcb'):
    """Maps a numeric AQI to its category on the given scale."""
    if _is_missing(aqi_value):
        return 'unknown'
    bands = US_EPA_CATEGORIES if scale in ('us_epa', 'epa', 'us') else CPCB_CATEGORIES
    value = float(aqi_value)
    for upper, label in bands:
        if value <= upper:
            return label
    return bands[-1][1]


def cpcb_aqi_from_concentrations(concentrations, min_pollutants=3):
    """
    Computes a CPCB National AQI from a mapping of pollutant -> concentration.

    CPCB requires sub-indices for at least three pollutants, one of which must
    be PM2.5 or PM10; otherwise the AQI is not defined. Returning None here is
    what stops the pipeline publishing a confident-looking number computed from
    a single pollutant.

    Returns (aqi, category, contributing_pollutant).
    """
    subindices = {}
    for pollutant, value in concentrations.items():
        if pollutant not in CPCB_BREAKPOINTS:
            continue
        sub = cpcb_subindex(value, pollutant)
        if sub is not None:
            subindices[pollutant] = sub

    has_pm = 'pm25' in subindices or 'pm10' in subindices
    if len(subindices) < min_pollutants or not has_pm:
        return (None, 'insufficient_data', None)

    worst = max(subindices, key=subindices.get)
    aqi = subindices[worst]
    return (aqi, categorise(aqi, 'cpcb'), worst)

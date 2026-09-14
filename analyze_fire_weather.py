"""
analyze_fire_weather.py — Does weather track stubble burning?

Joins daily FIRMS detections inside a lat/lon box to one station's daily weather
and reports Pearson and Spearman, pooled, within the burning window, and per
year — plus a rain-suppression test.

Defaults to the Punjab/Haryana stubble belt against Delhi, the nearest station
the pipeline polls.

    python analyze_fire_weather.py
    python analyze_fire_weather.py --station Lucknow
    python analyze_fire_weather.py --belt 24 31 74 88 --station Kolkata
    python analyze_fire_weather.py --years 2023 2024 2025

Reading the output — three things decide whether a number means anything:

1. **Pooled figures are confounded by season.** January is cold and quiet;
   November is warm and peaks. A pooled temperature correlation mostly measures
   the calendar. The Oct+Nov block is the one to read.
2. **A correlation is only trustworthy if its sign holds across years.** The
   per-year table exists to check that. A column that flips sign is noise.
3. **Cloud cover suppresses detections as well as burning.** Rain days are
   cloudy days, and the satellite sees less through cloud, so the rain result
   mixes a real effect with an observational artefact. Separating them needs
   cloud-mask data this pipeline does not carry.

Needs `backfill_firms.py` and `backfill_weather.py` to have run over the same
window, or there will be nothing to join.
"""

import os
import sys
import sqlite3
import argparse

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'env_data.db')

# Punjab/Haryana stubble belt: lat_min, lat_max, lon_min, lon_max
DEFAULT_BELT = (29.0, 32.0, 74.0, 77.5)
DEFAULT_STATION = 'Delhi'
BURN_MONTHS = ('10', '11')

SECTORS = [('N', 337.5, 22.5), ('NE', 22.5, 67.5), ('E', 67.5, 112.5), ('SE', 112.5, 157.5),
           ('S', 157.5, 202.5), ('SW', 202.5, 247.5), ('W', 247.5, 292.5), ('NW', 292.5, 337.5)]

VARS = [('t_mean', 'mean temp'), ('t_max', 'max temp'), ('rh_mean', 'humidity'),
        ('rain_sum', 'rainfall'), ('ws_mean', 'wind speed')]


def circ_mean(deg):
    """Vector mean of bearings; an arithmetic mean of 350 and 10 gives 180."""
    v = pd.to_numeric(deg, errors='coerce').dropna()
    if v.empty:
        return np.nan
    r = np.deg2rad(v.to_numpy(float))
    return float(np.rad2deg(np.arctan2(np.sin(r).mean(), np.cos(r).mean())) % 360.0)


def corr(a, b):
    """Pearson and Spearman (Pearson on ranks), NaN-safe."""
    m = a.notna() & b.notna()
    x, y = a[m], b[m]
    if len(x) < 5 or x.std() == 0 or y.std() == 0:
        return np.nan, np.nan, len(x)
    return float(np.corrcoef(x, y)[0, 1]), float(np.corrcoef(x.rank(), y.rank())[0, 1]), len(x)


def sector(d):
    if pd.isna(d):
        return '?'
    for nm, lo, hi in SECTORS:
        if lo > hi:
            if d >= lo or d < hi:
                return nm
        elif lo <= d < hi:
            return nm
    return '?'


def load(belt, station, y0, y1):
    con = sqlite3.connect(DB_PATH, timeout=120)
    try:
        fire = pd.read_sql_query(
            """SELECT substr(timestamp,1,10) AS d, COUNT(*) AS fires
                 FROM cleaned_firms
                WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
                  AND timestamp >= ? AND timestamp < ?
                GROUP BY d""",
            con, params=(belt[0], belt[1], belt[2], belt[3], str(y0), str(y1 + 1)))
        wx = pd.read_sql_query(
            """SELECT substr(timestamp,1,10) AS d,
                      temperature_clean AS t, humidity_clean AS rh,
                      rainfall_clean AS rain, wind_speed_clean AS ws, wind_dir_clean AS wd
                 FROM cleaned_imd
                WHERE station = ? AND timestamp >= ? AND timestamp < ?""",
            con, params=(station, str(y0), str(y1 + 1)))
    finally:
        con.close()
    return fire, wx


def build(fire, wx):
    daily = wx.groupby('d').agg(
        t_mean=('t', 'mean'), t_max=('t', 'max'),
        rh_mean=('rh', 'mean'), rain_sum=('rain', 'sum'), ws_mean=('ws', 'mean'),
    ).reset_index()
    daily['wd_circ'] = wx.groupby('d')['wd'].apply(circ_mean).values

    # Left-join onto the weather spine, never an inner join: a day with zero
    # detections in the box is an absent row, not a zero row, and dropping it
    # biases the rain test — a wet day is exactly the day that records no fires.
    df = daily.merge(fire, on='d', how='left')
    df['fires'] = df['fires'].fillna(0).astype(int)
    df = df.sort_values('d').reset_index(drop=True)
    df['year'] = df['d'].str[:4]
    df['month'] = df['d'].str[5:7]
    return df


def table(df, title, note=None):
    print('\n' + '=' * 74)
    print(title)
    if note:
        print(note)
    print('=' * 74)
    print(f"  {'variable':14} {'Pearson':>9} {'Spearman':>10}   n")
    for col, name in VARS:
        p, s, n = corr(df['fires'], df[col])
        print(f"  {name:14} {p:>9.3f} {s:>10.3f}   {n}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--belt', nargs=4, type=float, default=list(DEFAULT_BELT),
                    metavar=('LAT_MIN', 'LAT_MAX', 'LON_MIN', 'LON_MAX'))
    ap.add_argument('--station', default=DEFAULT_STATION)
    ap.add_argument('--years', nargs='+', type=int, default=[2020, 2025],
                    help='first and last year, inclusive')
    args = ap.parse_args()

    belt = tuple(args.belt)
    y0, y1 = min(args.years), max(args.years)

    fire, wx = load(belt, args.station, y0, y1)
    if wx.empty:
        print(f"No weather rows for station '{args.station}' in {y0}-{y1}. "
              f"Run backfill_weather.py first.")
        return 1
    if fire.empty:
        print(f"No fire rows inside {belt} in {y0}-{y1}. Run backfill_firms.py first.")
        return 1

    df = build(fire, wx)
    print(f"box       : {belt[0]}-{belt[1]}N, {belt[2]}-{belt[3]}E")
    print(f"station   : {args.station}")
    print(f"days      : {len(df)}  ({df['d'].min()} .. {df['d'].max()})")
    print(f"zero-fire days: {(df['fires'] == 0).sum()}")
    print(f"detections: {df['fires'].sum():,}")

    table(df, 'POOLED - every month present in the data',
          'Confounded by season: read the burning window below instead.')

    sub = df[df['month'].isin(BURN_MONTHS)]
    if sub.empty:
        print("\nNo Oct/Nov days in range; skipping the burning-window analysis.")
        return 0
    table(sub, 'BURNING WINDOW - October + November only')

    print('\n' + '=' * 74)
    print('PER YEAR - Spearman, burning window')
    print('A column whose sign flips between years is noise, not signal.')
    print('=' * 74)
    print('  ' + 'year  ' + ''.join(f'{n:>12}' for _, n in VARS))
    for y in sorted(sub['year'].unique()):
        row = sub[sub['year'] == y]
        print(f"  {y}" + ''.join(f"{corr(row['fires'], row[c])[1]:>12.3f}" for c, _ in VARS))

    # Rain suppression - less confounded than any raw correlation, though still
    # entangled with cloud cover hiding fires from the sensor.
    print('\n' + '=' * 74)
    print('RAIN SUPPRESSION - detections the day after rain vs after a dry day')
    print('=' * 74)
    s2 = sub.sort_values('d').reset_index(drop=True).copy()
    s2['rain_prev'] = s2['rain_sum'].shift(1)
    s2['gap'] = (pd.to_datetime(s2['d']) - pd.to_datetime(s2['d']).shift(1)).dt.days
    s2 = s2[s2['gap'] == 1]                    # consecutive calendar days only
    wet, dry = s2[s2['rain_prev'] > 1.0]['fires'], s2[s2['rain_prev'] == 0.0]['fires']
    print(f"  after a wet day (>1mm) : n={len(wet):>4}  median {wet.median():>8.0f}  mean {wet.mean():>8.0f}")
    print(f"  after a dry day (0mm)  : n={len(dry):>4}  median {dry.median():>8.0f}  mean {dry.mean():>8.0f}")
    if len(wet) > 3 and len(dry) > 3 and dry.median() > 0:
        print(f"  median ratio wet/dry   : {wet.median()/dry.median():.2f}")

    print('\n' + '=' * 74)
    print(f'{args.station.upper()} WIND DIRECTION on the 30 heaviest fire days')
    print('Descriptive only. This is the wind at the station on the day the fires')
    print('burned; it does not establish that smoke reached the station.')
    print('=' * 74)
    top = sub.nlargest(30, 'fires')['wd_circ'].apply(sector).value_counts()
    base = sub['wd_circ'].apply(sector).value_counts(normalize=True)
    print(f"  {'sector':8} {'top-30 days':>12} {'share':>8} {'baseline':>10}")
    for nm, _, _ in SECTORS:
        c = int(top.get(nm, 0))
        print(f"  {nm:8} {c:>12} {c/30:>8.0%} {base.get(nm, 0):>10.0%}")
    return 0


if __name__ == '__main__':
    sys.exit(main())

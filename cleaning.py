import pandas as pd
import numpy as np

try:
    from sklearn.neighbors import KNeighborsRegressor
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

_SORT_KEY = '__qc_sort_key__'


def _with_sort_key(df, time_col):
    """
    Returns (frame, sort_column) with a real datetime sort key.

    BUGFIX: the QC checks used to sort by the raw timestamp STRING. ISO strings
    only sort correctly when every value carries the same UTC offset, and this
    pipeline mixes them routinely: WAQI reports '...+05:30' while the fallback
    timestamp and every other source use '...+00:00'. Under a string sort
    '2026-09-12T23:30:00+05:30' (18:00 UTC) sorts AFTER
    '2026-09-12T20:00:00+00:00' (20:00 UTC), so step and flatline checks
    compared readings in the wrong temporal order and produced wrong flags.
    """
    if not time_col or time_col not in df.columns:
        return df, None
    out = df.copy()
    parsed = pd.to_datetime(out[time_col], utc=True, errors='coerce', format='mixed')
    # Rows whose timestamp will not parse keep their original ordering rather
    # than being silently shuffled to one end of the frame.
    if parsed.isna().all():
        return out, time_col
    out[_SORT_KEY] = parsed
    return out, _SORT_KEY


def _sort_for_qc(df, group_cols, time_col):
    """Sorts by group then true chronological order, returning the sorted frame."""
    frame, sort_col = _with_sort_key(df, time_col)
    sort_cols = []
    if group_cols:
        sort_cols.extend(group_cols if isinstance(group_cols, list) else [group_cols])
    if sort_col:
        sort_cols.append(sort_col)
    if sort_cols:
        frame = frame.sort_values(by=sort_cols, kind='stable')
    return frame


def check_range(df, value_col, min_val=0.0, max_val=1000.0):
    """
    Flags readings outside parameter-specific physical bounds (range_fail).
    """
    if min_val is None and max_val is None:
        return pd.Series(False, index=df.index)
        
    s = df[value_col]
    fail = pd.Series(False, index=df.index)
    if min_val is not None:
        fail = fail | (s < min_val)
    if max_val is not None:
        fail = fail | (s > max_val)
    return fail & s.notna()

def check_step(df, value_col, group_cols=None, time_col=None, max_step_change=300.0, is_circular=False):
    """
    Flags implausibly fast jumps compared to the prior reading at the same station (step_fail).
    Supports circular differences for wind direction (0°–360°).
    """
    if max_step_change is None or max_step_change <= 0:
        return pd.Series(False, index=df.index)
        
    original_index = df.index
    df_sorted = _sort_for_qc(df, group_cols, time_col)

    s = df_sorted[value_col]

    if group_cols:
        s_prev = df_sorted.groupby(group_cols)[value_col].shift(1)
    else:
        s_prev = s.shift(1)
        
    valid_mask = s.notna() & s_prev.notna()
    
    if is_circular:
        # Circular diff: min(abs(a - b), 360 - abs(a - b))
        diff_raw = (s - s_prev).abs()
        diff = np.minimum(diff_raw, 360.0 - diff_raw)
    else:
        diff = (s - s_prev).abs()
        
    is_step_fail = valid_mask & (diff > max_step_change)
    return is_step_fail.reindex(original_index, fill_value=False)

def check_flatline(df, value_col, group_cols=None, time_col=None, window=12, ignore_zero=False):
    """
    Flags values that repeat identically for window consecutive readings (flatline).
    If ignore_zero is True (e.g. for rainfall/precipitation), consecutive zero values are not flagged.
    """
    if window is None or window <= 1:
        return pd.Series(False, index=df.index)
        
    original_index = df.index
    df_sorted = _sort_for_qc(df, group_cols, time_col)

    s = df_sorted[value_col]

    # Run-length identification
    is_diff = (s != s.shift(1)) | s.isna()
    if group_cols:
        group_diff = pd.Series(False, index=df_sorted.index)
        for gc in (group_cols if isinstance(group_cols, list) else [group_cols]):
            group_diff = group_diff | (df_sorted[gc] != df_sorted[gc].shift(1))
        is_diff = is_diff | group_diff
        
    streak_id = is_diff.cumsum()
    run_sizes = streak_id.groupby(streak_id).transform('count')
    
    is_flatline = (run_sizes >= window) & s.notna()
    if ignore_zero:
        is_flatline = is_flatline & (s != 0)
        
    return is_flatline.reindex(original_index, fill_value=False)

def detect_outliers_log_mad(df, value_col, group_cols=None, threshold=3.5):
    """
    Skew-robust statistical outlier detection operating in log-space (log(x + 1))
    using Median Absolute Deviation (MAD).
    """
    s = df[value_col]
    valid_mask = s.notna() & (s >= 0)
    if not valid_mask.any():
        return pd.Series(False, index=df.index)
        
    y = np.log1p(s[valid_mask])
    
    def calc_mad_outlier(sub_y):
        med = sub_y.median()
        mad = (sub_y - med).abs().median()
        if mad == 0 or pd.isna(mad):
            # Fallback to IQR in log-space if MAD is zero
            q25, q75 = sub_y.quantile(0.25), sub_y.quantile(0.75)
            iqr = q75 - q25
            if iqr == 0 or pd.isna(iqr):
                return pd.Series(False, index=sub_y.index)
            dev = (sub_y - med).abs()
            return dev > (1.5 * iqr)
        
        # Standardized MAD score
        mod_z = 0.6745 * (sub_y - med).abs() / mad
        return mod_z > threshold

    if group_cols:
        is_outlier_valid = df.loc[valid_mask].groupby(group_cols)[value_col].transform(
            lambda sub: calc_mad_outlier(np.log1p(sub))
        )
    else:
        is_outlier_valid = calc_mad_outlier(y)
        
    res = pd.Series(False, index=df.index)
    res.loc[valid_mask] = is_outlier_valid
    return res

def impute_spatial_knn(df, value_col, lat_col='lat', lon_col='lon', n_neighbors=5):
    """
    Imputes missing values using spatial KNN or inverse-distance weighting fallback.
    Filters out rows with invalid/NaN coordinates before spatial fitting.
    """
    df_out = df.copy()
    
    valid_coords = df_out[lat_col].notna() & df_out[lon_col].notna()
    known_mask = valid_coords & df_out[f'{value_col}_clean'].notna()
    unknown_mask = valid_coords & df_out[f'{value_col}_clean'].isna()
    
    if unknown_mask.sum() == 0 or known_mask.sum() == 0:
        return df_out
        
    X_train = df_out.loc[known_mask, [lat_col, lon_col]].values
    y_train = df_out.loc[known_mask, f'{value_col}_clean'].values
    X_test = df_out.loc[unknown_mask, [lat_col, lon_col]].values
    
    if HAS_SKLEARN:
        knn = KNeighborsRegressor(n_neighbors=min(n_neighbors, len(X_train)), weights='distance')
        knn.fit(X_train, y_train)
        df_out.loc[unknown_mask, f'{value_col}_clean'] = knn.predict(X_test)
    else:
        preds = []
        for pt in X_test:
            dists = np.linalg.norm(X_train - pt, axis=1)
            dists = np.where(dists == 0, 1e-6, dists)
            weights = 1.0 / dists
            preds.append(np.sum(weights * y_train) / np.sum(weights))
        df_out.loc[unknown_mask, f'{value_col}_clean'] = preds
        
    return df_out

def clean_and_impute(df, value_col, time_col=None, lat_col=None, lon_col=None, group_cols=None,
                     min_val=None, max_val=None, max_step_change=None, is_circular=False,
                     ignore_zero_flatline=False, window_flatline=12, enable_mad=False):
    """
    Master cleaning and quality control function:
    1. Multi-stage QC checks (range, step, flatline, optional log-MAD).
    2. Constructs comma-separated QC flags (e.g. 'ok' or 'range_fail,step_fail').
    3. Keeps raw values in {value_col}_clean (does NOT delete/zero flagged values).
    4. Interpolates (time & spatial KNN) ONLY for originally missing (NaN) values.
    """
    df = df.copy()
    
    # 1. Quality Control Checks
    r_fail = check_range(df, value_col, min_val, max_val)
    s_fail = check_step(df, value_col, group_cols, time_col, max_step_change, is_circular)
    f_fail = check_flatline(df, value_col, group_cols, time_col, window_flatline, ignore_zero_flatline)
    
    if enable_mad:
        m_fail = detect_outliers_log_mad(df, value_col, group_cols)
    else:
        m_fail = pd.Series(False, index=df.index)
        
    # Build the comma-separated flag string per row.
    # Vectorised: the previous implementation looped over df.index with .loc
    # per flag, which is four scalar lookups per row. On the GFS grid that is
    # ~234,000 lookups per metric per run.
    flag_masks = [
        ('range_fail', r_fail),
        ('step_fail', s_fail),
        ('flatline', f_fail),
        ('mad_outlier', m_fail),
    ]
    parts = [
        pd.Series(np.where(mask.fillna(False), name, ''), index=df.index)
        for name, mask in flag_masks
    ]
    joined = parts[0]
    for part in parts[1:]:
        both = (joined != '') & (part != '')
        joined = joined + np.where(both, ',', '') + part
    df[f'{value_col}_qc_flag'] = joined.replace('', 'ok')
    
    # 2. Set clean column (retain original raw value, DO NOT replace flagged values with NaN)
    df[f'{value_col}_clean'] = df[value_col]
    
    # Track originally missing values for imputation
    originally_missing = df[value_col].isna()
    
    # 3. Imputation strictly on originally missing (NaN) values
    if originally_missing.any():
        if time_col and time_col in df.columns:
            # Sort chronologically, not lexicographically: linear interpolation
            # between neighbours is only meaningful if the neighbours really
            # are adjacent in time. See _with_sort_key.
            df = _sort_for_qc(df, None, time_col)
            if group_cols:
                df[f'{value_col}_clean'] = df.groupby(group_cols)[f'{value_col}_clean'].transform(
                    lambda x: x.interpolate(method='linear', limit=3)
                )
            else:
                df[f'{value_col}_clean'] = df[f'{value_col}_clean'].interpolate(method='linear', limit=3)
                
        if lat_col and lon_col and lat_col in df.columns and lon_col in df.columns:
            df = impute_spatial_knn(df, value_col, lat_col, lon_col)

    df[f'{value_col}_imputed'] = originally_missing & df[f'{value_col}_clean'].notna()
    # The sort key is an internal artefact; it must not reach Parquet or the DB.
    return df.drop(columns=[_SORT_KEY], errors='ignore')


import pandas as pd
import numpy as np

try:
    from sklearn.neighbors import KNeighborsRegressor
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

def detect_outliers_3std(df, value_col, group_cols=None):
    """
    Flags outliers beyond 3 standard deviations.
    """
    df_out = df.copy()
    if group_cols:
        grouped = df_out.groupby(group_cols)[value_col]
        mean = grouped.transform('mean')
        std = grouped.transform('std')
        std = std.replace(0, np.nan)  # Series .replace() — safe
    else:
        mean = df_out[value_col].mean()
        std = df_out[value_col].std()
        if pd.isna(std) or std == 0:
            # No variance — nothing to flag as outlier
            df_out[f'{value_col}_clean'] = df_out[value_col]
            return df_out
    
    is_outlier = np.abs(df_out[value_col] - mean) > (3 * std)
    
    df_out[f'{value_col}_clean'] = np.where(is_outlier, np.nan, df_out[value_col])
    return df_out

def impute_spatial_knn(df, value_col, lat_col='lat', lon_col='lon', n_neighbors=5):
    """
    Imputes missing values using spatial KNN or inverse-distance weighting fallback.
    Filters out rows with invalid/NaN coordinates before spatial fitting.
    """
    df_out = df.copy()
    
    # Coordinates must be valid
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

def clean_and_impute(df, value_col, time_col=None, lat_col=None, lon_col=None, group_cols=None):
    """
    Master cleaning function:
    1. Outlier detection (> 3 std dev) -> replaced with NaN
    2. Time-based interpolation (gap <= 3)
    3. Spatial KNN interpolation for remaining missing values
    """
    df = df.copy()
    if f'{value_col}_clean' not in df.columns:
        df[f'{value_col}_clean'] = df[value_col]
        
    df = detect_outliers_3std(df, value_col, group_cols)
    
    if time_col and time_col in df.columns:
        df = df.sort_values(by=time_col)
        if group_cols:
            df[f'{value_col}_clean'] = df.groupby(group_cols)[f'{value_col}_clean'].transform(
                lambda x: x.interpolate(method='linear', limit=3)
            )
        else:
            df[f'{value_col}_clean'] = df[f'{value_col}_clean'].interpolate(method='linear', limit=3)
            
    if lat_col and lon_col and lat_col in df.columns and lon_col in df.columns:
        df = impute_spatial_knn(df, value_col, lat_col, lon_col)
        
    # Safe computation of imputed flag
    was_nan = df[value_col].isna()
    val_orig = df[value_col].fillna(-999999)
    val_clean = df[f'{value_col}_clean'].fillna(-999999)
    is_changed = (val_orig != val_clean)
    
    df[f'{value_col}_imputed'] = (was_nan | is_changed) & df[f'{value_col}_clean'].notna()
    return df

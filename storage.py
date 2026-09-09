import os
import json
import hashlib
import tempfile
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger('storage')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

def compute_payload_hash(data):
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(b).hexdigest()

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def save_raw_data(source, timestamp_str, payload, ext='json'):
    """
    Saves raw unparsed payloads to their natural format in the local data lake.
    """
    try:
        if not timestamp_str:
            timestamp_str = datetime.now(timezone.utc).isoformat()
        
        # Simple date extraction
        date_str = timestamp_str[:10]
        payload_hash = compute_payload_hash(payload)
        
        target_dir = os.path.join(DATA_DIR, 'raw', source, f"date={date_str}")
        ensure_dir(target_dir)
        
        file_path = os.path.join(target_dir, f"{payload_hash}.{ext}")
        
        if not os.path.exists(file_path):
            mode = 'wb' if ext == 'bin' or isinstance(payload, bytes) else 'w'
            with open(file_path, mode) as f:
                if mode == 'wb':
                    f.write(payload if isinstance(payload, bytes) else payload.encode('utf-8'))
                else:
                    if isinstance(payload, dict) or isinstance(payload, list):
                        json.dump(payload, f)
                    else:
                        f.write(str(payload))
    except Exception as e:
        logger.error(f"Failed to save raw data for {source}: {e}")

def save_cleaned_data_parquet(df, source, partition_key, partition_value, dedup_keys, pure_overwrite=False):
    """
    Saves cleaned tabular data to Parquet using a partition strategy.
    Implements Read-Merge-Write for continuous sources to prevent data loss on same-day reruns.
    """
    if df is None or df.empty:
        return

    target_dir = os.path.join(DATA_DIR, f"cleaned_{source}")
    ensure_dir(target_dir)
    
    file_path = os.path.join(target_dir, f"{partition_key}={partition_value}.parquet")
    
    try:
        # Convert all object columns to string to avoid pyarrow inference issues
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str)
            
        if pure_overwrite or not os.path.exists(file_path):
            df_to_save = df
        else:
            # Read-Merge-Write
            existing_df = pd.read_parquet(file_path)
            # Combine, placing new data at the end
            combined = pd.concat([existing_df, df], ignore_index=True)
            # Deduplicate by unique keys, keeping the last (newest) record
            df_to_save = combined.drop_duplicates(subset=dedup_keys, keep='last')
            
        # Write to temp file then rename for atomic replace
        fd, temp_path = tempfile.mkstemp(suffix='.parquet', dir=target_dir)
        os.close(fd)
        
        df_to_save.to_parquet(temp_path, index=False)
        # On Windows, os.replace guarantees atomic rename and handles existing files
        os.replace(temp_path, file_path)
        
    except Exception as e:
        logger.error(f"Failed to save parquet for {source}: {e}")

def load_historical_context(source, current_date_str, days_back=1):
    """
    Loads Parquet data from previous dates to provide historical context for QC checks 
    (like step change or flatline) across midnight boundaries.
    """
    try:
        target_dir = os.path.join(DATA_DIR, f"cleaned_{source}")
        if not os.path.exists(target_dir):
            return None
            
        dt = datetime.strptime(current_date_str[:10], '%Y-%m-%d')
        dfs = []
        
        # Load up to `days_back` days + current day (Wait, the QC needs prior context, not future. The current day might not be saved yet).
        for i in range(days_back, 0, -1):
            past_dt = dt - timedelta(days=i)
            past_date_str = past_dt.strftime('%Y-%m-%d')
            file_path = os.path.join(target_dir, f"date={past_date_str}.parquet")
            
            if os.path.exists(file_path):
                dfs.append(pd.read_parquet(file_path))
                
        # Also load current day in case there's earlier data in the current day partition!
        curr_file_path = os.path.join(target_dir, f"date={current_date_str[:10]}.parquet")
        if os.path.exists(curr_file_path):
            dfs.append(pd.read_parquet(curr_file_path))
                
        if not dfs:
            return None
            
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logger.warning(f"Failed to load historical context for {source}: {e}")
        return None

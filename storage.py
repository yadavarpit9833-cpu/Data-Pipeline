"""
storage.py — Local data lake: content-addressed raw payloads and
partitioned Parquet for the cleaned (silver) layer.

Fixes in this revision:

  * save_cleaned_data_parquet used to cast every object column with
    `df[col] = df[col].astype(str)`, mutating the CALLER's DataFrame as a side
    effect. Depending on the pandas version that also turned NaN into the
    literal string 'nan', which then round-tripped back into the QC layer via
    load_historical_context as a value that is neither missing nor numeric.
    The frame is copied first and missing values are preserved.
  * A failed write left its temporary file behind in the partition directory,
    where the next read picked it up as data. Temp files are now cleaned up.
  * save_raw_data returns the path it wrote, so callers can record lineage.
"""

import os
import json
import hashlib
import tempfile
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

logger = logging.getLogger('storage')

DATA_DIR = os.getenv(
    'DATA_DIR',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'),
)


def compute_payload_hash(data):
    """SHA-256 of a payload, used to address raw files by content."""
    if isinstance(data, str):
        b = data.encode('utf-8')
    elif isinstance(data, bytes):
        b = data
    else:
        b = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(b).hexdigest()


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_raw_data(source, timestamp_str, payload, ext='json'):
    """
    Writes a raw payload to data/raw/<source>/date=YYYY-MM-DD/<sha256>.<ext>.

    Content addressing makes this idempotent: an identical payload fetched
    twice occupies one file. Returns the path written (or the existing path),
    or None on failure — raw archival must never break a run.
    """
    try:
        if not timestamp_str:
            timestamp_str = datetime.now(timezone.utc).isoformat()

        date_str = str(timestamp_str)[:10]
        payload_hash = compute_payload_hash(payload)

        target_dir = os.path.join(DATA_DIR, 'raw', source, f"date={date_str}")
        ensure_dir(target_dir)
        file_path = os.path.join(target_dir, f"{payload_hash}.{ext}")

        if os.path.exists(file_path):
            return file_path

        is_binary = isinstance(payload, (bytes, bytearray))
        if is_binary:
            with open(file_path, 'wb') as f:
                f.write(payload)
        else:
            with open(file_path, 'w', encoding='utf-8') as f:
                if isinstance(payload, (dict, list)):
                    json.dump(payload, f, sort_keys=True)
                else:
                    f.write(str(payload))
        return file_path
    except Exception as e:
        logger.error(f"Failed to save raw data for {source}: {e}")
        return None


def _type_category(value):
    """
    Buckets a value into 'number', 'text', 'bool' or its type name.

    Comparing type() directly does not work here: a float64 Series returns
    numpy.float64 from .iloc[0] but plain Python floats when iterated, so an
    entirely numeric column looked mixed.
    """
    if isinstance(value, (bool, np.bool_)):
        return 'bool'
    if isinstance(value, (int, float, np.integer, np.floating)):
        return 'number'
    if isinstance(value, str):
        return 'text'
    return type(value).__name__


def _is_mixed_type(series):
    """
    True when a column holds more than one kind of value among its non-nulls.

    This is what pyarrow cannot infer a type for. FIRMS is the live example:
    MODIS reports `confidence` as an integer percentage (85) and VIIRS as a
    class letter ('n'), so a partition holding both fails with
        Could not convert 'n' with type str: tried to convert to int64
    """
    values = series.dropna()
    if values.empty:
        return False
    categories = set()
    for value in values:
        categories.add(_type_category(value))
        if len(categories) > 1:
            return True
    return False


def _harmonise_for_parquet(df):
    """
    Makes a frame safe to write as Parquet without destroying missing values.

    Only textual and object columns are touched. `object` is precisely the
    dtype pandas uses when it could not resolve a single type, which is what a
    column merged from two sensors becomes — so casting those to text is both
    necessary and sufficient. Numeric, boolean and datetime columns keep their
    types.

    Casting only the non-null values matters: `astype(str)` renders NaN as the
    four-character string 'nan' on some pandas versions, which then reads back
    as a real value.
    """
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        if not (dtype == object or pd.api.types.is_string_dtype(dtype)):
            continue
        if dtype == object and _is_mixed_type(out[col]):
            logger.info(
                f"Column {col!r} holds more than one value type "
                f"(a provider-specific field such as FIRMS confidence); "
                f"storing it as text so the partition stays readable.")
        mask = out[col].notna()
        out[col] = out[col].where(~mask, out[col][mask].astype(str))
    return out


# Kept as an alias: the previous name is referenced by the regression suite.
_stringify_object_columns = _harmonise_for_parquet


def save_cleaned_data_parquet(df, source, partition_key, partition_value,
                              dedup_keys, pure_overwrite=False):
    """
    Writes cleaned data to data/cleaned_<source>/<key>=<value>.parquet.

    Continuous sources use read-merge-write so a same-day rerun adds to the
    partition instead of replacing it. Sources that always produce a complete
    partition in one run (GFS cycles) pass pure_overwrite=True.

    The write goes to a temporary file and is moved into place with os.replace,
    which is atomic on POSIX and Windows, so a reader never sees a half file.
    """
    if df is None or df.empty:
        return None

    target_dir = os.path.join(DATA_DIR, f"cleaned_{source}")
    ensure_dir(target_dir)
    file_path = os.path.join(target_dir, f"{partition_key}={partition_value}.parquet")

    temp_path = None
    try:
        df_to_save = df

        if not pure_overwrite and os.path.exists(file_path):
            existing = pd.read_parquet(file_path)
            combined = pd.concat([existing, df_to_save], ignore_index=True)
            usable_keys = [k for k in dedup_keys if k in combined.columns]
            if usable_keys:
                # keep='last' so the freshly fetched record wins over the
                # stored one when both describe the same observation.
                combined = combined.drop_duplicates(subset=usable_keys, keep='last')
            else:
                logger.warning(
                    f"None of the dedup keys {dedup_keys} are present in the "
                    f"{source} frame; writing without deduplication."
                )
            df_to_save = combined

        # BUGFIX: this used to run on `df` BEFORE the merge, so the frame that
        # was actually written — the concatenation of the stored partition and
        # the new rows — was never harmonised. A column that arrived as int64
        # from one sensor and as text from another became mixed only at that
        # point, and every FIRMS Parquet write failed with
        #   Could not convert 'n' with type str: tried to convert to int64
        # while the SQLite write succeeded, so the run looked healthy and the
        # gold layer silently had no fire data.
        df_to_save = _harmonise_for_parquet(df_to_save)

        fd, temp_path = tempfile.mkstemp(suffix='.parquet.tmp', dir=target_dir)
        os.close(fd)
        df_to_save.to_parquet(temp_path, index=False)
        os.replace(temp_path, file_path)
        temp_path = None
        return file_path
    except Exception as e:
        logger.error(f"Failed to save parquet for {source}: {e}")
        return None
    finally:
        # A leftover temp file in the partition directory would be picked up
        # by the next glob as if it were data.
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def load_historical_context(source, current_date_str, days_back=1):
    """
    Loads recent partitions so QC checks (step change, flatline) have prior
    readings to compare against across the midnight partition boundary.

    Returns None when there is nothing stored yet.
    """
    try:
        target_dir = os.path.join(DATA_DIR, f"cleaned_{source}")
        if not os.path.isdir(target_dir):
            return None

        dt = datetime.strptime(str(current_date_str)[:10], '%Y-%m-%d')
        frames = []

        for i in range(days_back, 0, -1):
            past = (dt - timedelta(days=i)).strftime('%Y-%m-%d')
            path = os.path.join(target_dir, f"date={past}.parquet")
            if os.path.exists(path):
                frames.append(pd.read_parquet(path))

        # The current day's partition holds earlier readings from today.
        current_path = os.path.join(target_dir, f"date={str(current_date_str)[:10]}.parquet")
        if os.path.exists(current_path):
            frames.append(pd.read_parquet(current_path))

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)
    except Exception as e:
        logger.warning(f"Failed to load historical context for {source}: {e}")
        return None

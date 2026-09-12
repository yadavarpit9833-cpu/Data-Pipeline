# The pipeline used to be deployable only on Windows, via three PowerShell
# scheduled-task scripts. This image makes it run identically anywhere, which
# matters when the demo machine is not the development machine.
FROM python:3.12-slim

# Current eccodes wheels bundle the native library, so this is belt-and-braces
# rather than strictly required: it guarantees GRIB2 decoding still works on a
# platform where pip falls back to a source build. fetch_gfs raises a clear
# error if ecCodes is unavailable rather than fabricating a grid, which is what
# the previous version did.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libeccodes0 \
        libeccodes-data \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    SQLITE_DB_PATH=/data/env_data.db

VOLUME ["/data"]

# monitor_health exits non-zero when any source is stale.
HEALTHCHECK --interval=5m --timeout=30s --start-period=2m --retries=3 \
    CMD python monitor_health.py --json > /dev/null || exit 1

CMD ["python", "scheduler.py"]

# The pipeline used to be deployable only on Windows, via three PowerShell
# scheduled-task scripts. This image makes it run identically anywhere, which
# matters when the demo machine is not the development machine.
FROM python:3.12-slim

# ecCodes is the system library behind cfgrib. Without it fetch_gfs cannot
# decode NOAA GRIB2 files — and it will say so loudly rather than fabricate
# a grid, which is what the previous version did.
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

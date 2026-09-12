# scripts/

One-off migrations, inspection helpers and Windows deployment scripts. None of
these are part of the running pipeline — they live here so the repository root
contains only modules the pipeline actually imports.

| Script | Purpose |
|---|---|
| `migrate_db.py` | Rebuilds tables against the current `schema.sql`, backfilling hashes and `valid_time`. Takes a backup first. |
| `migrate_to_v2_schema.py` | Renames the pre-v2 tables (`*_cpcb`, `*_imd`, `*_sentinel5p`) and quarantines the synthetic GFS rows. Run once. |
| `migrate_sqlite_to_parquet.py` | Backfills the Parquet lake from an existing SQLite database. |
| `query_db.py` | Row counts per table, plus an ad-hoc query passed as an argument. |
| `diagnose_gfs.py` | GFS coverage and cycle diagnostics. |
| `inspect_firms.py` | FIRMS payload and duplicate inspection. |
| `test_duckdb.py` | Scratch DuckDB check. Not part of the test suite despite the name. |
| `normalize_gfs_cycles.py` | Historical one-off: repaired `cycle='00'` rows. Contains a hardcoded date and is kept only for provenance. |
| `setup_*.ps1`, `check_task_alive.ps1`, `capture_task_deletion_evidence.ps1` | Windows Task Scheduler deployment. Superseded by the Dockerfile for anything portable. |

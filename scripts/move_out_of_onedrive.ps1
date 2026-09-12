<#
.SYNOPSIS
    Moves this repository out of OneDrive and re-points the scheduled tasks.

.DESCRIPTION
    OneDrive's sync engine opens and locks files while they are being written.
    For this pipeline that means:

      * env_data.db is locked mid-transaction  -> "database is locked"
      * data/*.parquet is locked during the atomic rename -> PermissionError
      * env_data.db-wal / -shm are synced SEPARATELY from env_data.db

    The last one is the dangerous one and it got worse, not better, when WAL
    mode was enabled to fix the locking. SQLite's write-ahead log only makes
    sense when the -wal and -shm sidecar files stay consistent with the main
    database file. OneDrive uploads all three independently and can restore
    them from different points in time, which can leave the database
    unrecoverable rather than merely locked.

    This script:
      1. Refuses to run if the repo is not actually inside OneDrive.
      2. Stops the scheduled tasks and any python holding the database.
      3. Checkpoints the SQLite WAL so no sidecar state is left in flight.
      4. Warns about OneDrive cloud-only placeholder files, which move as
         0-byte stubs unless they are downloaded first.
      5. Moves the whole folder to the destination.
      6. Re-registers the scheduled tasks from the new location.

    Nothing is deleted. The move is a Move-Item; if it fails, the original
    folder is still there.

.PARAMETER Destination
    Where to move the repo. Default: C:\DataPipeline

.PARAMETER Force
    Actually perform the move. Without this the script only reports what it
    would do.

.EXAMPLE
    # See what would happen
    powershell -ExecutionPolicy Bypass -File scripts\move_out_of_onedrive.ps1

.EXAMPLE
    # Do it
    powershell -ExecutionPolicy Bypass -File scripts\move_out_of_onedrive.ps1 -Force
#>

[CmdletBinding()]
param(
    [string]$Destination = 'C:\DataPipeline',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

$repo = Split-Path -Parent $PSScriptRoot
if (-not $repo) { throw 'Could not resolve the repository root from $PSScriptRoot.' }

$taskNames = @(
    'DataPipelineScheduler',
    'DataPipelineSchedulerMonitor',
    'DataPipelineEvidenceMonitor'
)

Write-Host ''
Write-Host '=== Move repository out of OneDrive ===' -ForegroundColor Cyan
Write-Host "  Source      : $repo"
Write-Host "  Destination : $Destination"
Write-Host ''

# ── 1. Is it even in OneDrive? ──────────────────────────────────────────────
$inOneDrive = ($repo -like '*OneDrive*') -or
              ($env:OneDrive -and $repo.StartsWith($env:OneDrive, 'OrdinalIgnoreCase')) -or
              ($env:OneDriveCommercial -and $repo.StartsWith($env:OneDriveCommercial, 'OrdinalIgnoreCase'))

if (-not $inOneDrive) {
    Write-Host 'This repository is NOT inside OneDrive. Nothing to do.' -ForegroundColor Green
    Write-Host "(Checked against the path itself and `$env:OneDrive = '$env:OneDrive')"
    return
}
Write-Host 'CONFIRMED: this repository is inside OneDrive.' -ForegroundColor Yellow

if (Test-Path $Destination) {
    throw "Destination '$Destination' already exists. Move or rename it first — this script will not merge into an existing folder."
}

# ── 2. What is currently writing to it? ─────────────────────────────────────
Write-Host ''
Write-Host '--- Checking for running writers ---'

$liveTasks = @()
foreach ($name in $taskNames) {
    $t = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($t) {
        $liveTasks += $name
        Write-Host "  scheduled task '$name' exists (state: $($t.State))"
    }
}
if (-not $liveTasks) { Write-Host '  no pipeline scheduled tasks registered' }

$pythons = Get-Process python, pythonw -ErrorAction SilentlyContinue |
           Where-Object { $_.Path -and $_.Path.StartsWith($repo, 'OrdinalIgnoreCase') }
if ($pythons) {
    Write-Host "  $($pythons.Count) python process(es) running from this folder" -ForegroundColor Yellow
}

# ── 3. OneDrive cloud-only placeholders ─────────────────────────────────────
# A file marked RecallOnDataAccess exists only in the cloud. Moving it out of
# the sync root can produce a 0-byte stub, so it must be materialised first.
Write-Host ''
Write-Host '--- Checking for cloud-only (online-only) files ---'
$placeholders = Get-ChildItem -Path $repo -Recurse -Force -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Attributes.ToString() -match 'RecallOnDataAccess|Offline' }

if ($placeholders) {
    Write-Host "  $($placeholders.Count) file(s) are online-only and must be downloaded first." -ForegroundColor Yellow
    Write-Host '  Right-click the folder in Explorer -> "Always keep on this device", wait for'
    Write-Host '  the green tick, then re-run this script.'
    $placeholders | Select-Object -First 5 | ForEach-Object { Write-Host "    $($_.FullName)" }
    if (-not $Force) { Write-Host '' } else {
        throw 'Refusing to move while files are online-only — they would move as empty stubs.'
    }
} else {
    Write-Host '  none — all files are local.' -ForegroundColor Green
}

# ── 4. SQLite WAL state ─────────────────────────────────────────────────────
$dbPath = Join-Path $repo 'env_data.db'
$walPath = "$dbPath-wal"
Write-Host ''
Write-Host '--- SQLite state ---'
if (Test-Path $dbPath) {
    $dbSize = [math]::Round((Get-Item $dbPath).Length / 1MB, 1)
    Write-Host "  env_data.db present (${dbSize} MB)"
    if (Test-Path $walPath) {
        Write-Host '  env_data.db-wal present — uncheckpointed writes exist' -ForegroundColor Yellow
    }
} else {
    Write-Host '  no env_data.db yet'
}

if (-not $Force) {
    Write-Host ''
    Write-Host 'DRY RUN — nothing has been changed.' -ForegroundColor Cyan
    Write-Host 'Re-run with -Force to perform the move:' -ForegroundColor Cyan
    Write-Host "  powershell -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Force"
    return
}

# ═══════════════════════════════════════════════════════════════════════════
# From here on we change things.
# ═══════════════════════════════════════════════════════════════════════════

Write-Host ''
Write-Host '--- Stopping writers ---'
foreach ($name in $liveTasks) {
    Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    Disable-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue | Out-Null
    Write-Host "  stopped and disabled '$name'"
}

if ($pythons) {
    foreach ($p in $pythons) {
        Write-Host "  stopping python PID $($p.Id)"
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 3
}

# Checkpoint the WAL so the -wal/-shm sidecars are folded back into the main
# database file before it travels. Without this the three files can arrive in
# inconsistent states.
if (Test-Path $walPath) {
    Write-Host ''
    Write-Host '--- Checkpointing the SQLite WAL ---'
    $python = Join-Path $repo '.venv\Scripts\python.exe'
    if (-not (Test-Path $python)) { $python = 'python' }
    $checkpoint = @"
import sqlite3
c = sqlite3.connect(r'$dbPath')
c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
c.execute('PRAGMA journal_mode=DELETE')
c.close()
print('  WAL checkpointed and folded into env_data.db')
"@
    try {
        & $python -c $checkpoint
    } catch {
        Write-Warning "  Could not checkpoint the WAL: $_"
        Write-Warning '  The -wal and -shm files will be moved alongside the database.'
    }
}

# ── 5. The move ─────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '--- Moving ---'
$parent = Split-Path -Parent $Destination
if ($parent -and -not (Test-Path $parent)) {
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
}

Move-Item -LiteralPath $repo -Destination $Destination -Force
Write-Host "  moved to $Destination" -ForegroundColor Green

# ── 6. Re-register the tasks from the new location ──────────────────────────
Write-Host ''
Write-Host '--- Re-registering scheduled tasks ---'
$newScripts = Join-Path $Destination 'scripts'

foreach ($name in $liveTasks) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "  unregistered stale '$name'"
}

$setupScripts = @{
    'setup_background_task.ps1' = 'DataPipelineScheduler'
    'setup_monitor_task.ps1'    = 'DataPipelineSchedulerMonitor'
    'setup_evidence_task.ps1'   = 'DataPipelineEvidenceMonitor'
}
foreach ($entry in $setupScripts.GetEnumerator()) {
    $path = Join-Path $newScripts $entry.Key
    if (Test-Path $path) {
        try {
            & powershell -ExecutionPolicy Bypass -File $path
            Write-Host "  re-registered $($entry.Value)" -ForegroundColor Green
        } catch {
            Write-Warning "  $($entry.Key) failed: $_"
        }
    }
}

Write-Host ''
Write-Host '=== Done ===' -ForegroundColor Green
Write-Host "  The repository now lives at: $Destination"
Write-Host '  OneDrive no longer syncs the database or the Parquet lake.'
Write-Host ''
Write-Host '  Next steps:'
Write-Host "    cd $Destination"
Write-Host '    python monitor_health.py      # confirm the pipeline is healthy'
Write-Host ''
Write-Host '  Your .venv may hold absolute paths from the old location. If python'
Write-Host '  misbehaves, rebuild it:'
Write-Host "    rmdir /s /q `"$Destination\.venv`""
Write-Host "    python -m venv `"$Destination\.venv`""
Write-Host "    `"$Destination\.venv\Scripts\pip`" install -r `"$Destination\requirements.txt`""

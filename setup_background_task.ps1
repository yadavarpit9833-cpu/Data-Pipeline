# Registers "DataPipelineScheduler" — the always-on pipeline orchestrator.
# Run this from anywhere; every path is derived from $PSScriptRoot.
#
# IMPORTANT: keep the repo OUT of OneDrive. OneDrive's sync engine locks
# env_data.db and the data/*.parquet files mid-write, which produces
# "database is locked" and PermissionError on the atomic parquet rename.

$ErrorActionPreference = 'Stop'

$repo   = $PSScriptRoot
$target = Join-Path $repo "scheduler.py"

if (-not (Test-Path $target)) {
    Write-Error "scheduler.py not found in '$repo'. Run this script from inside the repo."
    exit 1
}

if ($repo -like "*OneDrive*") {
    Write-Warning "This repo is inside OneDrive. Move it to C:\DataPipeline before running the scheduler."
}

# BUGFIX: this pointed unconditionally at .venv\Scripts\python.exe. When no
# virtualenv exists the task still registered fine and then failed silently at
# every trigger, because Task Scheduler does not validate -Execute up front.
# Resolve a real interpreter now and refuse to register without one.
$python = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $fallback = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $fallback) {
        Write-Error "No interpreter found: '$python' is missing and python.exe is not on PATH. Create the venv or install Python, then re-run."
        exit 1
    }
    $python = $fallback.Source
    Write-Warning "No .venv found; falling back to the interpreter on PATH: $python"
}
Write-Host "Interpreter: $python"

# Pass scheduler.py by absolute path. -WorkingDirectory is still set (the
# fetchers resolve their own paths now, but cwd matters for anything relative).
$action = New-ScheduledTaskAction -Execute $python -Argument "`"$target`"" -WorkingDirectory $repo

# AtLogOn, not AtStartup: an Interactive-logon task cannot run before the user logs in.
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
# RunLevel Limited, not Highest: the pipeline only makes HTTP calls and
# reads/writes files under this repo, so it never needed admin. Highest
# also requires an elevated shell just to register the task.
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

# ExecutionTimeLimit 0 = run forever. The default is PT72H, which is why the
# task kept "disappearing" after exactly three days.
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -DontStopOnIdleEnd `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -MultipleInstances IgnoreNew

$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings `
    -Description "Data Pipeline Background Scheduler"

Register-ScheduledTask -TaskName "DataPipelineScheduler" -InputObject $task -Force | Out-Null
Write-Host "Registered 'DataPipelineScheduler'. ExecutionTimeLimit is unlimited; task starts at logon."
Start-ScheduledTask -TaskName "DataPipelineScheduler"
Write-Host "Started. Check progress with: Get-ScheduledTask -TaskName DataPipelineScheduler"

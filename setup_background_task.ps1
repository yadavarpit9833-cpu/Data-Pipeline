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
#
# BUGFIX: prefer pythonw.exe. python.exe allocates a console, and at logon that
# console is torn down moments later, killing the scheduler with 0xC000013A
# (STATUS_CONTROL_C_EXIT) before it writes a single log line. pythonw.exe has no
# console, so nothing can close it. scheduler.py logs to scheduler.log either way.
$candidates = @(
    (Join-Path $repo ".venv\Scripts\pythonw.exe"),
    (Join-Path $repo ".venv\Scripts\python.exe")
)
foreach ($exe in @('pythonw.exe', 'python.exe')) {
    $found = Get-Command $exe -ErrorAction SilentlyContinue
    if ($found) { $candidates += $found.Source }
}

$python = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $python) {
    Write-Error "No interpreter found: no pythonw.exe/python.exe in '$repo\.venv\Scripts' or on PATH. Create the venv or install Python, then re-run."
    exit 1
}
if ($python -notlike '*pythonw.exe') {
    Write-Warning "Using $python, which allocates a console. pythonw.exe was not found, so the task may be killed at logon when that console closes."
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

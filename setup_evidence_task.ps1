# Registers "DataPipelineEvidenceMonitor" — captures diagnostic evidence when
# the DataPipelineScheduler task crashes or is deleted.

$ErrorActionPreference = 'Stop'

$repo   = $PSScriptRoot
$target = Join-Path $repo "capture_task_deletion_evidence.ps1"

# BUGFIX: same hardcoded "Data Pipeline" (space) path as the monitor task,
# while the checkout is "Data-Pipeline" (hyphen) — the task registered and
# then silently did nothing. Derive it from $PSScriptRoot and verify.
if (-not (Test-Path $target)) {
    Write-Error "capture_task_deletion_evidence.ps1 not found in '$repo'. Run this script from inside the repo."
    exit 1
}

$action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$target`"" `
    -WorkingDirectory $repo

# BUGFIX: was -AtStartup paired with -LogonType Interactive, which can never
# fire — an Interactive-logon task has no session before the user logs in.
# This is the same trap already documented in setup_background_task.ps1.
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest

# BUGFIX: without an explicit ExecutionTimeLimit this inherits the PT72H
# default — the same reason the scheduler task kept vanishing after 3 days.
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -MultipleInstances IgnoreNew

$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings `
    -Description "Monitors DataPipelineScheduler for crashes and captures evidence logs."

Register-ScheduledTask -TaskName "DataPipelineEvidenceMonitor" -InputObject $task -Force | Out-Null
Write-Host "Registered 'DataPipelineEvidenceMonitor'."
Write-Host "Watching: $target"
Start-ScheduledTask -TaskName "DataPipelineEvidenceMonitor"
Write-Host "Started in the background."

# Registers "DataPipelineSchedulerMonitor" — checks every 30 minutes that the
# DataPipelineScheduler task still exists and is alive.

$ErrorActionPreference = 'Stop'

$repo   = $PSScriptRoot
$target = Join-Path $repo "check_task_alive.ps1"

# BUGFIX: this path was hardcoded to "...\Desktop\Data Pipeline\..." (a space),
# but the checkout is "Data-Pipeline" (a hyphen). Task Scheduler does not
# validate -Argument, so the task registered "successfully" and then did
# nothing at every trigger — the monitor never ran once. Derive it instead.
if (-not (Test-Path $target)) {
    Write-Error "check_task_alive.ps1 not found in '$repo'. Run this script from inside the repo."
    exit 1
}

$action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$target`"" `
    -WorkingDirectory $repo

# Omit -RepetitionDuration on purpose: in Task Scheduler XML an absent
# <Duration> under <Repetition> means "repeat indefinitely". Passing
# [TimeSpan]::MaxValue instead serializes to P99999999DT23H59M59S, which the
# service rejects outright with "value ... incorrectly formatted or out of range".
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 30)

# RunLevel Limited, not Highest: this probe only reads the state of the
# current user's own scheduled task and appends to a log, which needs no
# admin. Highest also requires an elevated shell just to register the task.
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$settings  = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -DontStopOnIdleEnd `
    -MultipleInstances IgnoreNew

$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings `
    -Description "Checks every 30 minutes that DataPipelineScheduler is still registered and running."

Register-ScheduledTask -TaskName "DataPipelineSchedulerMonitor" -InputObject $task -Force | Out-Null
Write-Host "Registered 'DataPipelineSchedulerMonitor'. It checks the scheduler every 30 minutes."
Write-Host "Watching: $target"

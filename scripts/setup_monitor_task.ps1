# Registers the 30-minute liveness check as a Windows scheduled task.
#
# The script path was hardcoded to a OneDrive Desktop folder belonging to one
# developer. It now resolves from $PSScriptRoot, so the task points at whatever
# checkout it was registered from and carries no personal path.

$ErrorActionPreference = 'Stop'

$scriptPath = Join-Path $PSScriptRoot 'check_task_alive.ps1'
if (-not (Test-Path $scriptPath)) {
    throw "Cannot find $scriptPath. Run this script from the repo's scripts\ folder."
}

if ($PSScriptRoot -like '*OneDrive*') {
    Write-Warning "This repo is inside OneDrive. Run scripts\move_out_of_onedrive.ps1 first."
}

$action = New-ScheduledTaskAction -Execute 'PowerShell.exe' `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$scriptPath`"" `
    -WorkingDirectory $PSScriptRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 30)

Register-ScheduledTask -TaskName 'DataPipelineSchedulerMonitor' `
    -Action $action -Trigger $trigger -RunLevel Highest -Force | Out-Null
Write-Host "Registered 'DataPipelineSchedulerMonitor' -> $scriptPath (checks every 30 minutes)."

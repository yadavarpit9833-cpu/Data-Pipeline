# Registers the evidence-capture watchdog as a Windows scheduled task.
#
# The script path used to be hardcoded to
#   C:\Users\<name>\OneDrive\Desktop\Data Pipeline\
# which did two harmful things: it embedded one developer's Windows username in
# the repository, and it pointed the pipeline at a OneDrive folder. OneDrive's
# sync engine locks env_data.db and data/*.parquet mid-write, which is what
# produced the "database is locked" errors and the PermissionError on the
# atomic Parquet rename. $PSScriptRoot resolves to wherever this script
# actually lives, so the task follows the repo instead of a stale path.

$ErrorActionPreference = 'Stop'

$scriptPath = Join-Path $PSScriptRoot 'capture_task_deletion_evidence.ps1'
if (-not (Test-Path $scriptPath)) {
    throw "Cannot find $scriptPath. Run this script from the repo's scripts\ folder."
}

if ($PSScriptRoot -like '*OneDrive*') {
    Write-Warning "This repo is inside OneDrive. Run scripts\move_out_of_onedrive.ps1 first."
}

$action = New-ScheduledTaskAction -Execute 'PowerShell.exe' `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$scriptPath`"" `
    -WorkingDirectory $PSScriptRoot
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -DontStopOnIdleEnd

$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings `
    -Description 'Monitors DataPipelineScheduler for crashes and captures evidence logs.'

Register-ScheduledTask -TaskName 'DataPipelineEvidenceMonitor' -InputObject $task -Force | Out-Null
Write-Host "Registered 'DataPipelineEvidenceMonitor' -> $scriptPath"
Start-ScheduledTask -TaskName 'DataPipelineEvidenceMonitor'
Write-Host 'Task started.'

# Run from the repo root. Adjust $repo if your checkout lives elsewhere.
# IMPORTANT: keep the repo OUT of OneDrive. OneDrive's sync engine locks
# env_data.db and the data/*.parquet files mid-write, which produces
# "database is locked" and PermissionError on the atomic parquet rename.
$repo   = $PSScriptRoot
$python = Join-Path $repo ".venv\Scripts\python.exe"

if ($repo -like "*OneDrive*") {
    Write-Warning "This repo is inside OneDrive. Move it to C:\DataPipeline before running the scheduler."
}

$action  = New-ScheduledTaskAction -Execute $python -Argument "scheduler.py" -WorkingDirectory $repo
# AtLogOn, not AtStartup: an Interactive-logon task cannot run before the user logs in.
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
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

Register-ScheduledTask -TaskName "DataPipelineScheduler" -InputObject $task -Force
Write-Host "Registered. ExecutionTimeLimit is unlimited; task starts at logon."
Start-ScheduledTask -TaskName "DataPipelineScheduler"

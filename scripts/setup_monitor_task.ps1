$action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"C:\Users\Arpit Yadav\OneDrive\Desktop\Data Pipeline\check_task_alive.ps1`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 30)
Register-ScheduledTask -TaskName "DataPipelineSchedulerMonitor" -Action $action -Trigger $trigger -RunLevel Highest -Force
Write-Host "Monitor task 'DataPipelineSchedulerMonitor' created successfully. It will check the task status every 30 minutes."

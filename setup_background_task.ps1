$action = New-ScheduledTaskAction -Execute "C:\Users\Arpit Yadav\OneDrive\Desktop\Data Pipeline\.venv\Scripts\python.exe" -Argument "scheduler.py" -WorkingDirectory "C:\Users\Arpit Yadav\OneDrive\Desktop\Data Pipeline"
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -DontStopOnIdleEnd -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5)
$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings -Description "Data Pipeline Background Scheduler"

Register-ScheduledTask -TaskName "DataPipelineScheduler" -InputObject $task -Force
Write-Host "Task Scheduler job 'DataPipelineScheduler' created successfully!"
Write-Host "It will run automatically on Windows startup in the background."
Write-Host "Starting the task right now..."
Start-ScheduledTask -TaskName "DataPipelineScheduler"
Write-Host "Task started! Check scheduler.log for outputs."

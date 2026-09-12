$action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"C:\Users\Arpit Yadav\OneDrive\Desktop\Data Pipeline\capture_task_deletion_evidence.ps1`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -DontStopOnIdleEnd

$task = New-ScheduledTask -Action $action -Principal $principal -Trigger $trigger -Settings $settings -Description "Monitors DataPipelineScheduler for crashes and captures evidence logs."

Register-ScheduledTask -TaskName "DataPipelineEvidenceMonitor" -InputObject $task -Force
Write-Host "Evidence monitor task 'DataPipelineEvidenceMonitor' created successfully."
Write-Host "Starting it in the background now..."
Start-ScheduledTask -TaskName "DataPipelineEvidenceMonitor"
Write-Host "Task is running in the background!"

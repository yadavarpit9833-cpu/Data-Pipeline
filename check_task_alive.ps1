$taskName = "DataPipelineScheduler"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

if ($task -eq $null -or ($task.State -ne 'Running' -and $task.State -ne 'Ready')) {
    $desktopPath = [Environment]::GetFolderPath("Desktop")
    $alertFile = Join-Path -Path $desktopPath -ChildPath "ALERT_task_missing.txt"
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $state = if ($task -eq $null) { "Missing" } else { $task.State }
    $message = "[$timestamp] Alert: Task '$taskName' is in '$state' state or missing!"
    Add-Content -Path $alertFile -Value $message
}

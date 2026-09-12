$taskName = "DataPipelineScheduler"
$scriptDir = $PSScriptRoot
if (-not $scriptDir) { $scriptDir = $PWD.Path }
$evidenceFile = Join-Path -Path $scriptDir -ChildPath "deletion_evidence.txt"

Write-Host "Monitoring task '$taskName' every 2 minutes."
Write-Host "Evidence will be saved to: $evidenceFile"
Write-Host "Press Ctrl+C to stop manually..."

while ($true) {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    
    # Check if task is missing or not in 'Running' state
    if ($task -eq $null -or $task.State -ne 'Running') {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $state = if ($task -eq $null) { "Missing" } else { $task.State }
        
        Write-Host ""
        Write-Host "=================================================="
        Write-Host "[$timestamp] ALERT: Task is in '$state' state!"
        Write-Host "Capturing Event Viewer evidence..."
        Write-Host "=================================================="
        
        $header = "[$timestamp] Task state changed to: $state`r`n`r`n--- Last 20 Task Scheduler Events ---`r`n"
        Set-Content -Path $evidenceFile -Value $header
        
        # Capture Task Scheduler Operational Events
        $tsEvents = Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" -MaxEvents 20 -ErrorAction SilentlyContinue
        if ($tsEvents) {
            $tsEvents | Select-Object TimeCreated, Id, LevelDisplayName, TaskDisplayName, Message | Format-Table -AutoSize | Out-String | Add-Content -Path $evidenceFile
        } else {
            Add-Content -Path $evidenceFile -Value "Could not retrieve Task Scheduler events. (Ensure script is run as Administrator)`r`n"
        }

        # Capture General System Events just in case (e.g. unexpected shutdowns or service stops)
        Add-Content -Path $evidenceFile -Value "`r`n--- Last 20 System Events ---`r`n"
        $sysEvents = Get-WinEvent -LogName "System" -MaxEvents 20 -ErrorAction SilentlyContinue
        if ($sysEvents) {
            $sysEvents | Select-Object TimeCreated, Id, LevelDisplayName, Message | Format-Table -AutoSize | Out-String | Add-Content -Path $evidenceFile
        }

        Write-Host "Evidence successfully saved to: $evidenceFile"
        Write-Host "Stopping monitor to preserve the captured evidence."
        break
    }
    
    # Wait for 2 minutes (120 seconds) before checking again
    Start-Sleep -Seconds 120
}

# Health probe for the DataPipelineScheduler task. Registered by
# setup_monitor_task.ps1 to run every 30 minutes.
#
# Pass -AutoRestart to have it restart a dead scheduler instead of only
# recording the alert:  powershell -File check_task_alive.ps1 -AutoRestart
param(
    [switch]$AutoRestart
)

$taskName = "DataPipelineScheduler"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

# BUGFIX: 'Ready' was treated as healthy. For a continuously-running scheduler
# 'Ready' means the python process has EXITED and the task is merely waiting
# for its next trigger — precisely the silent-death case this probe exists to
# catch. Only 'Running' counts as alive.
$state = if ($null -eq $task) { "Missing" } else { [string]$task.State }
$isAlive = ($null -ne $task) -and ($task.State -eq 'Running')

if (-not $isAlive) {
    $logDir = $PSScriptRoot
    if (-not $logDir) { $logDir = $PWD.Path }

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    # Keep the Desktop alert (visible), and also log next to the repo so the
    # history survives a Desktop cleanup.
    $desktopPath = [Environment]::GetFolderPath("Desktop")
    $targets = @(
        (Join-Path -Path $desktopPath -ChildPath "ALERT_task_missing.txt"),
        (Join-Path -Path $logDir -ChildPath "task_health.log")
    )

    $message = "[$timestamp] Alert: Task '$taskName' is in '$state' state or missing!"

    if ($AutoRestart -and $null -ne $task) {
        try {
            Start-ScheduledTask -TaskName $taskName -ErrorAction Stop
            $message += " Auto-restart issued."
        } catch {
            $message += " Auto-restart FAILED: $($_.Exception.Message)"
        }
    }

    foreach ($t in $targets) {
        try { Add-Content -Path $t -Value $message -ErrorAction Stop } catch { }
    }
}

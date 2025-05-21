$env:DBT_PROFILES_DIR = ".\dbt_profiles"

# Start stopwatch
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Get existing Python process IDs before dbt run
$existingPids = Get-Process -Name "python" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id

# Start dbt run
$dbtProcess = Start-Process -FilePath "dbt" -ArgumentList "run " -PassThru
Start-Sleep -Seconds 2  # Give time for dbt to launch subprocesses

# Get number of logical processors
$cores = (Get-CimInstance -ClassName Win32_ComputerSystem).NumberOfLogicalProcessors

# Detect new Python processes after dbt run starts
$newPythonProcs = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { $existingPids -notcontains $_.Id }

if (-not $newPythonProcs) {
    Write-Host "No new Python processes detected for dbt. Looking for dbt parent process instead."
    # Try to find dbt process itself
    $trackedProcesses = @($dbtProcess)
} else {
    $trackedProcesses = $newPythonProcs
    Write-Host "Found $($trackedProcesses.Count) Python processes to monitor."
}

# Track PIDs and store initial CPU metrics
$trackedPids = $trackedProcesses | Select-Object -ExpandProperty Id
$cpuStartMap = @{}
foreach ($proc in $trackedProcesses) {
    $cpuStartMap[$proc.Id] = $proc.TotalProcessorTime.TotalSeconds
}

# Variables for tracking
$sampleCount = 0
$totalMem = 0
$cpuSamples = @()

# Monitor while any tracked process is still alive
while ($true) {
    $liveProcs = @()
    foreach ($procId in $trackedPids) {
        $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if ($proc) {
            $liveProcs += $proc
        }
    }

    if ($liveProcs.Count -eq 0) {
        if ($dbtProcess.HasExited) {
            break
        } else {
            # DBT might be using a different process we're not tracking
            # Just check if the parent process is still running
            Start-Sleep -Seconds 1
            continue
        }
    }

    # Calculate current CPU usage
    $currentCpuUsed = 0
    foreach ($proc in $liveProcs) {
        $currentCpu = $proc.TotalProcessorTime.TotalSeconds - $cpuStartMap[$proc.Id]
        $currentCpuUsed += $currentCpu
        # Update the base for next calculation
        $cpuStartMap[$proc.Id] = $proc.TotalProcessorTime.TotalSeconds
    }

    # Record CPU sample (adjusted for time period of 1 second)
    $cpuPercent = ($currentCpuUsed / $cores) * 100
    $cpuSamples += $cpuPercent

    # Sum memory
    $memSample = ($liveProcs | Measure-Object -Property WorkingSet64 -Sum).Sum / 1MB
    $totalMem += $memSample
    $sampleCount++

    Start-Sleep -Seconds 1
}

# Wait for dbt to complete if it hasn't already
if (-not $dbtProcess.HasExited) {
    Write-Host "Waiting for dbt process to complete..."
    $dbtProcess.WaitForExit()
}

# Stop stopwatch
$stopwatch.Stop()
$duration = $stopwatch.Elapsed.TotalSeconds

# Final calculations
$avgCpuPercent = if ($cpuSamples.Count -gt 0) {
    [math]::Round(($cpuSamples | Measure-Object -Average).Average, 2)
} else {
    0
}

$avgMem = [math]::Round($totalMem / [math]::Max($sampleCount, 1), 2)

# Output
Write-Host "Total time taken    : $([math]::Round($duration, 2)) seconds"
Write-Host "Average CPU usage   : $avgCpuPercent%"
Write-Host "Average memory usage: $avgMem MB"

"$((Get-Date).ToString("yyyy-MM-dd HH:mm:ss")) - Time: $duration sec | CPU: $avgCpuPercent% | RAM: $avgMem MB" | Out-File -Append "resource_log.txt"
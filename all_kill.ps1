function Kill-ProcessTree([int]$procId) {
    Get-WmiObject Win32_Process -Filter "ParentProcessID=$procId" | ForEach-Object { Kill-ProcessTree($_.ProcessID) }
    Stop-Process -ID $procId -Force
}

$processes = Get-WmiObject Win32_Process | Where-Object { $_.CommandLine -like "*akebono_crown*" }
foreach ($process in $processes) {
    Kill-ProcessTree($process.ProcessID)
}

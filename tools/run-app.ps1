param(
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
$Python = if (Test-Path $VenvPython) { $VenvPython } else { "python" }

function Test-PortFree {
    param([int]$Port)
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), $Port)
    try {
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        try { $listener.Stop() } catch {}
    }
}

$Port = 8080
while ($Port -lt 8090 -and -not (Test-PortFree -Port $Port)) {
    $Port += 1
}
if ($Port -ge 8090) {
    throw "No free local port found in 8080-8089."
}

$Url = "http://127.0.0.1:$Port"
Write-Host "Starting Portfolio Agent on $Url"
if (-not $NoBrowser) {
    Start-Job -ScriptBlock {
        param([string]$TargetUrl)
        Start-Sleep -Seconds 2
        Start-Process $TargetUrl
    } -ArgumentList $Url | Out-Null
    Write-Host "Browser will open automatically. Keep this window open while using the app."
}
& $Python (Join-Path $Root "app.py") --port $Port

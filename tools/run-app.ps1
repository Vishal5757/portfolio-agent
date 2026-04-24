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

Write-Host "Starting Portfolio Agent on http://127.0.0.1:$Port"
& $Python (Join-Path $Root "app.py") --port $Port

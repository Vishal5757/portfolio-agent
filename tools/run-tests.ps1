$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Project venv not found at $python"
}

Push-Location $root
try {
    & $python "tools\full_system_smoke_test.py"
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    & $python "tools\button_contract_test.py"
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

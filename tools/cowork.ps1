param(
    [ValidateSet("add", "read", "open", "tail", "live")]
    [string]$Action = "read",
    [string]$Agent = "Codex",
    [string]$Type = "note",
    [string]$Message = "",
    [string]$Files = "-",
    [string]$Tests = "-",
    [string]$Next = "-",
    [string]$Blockers = "none",
    [int]$Lines = 80,
    [switch]$Follow
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$ChatPath = Join-Path $Root "cowork.md"

if (!(Test-Path -LiteralPath $ChatPath)) {
    @"
# Cowork Chat

## Chat
"@ | Set-Content -LiteralPath $ChatPath -Encoding UTF8
}

function Get-IstStamp {
    return (Get-Date).ToString("yyyy-MM-dd HH:mm") + " IST"
}

switch ($Action) {
    "add" {
        if ([string]::IsNullOrWhiteSpace($Message)) {
            throw "Message is required for add. Example: tools\cowork.ps1 add -Agent Claude -Type claim -Message 'Claiming UI tests.'"
        }
        $entry = @"

### $(Get-IstStamp) | $Agent | $Type
State: $Message
Files: $Files
Tests: $Tests
Next: $Next
Blockers: $Blockers
"@
        Add-Content -LiteralPath $ChatPath -Value $entry -Encoding UTF8
        Write-Output "Appended cowork entry to $ChatPath"
    }
    "read" {
        Get-Content -LiteralPath $ChatPath
    }
    "tail" {
        Write-Host "Watching $ChatPath. Press Ctrl+C to stop." -ForegroundColor Cyan
        if ($Follow) {
            Get-Content -LiteralPath $ChatPath -Tail $Lines -Wait
        } else {
            Get-Content -LiteralPath $ChatPath -Tail $Lines
        }
    }
    "live" {
        Start-Process powershell.exe -ArgumentList @(
            "-NoExit",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            $PSCommandPath,
            "-Action",
            "tail",
            "-Lines",
            [string]$Lines,
            "-Follow"
        )
        Write-Output "Opened live cowork viewer for $ChatPath"
    }
    "open" {
        Start-Process notepad.exe $ChatPath
        Write-Output "Opened $ChatPath in Notepad"
    }
}

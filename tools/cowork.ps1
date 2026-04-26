param(
    [ValidateSet("add", "read", "open", "tail")]
    [string]$Action = "read",
    [string]$Agent = "Codex",
    [string]$Type = "note",
    [string]$Message = "",
    [string]$Files = "-",
    [string]$Tests = "-",
    [string]$Next = "-",
    [string]$Blockers = "none",
    [int]$Lines = 80
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
        Get-Content -LiteralPath $ChatPath -Tail $Lines
    }
    "open" {
        Start-Process notepad.exe $ChatPath
        Write-Output "Opened $ChatPath in Notepad"
    }
}

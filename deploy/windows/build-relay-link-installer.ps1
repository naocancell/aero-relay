[CmdletBinding()]
param(
    [switch]$ValidateOnly,

    [switch]$RequireMediaTools
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$resourceCheck = Join-Path $PSScriptRoot "test-installer-resources.ps1"

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $resourceCheck
if ($LASTEXITCODE -ne 0) {
    throw "Installer resource validation failed."
}

if ($ValidateOnly) {
    Write-Host "Validation completed."
    exit 0
}

if ($RequireMediaTools) {
    foreach ($requiredMediaTool in @(
        (Join-Path $root "deploy\windows\installer-resources\media\ffmpeg\ffmpeg.exe"),
        (Join-Path $root "deploy\windows\installer-resources\media\mediamtx\mediamtx.exe")
    )) {
        if (-not (Test-Path -LiteralPath $requiredMediaTool -PathType Leaf)) {
            throw "Video-enabled installer requires staged media tool: $requiredMediaTool"
        }
    }
}

Push-Location $root
try {
    cargo build --release -p relay-linkd
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }

    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "test-installer-bundle.ps1")
    if ($LASTEXITCODE -ne 0) {
        throw "Installer bundle validation failed."
    }

    Push-Location (Join-Path $root "apps\relay-link-ui")
    try {
        npm.cmd run tauri -- build --bundles nsis
        if ($LASTEXITCODE -ne 0) {
            throw "Tauri NSIS build failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} finally {
    Pop-Location
}

Write-Host "Relay Link NSIS installer build completed."

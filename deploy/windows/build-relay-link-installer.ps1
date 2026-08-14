[CmdletBinding()]
param(
    [switch]$ValidateOnly,

    [switch]$RequireMediaTools,

    [switch]$FetchMediaTools
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$resourceCheck = Join-Path $PSScriptRoot "test-installer-resources.ps1"
$mediaMtxVersionFile = Join-Path $PSScriptRoot "mediamtx-version.txt"
$expectedMediaMtxVersion = (Get-Content -LiteralPath $mediaMtxVersionFile -Raw).Trim()

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $resourceCheck
if ($LASTEXITCODE -ne 0) {
    throw "Installer resource validation failed."
}

if ($ValidateOnly) {
    Write-Host "Validation completed."
    exit 0
}

if ($FetchMediaTools) {
    Write-Host "Fetching media tools..."
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "fetch-media-tools.ps1")
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to fetch media tools."
    }
}

if ($RequireMediaTools) {
    $mediaMtxExecutable = Join-Path $root "deploy\windows\installer-resources\media\mediamtx\mediamtx.exe"
    foreach ($requiredMediaTool in @(
        (Join-Path $root "deploy\windows\installer-resources\media\ffmpeg\ffmpeg.exe"),
        $mediaMtxExecutable
    )) {
        if (-not (Test-Path -LiteralPath $requiredMediaTool -PathType Leaf)) {
            throw "Video-enabled installer requires staged media tool: $requiredMediaTool"
        }
    }
    $versionOutput = & $mediaMtxExecutable --version 2>&1
    $versionExitCode = $LASTEXITCODE
    $versionText = ($versionOutput | Out-String).Trim()
    if ($versionExitCode -ne 0 -or $versionText -notmatch [regex]::Escape($expectedMediaMtxVersion)) {
        throw "Video-enabled installer requires MediaMTX $expectedMediaMtxVersion, got: $versionText"
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

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$required = @(
    "deploy\windows\relay-link-installer-hooks.nsh",
    "deploy\windows\installer-resources\config\config.toml",
    "deploy\windows\installer-resources\config\mediamtx.yml",
    "deploy\windows\installer-resources\media\README.md",
    "deploy\windows\stage-media-tools.ps1"
)

$missing = @(
    $required |
        Where-Object { -not (Test-Path -LiteralPath (Join-Path $root $_) -PathType Leaf) }
)
if ($missing.Count -gt 0) {
    throw "Installer resources are missing: $($missing -join ', ')"
}

${configTemplate} = Get-Content -LiteralPath (Join-Path $root "deploy\windows\installer-resources\config\config.toml") -Raw
if ($configTemplate -notmatch '(?m)^\[relay\]' -or $configTemplate -notmatch '(?m)^\[server\]') {
    throw "Unified installer config must contain both relay and server sections."
}
if ($configTemplate -notmatch '__RELAY_ID__') {
    throw "Relay installer template must contain the __RELAY_ID__ marker."
}

$mediaConfig = Get-Content -LiteralPath (Join-Path $root "deploy\windows\installer-resources\config\mediamtx.yml") -Raw
foreach ($requiredSetting in @("api: true", "apiAddress: 127.0.0.1:9997", "rtmp: true", "rtmpAddress: :1935", "rtsp: true", "rtspAddress: :8554")) {
    if ($mediaConfig -notmatch [regex]::Escape($requiredSetting)) {
        throw "MediaMTX configuration is missing required setting: $requiredSetting"
    }
}

$tauriConfig = Get-Content -LiteralPath (Join-Path $root "apps\relay-link-ui\src-tauri\tauri.conf.json") -Raw | ConvertFrom-Json
$resources = $tauriConfig.bundle.resources
foreach ($resourcePath in @(
    "../../../deploy/windows/installer-resources/config/mediamtx.yml",
    "../../../deploy/windows/installer-resources/media"
)) {
    if ($null -eq $resources.PSObject.Properties[$resourcePath]) {
        throw "Tauri bundle is missing media resource mapping: $resourcePath"
    }
}

Write-Host "Installer resource check passed."

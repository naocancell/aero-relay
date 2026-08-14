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
    "deploy\windows\mediamtx-version.txt",
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
$expectedMediaMtxVersion = (Get-Content -LiteralPath (Join-Path $root "deploy\windows\mediamtx-version.txt") -Raw).Trim()
if ($expectedMediaMtxVersion -notmatch '^v1\.18\.2$') {
    throw "Pinned MediaMTX version must be v1.18.2, got: $expectedMediaMtxVersion"
}
$mediaFetchScript = Get-Content -LiteralPath (Join-Path $root "deploy\windows\fetch-media-tools.ps1") -Raw
if ($mediaFetchScript -notmatch 'mediamtx-version\.txt' -or $mediaFetchScript -notmatch 'releases/download/\$expectedMediaMtxVersion/') {
    throw "MediaMTX download script must use the pinned version file."
}
$stageMediaToolsScript = Get-Content -LiteralPath (Join-Path $root "deploy\windows\stage-media-tools.ps1") -Raw
if ($stageMediaToolsScript -notmatch 'mediamtx-version\.txt' -or $stageMediaToolsScript -notmatch '--version') {
    throw "Media tool staging must validate the pinned MediaMTX version."
}
$buildInstallerScript = Get-Content -LiteralPath (Join-Path $root "deploy\windows\build-relay-link-installer.ps1") -Raw
if ($buildInstallerScript -notmatch 'mediamtx-version\.txt' -or $buildInstallerScript -notmatch '--version') {
    throw "Video-enabled installer builds must validate the pinned MediaMTX version."
}
foreach ($requiredSetting in @("api: true", "apiAddress: 127.0.0.1:9997", "rtmp: true", "rtmpAddress: :1935", "rtsp: true", "rtspAddress: :554")) {
    if ($mediaConfig -notmatch [regex]::Escape($requiredSetting)) {
        throw "MediaMTX configuration is missing required setting: $requiredSetting"
    }
}
foreach ($forbiddenSetting in @("moq:", "protocols:", "encryption:")) {
    if ($mediaConfig -match "(?m)^\s*$([regex]::Escape($forbiddenSetting))") {
        throw "MediaMTX configuration contains a setting incompatible with $expectedMediaMtxVersion`: $forbiddenSetting"
    }
}
foreach ($requiredSetting in @("rtspTransports: [tcp]", 'rtspEncryption: "no"')) {
    if ($mediaConfig -notmatch [regex]::Escape($requiredSetting)) {
        throw "MediaMTX $expectedMediaMtxVersion configuration is missing required setting: $requiredSetting"
    }
}
if ($mediaConfig -notmatch '(?ms)^paths:\s*\r?\n\s+all_others:\s*\r?\n\s+source: publisher\s*\r?\n\s+overridePublisher: false') {
    throw "MediaMTX configuration must reject publisher replacement on the managed path."
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

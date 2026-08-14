[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("relay-link-test-" + [Guid]::NewGuid().ToString("N"))
try {
    New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $tempRoot "config") -Force | Out-Null

    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "..\..\target\release\relay-linkd.exe") `
        -Destination (Join-Path $tempRoot "relay-linkd.exe")
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "installer-resources\config\config.toml") `
        -Destination (Join-Path $tempRoot "config\config.toml.template")
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "installer-resources\config\mediamtx.yml") `
        -Destination (Join-Path $tempRoot "config\mediamtx.yml")

    foreach ($role in @("relay", "server")) {
        $validationOutput = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "install-relay-linkd.ps1") `
            -InstallRoot $tempRoot -ResourceRoot $tempRoot -Role $role -ValidateOnly
        if ($LASTEXITCODE -ne 0) {
            throw "Bundle validation failed for role '$role'."
        }
        $serviceOutput = $validationOutput -join "`n"
        if ($serviceOutput -notmatch [regex]::Escape(" $role")) {
            throw "Service command for role '$role' is missing the role argument."
        }
        if ($serviceOutput -match "--config|data-") {
            throw "Service command for role '$role' still contains a configurable/data-root path."
        }
    }

    $installerScript = Get-Content -LiteralPath (Join-Path $PSScriptRoot "install-relay-linkd.ps1") -Raw
    foreach ($requiredText in @(
        "function Wait-ServiceState",
        "function Wait-ServiceDeleted",
        '"failureflag"',
        "entered STOPPED state while waiting"
    )) {
        if ($installerScript -notmatch [regex]::Escape($requiredText)) {
            throw "Installer script is missing required service lifecycle handling: $requiredText"
        }
    }
    if ($installerScript -notmatch '(?m)Invoke-ScCommand @\("start",\s*\$ServiceName,\s*\$Role\)') {
        throw "Installer must pass the selected role to sc.exe start so the Windows service callback receives it."
    }

    $unifiedConfig = Get-Content -LiteralPath (Join-Path $tempRoot "config\config.toml") -Raw
    if ($unifiedConfig -match "__RELAY_ID__") {
        throw "Relay ID marker was not replaced."
    }
    if ($unifiedConfig -notmatch "(?m)^\[relay\]" -or $unifiedConfig -notmatch "(?m)^\[server\]") {
        throw "Unified config is missing a relay or server section."
    }
    $mediaConfig = Get-Content -LiteralPath (Join-Path $tempRoot "config\mediamtx.yml") -Raw
    if ($mediaConfig -notmatch "(?m)^apiAddress: 127\.0\.0\.1:9997$" -or $mediaConfig -notmatch "(?m)^rtmpAddress: :1935$") {
        throw "MediaMTX bundle configuration is missing the managed API or RTMP listener."
    }
    $relayMatch = [regex]::Match($unifiedConfig, '(?ms)^\[relay\]\s*id = "([^\"]+)"')
    if (-not $relayMatch.Success -or $relayMatch.Groups[1].Value -notmatch '^r-[0-9a-f]{8}$') {
        throw "Installer must generate an 8-hex short relay ID."
    }
    $relayId = $relayMatch.Groups[1].Value
    foreach ($linkId in @('s1', 'u1')) {
        if ($relayId.Length + $linkId.Length -gt 22) {
            throw "Installer-generated path '$relayId/$linkId' exceeds the heartbeat metadata limit."
        }
    }
    Write-Host "Installer bundle validation passed for server and relay roles."
} finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}

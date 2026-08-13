[CmdletBinding()]
param(
    [string]$InstallRoot,

    [ValidateSet("relay", "server")]
    [string]$Role,

    [string]$ResourceRoot,

    [string]$ServiceName = "RelayLinkD",

    [switch]$UninstallService,

    [switch]$ValidateOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:LastScOutput = ""

function Invoke-ScCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & "$env:SystemRoot\System32\sc.exe" @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    $script:LastScOutput = (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine).Trim()
    return $exitCode
}

function Assert-ScSuccess {
    param(
        [Parameter(Mandatory = $true)]
        [int]$ExitCode,

        [Parameter(Mandatory = $true)]
        [string]$Operation
    )

    if ($ExitCode -ne 0) {
        $details = if ([string]::IsNullOrWhiteSpace($script:LastScOutput)) {
            "no output"
        } else {
            $script:LastScOutput
        }
        throw ("sc.exe {0} failed with exit code {1}. Output: {2}" -f $Operation, $ExitCode, $details)
    }
}

function Get-ServiceState {
    $output = & "$env:SystemRoot\System32\sc.exe" query $ServiceName 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        return $null
    }

    $text = (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
    $match = [regex]::Match($text, "STATE\s*:\s*\d+\s+([A-Z_]+)")
    if (-not $match.Success) {
        throw "Unable to read the state of service '$ServiceName'. Output: $text"
    }
    return $match.Groups[1].Value
}

function Wait-ServiceState {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExpectedState,

        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $state = Get-ServiceState
        if ($state -eq $ExpectedState) {
            return
        }
        if ($ExpectedState -ne "STOPPED" -and $state -eq "STOPPED") {
            throw "Service '$ServiceName' entered STOPPED state while waiting for '$ExpectedState'."
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)

    $lastState = Get-ServiceState
    throw "Timed out waiting for service '$ServiceName' to reach '$ExpectedState'; current state: $lastState."
}

function Wait-ServiceDeleted {
    param(
        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if ($null -eq (Get-ServiceState)) {
            return
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for service '$ServiceName' to be deleted."
}

function Remove-ServiceRegistration {
    $state = Get-ServiceState
    if ($null -eq $state) {
        return
    }

    if ($state -ne "STOPPED") {
        $stopExitCode = Invoke-ScCommand @("stop", $ServiceName)
        if ($stopExitCode -ne 0 -and $stopExitCode -ne 1062) {
            Assert-ScSuccess -ExitCode $stopExitCode -Operation "stop $ServiceName"
        }
        Wait-ServiceState -ExpectedState "STOPPED"
    }

    $deleteExitCode = Invoke-ScCommand @("delete", $ServiceName)
    if ($deleteExitCode -ne 0 -and $deleteExitCode -ne 1072) {
        Assert-ScSuccess -ExitCode $deleteExitCode -Operation "delete $ServiceName"
    }
    Wait-ServiceDeleted
}

function Assert-File {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required file not found: $Path"
    }
}

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $utf8NoBom = New-Object -TypeName System.Text.UTF8Encoding -ArgumentList $false
    [IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

function Get-ServiceBinaryPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Backend,

        [Parameter(Mandatory = $true)]
        [ValidateSet("relay", "server")]
        [string]$ServiceRole
    )

    return '"' + $Backend + '" ' + $ServiceRole
}

if ($UninstallService) {
    Remove-ServiceRegistration
    Write-Host "Removed service '$ServiceName'."
    exit 0
}

if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
    throw "InstallRoot is required."
}
if ([string]::IsNullOrWhiteSpace($Role)) {
    throw "Role is required."
}

$resolvedRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$resolvedBackend = $null
$resolvedConfig = [IO.Path]::GetFullPath([IO.Path]::Combine($InstallRoot, "config", "config.toml"))

if (-not [string]::IsNullOrWhiteSpace($ResourceRoot)) {
    $resolvedResources = [IO.Path]::GetFullPath($ResourceRoot)
    $resolvedBackend = [IO.Path]::GetFullPath([IO.Path]::Combine($resolvedResources, "relay-linkd.exe"))
    $template = [IO.Path]::GetFullPath([IO.Path]::Combine($resolvedResources, "config", "config.toml.template"))
    if (-not (Test-Path -LiteralPath $template -PathType Leaf)) {
        $template = [IO.Path]::GetFullPath([IO.Path]::Combine($resolvedResources, "config", "config.toml"))
    }

    Assert-File $resolvedBackend
    Assert-File $template

    New-Item -ItemType Directory -Path (Split-Path -Parent $resolvedConfig) -Force | Out-Null

    if (-not (Test-Path -LiteralPath $resolvedConfig -PathType Leaf)) {
        $configContent = Get-Content -LiteralPath $template -Raw
        $machineName = [Environment]::MachineName
        $safeMachineName = $machineName -replace "[^A-Za-z0-9_.-]", "-"
        if ([string]::IsNullOrWhiteSpace($safeMachineName)) {
            $safeMachineName = "unknown"
        }
        $machineBytes = [Text.Encoding]::UTF8.GetBytes($safeMachineName.ToLowerInvariant())
        $sha256 = [Security.Cryptography.SHA256]::Create()
        try {
            $machineHash = [BitConverter]::ToString($sha256.ComputeHash($machineBytes)).Replace("-", "").ToLowerInvariant()
        } finally {
            $sha256.Dispose()
        }
        $relayId = "r-$($machineHash.Substring(0, 8))"
        $configContent = $configContent.Replace("__RELAY_ID__", $relayId)

        Write-Utf8NoBom -Path $resolvedConfig -Content $configContent
    } else {
        Write-Host "Config file already exists at '$resolvedConfig', skipping generation."
    }
} else {
    $resolvedBackend = [IO.Path]::GetFullPath([IO.Path]::Combine($InstallRoot, "relay-linkd.exe"))
    Assert-File $resolvedBackend
    Assert-File $resolvedConfig
}

if (-not $resolvedBackend.StartsWith($resolvedRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "relay-linkd.exe must be inside InstallRoot: $resolvedRoot"
}

$binaryPath = Get-ServiceBinaryPath -Backend $resolvedBackend -ServiceRole $Role
if ($ValidateOnly) {
    Write-Host "Validated $Role bundle configuration at '$resolvedConfig'."
    Write-Host "Service command: $binaryPath"
    exit 0
}

Remove-ServiceRegistration

$createExitCode = Invoke-ScCommand @(
    "create",
    $ServiceName,
    "binPath=",
    $binaryPath,
    "start=",
    "auto",
    "DisplayName=",
    "Relay Link backend ($Role)"
)
if ($createExitCode -ne 0) {
    Assert-ScSuccess -ExitCode $createExitCode -Operation "create $ServiceName"
}

$descriptionExitCode = Invoke-ScCommand @("description", $ServiceName, "Headless relay-link communication backend ($Role)")
Assert-ScSuccess -ExitCode $descriptionExitCode -Operation "description $ServiceName"

$failureExitCode = Invoke-ScCommand @("failure", $ServiceName, "reset=", "86400", "actions=", "restart/5000/restart/5000/restart/5000")
Assert-ScSuccess -ExitCode $failureExitCode -Operation "failure $ServiceName"

$failureFlagExitCode = Invoke-ScCommand @("failureflag", $ServiceName, "1")
Assert-ScSuccess -ExitCode $failureFlagExitCode -Operation "failureflag $ServiceName"

$startExitCode = Invoke-ScCommand @("start", $ServiceName)
if ($startExitCode -ne 0) {
    Assert-ScSuccess -ExitCode $startExitCode -Operation "start $ServiceName"
}
Wait-ServiceState -ExpectedState "RUNNING"

Write-Host "Installed and started $ServiceName for role '$Role'."

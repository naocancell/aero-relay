[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$FfmpegExecutable,

    [Parameter(Mandatory = $true)]
    [string]$MediaMtxExecutable
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-ExecutableFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $resolved = [IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
        throw "$Label was not found: $resolved"
    }
    return $resolved
}

function Copy-ToolDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceExecutable,

        [Parameter(Mandatory = $true)]
        [string]$DestinationDirectory
    )

    New-Item -ItemType Directory -Force -Path $DestinationDirectory | Out-Null
    Copy-Item -LiteralPath $SourceExecutable -Destination (Join-Path $DestinationDirectory (Split-Path -Leaf $SourceExecutable)) -Force
    $sourceDirectory = Split-Path -Parent $SourceExecutable
    foreach ($dependency in Get-ChildItem -LiteralPath $sourceDirectory -File -Filter "*.dll") {
        Copy-Item -LiteralPath $dependency.FullName -Destination $DestinationDirectory -Force
    }
}

$ffmpeg = Resolve-ExecutableFile -Path $FfmpegExecutable -Label "FFmpeg executable"
$mediamtx = Resolve-ExecutableFile -Path $MediaMtxExecutable -Label "MediaMTX executable"
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$mediaRoot = Join-Path $root "deploy\windows\installer-resources\media"
$ffmpegTarget = Join-Path $mediaRoot "ffmpeg"
$mediamtxTarget = Join-Path $mediaRoot "mediamtx"

Copy-ToolDirectory -SourceExecutable $ffmpeg -DestinationDirectory $ffmpegTarget
Copy-ToolDirectory -SourceExecutable $mediamtx -DestinationDirectory $mediamtxTarget

Write-Host "Staged FFmpeg and sibling dependencies at $ffmpegTarget"
Write-Host "Staged MediaMTX and sibling dependencies at $mediamtxTarget"

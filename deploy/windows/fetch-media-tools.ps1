[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$mediaRoot = Join-Path $root "deploy\windows\installer-resources\media"
$ffmpegTarget = Join-Path $mediaRoot "ffmpeg"
$mediamtxTarget = Join-Path $mediaRoot "mediamtx"
$mediaMtxVersionFile = Join-Path $PSScriptRoot "mediamtx-version.txt"
$expectedMediaMtxVersion = (Get-Content -LiteralPath $mediaMtxVersionFile -Raw).Trim()
if ($expectedMediaMtxVersion -notmatch '^v\d+\.\d+\.\d+$') {
    throw "Invalid pinned MediaMTX version: $expectedMediaMtxVersion"
}

# Create directories if they don't exist
New-Item -ItemType Directory -Force -Path $ffmpegTarget | Out-Null
New-Item -ItemType Directory -Force -Path $mediamtxTarget | Out-Null

$tempDir = Join-Path $env:TEMP "aero-relay-media-tools-$([guid]::NewGuid().ToString())"
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

try {
    Write-Host "Downloading FFmpeg..."
    $ffmpegUrl = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
    $ffmpegZip = Join-Path $tempDir "ffmpeg.zip"
    Invoke-WebRequest -Uri $ffmpegUrl -OutFile $ffmpegZip
    
    Write-Host "Extracting FFmpeg..."
    Expand-Archive -Path $ffmpegZip -DestinationPath $tempDir -Force
    $extractedFfmpeg = Get-ChildItem -Path $tempDir -Directory -Filter "ffmpeg-*" | Select-Object -First 1
    $ffmpegExe = Join-Path $extractedFfmpeg.FullName "bin\ffmpeg.exe"
    if (Test-Path $ffmpegExe) {
        Copy-Item -Path $ffmpegExe -Destination $ffmpegTarget -Force
        Write-Host "FFmpeg successfully staged at $ffmpegTarget"
    } else {
        throw "Could not find ffmpeg.exe in downloaded archive."
    }

    Write-Host "Downloading MediaMTX..."
    $mediamtxUrl = "https://github.com/bluenviron/mediamtx/releases/download/$expectedMediaMtxVersion/mediamtx_${expectedMediaMtxVersion}_windows_amd64.zip"
    $mediamtxZip = Join-Path $tempDir "mediamtx.zip"
    Invoke-WebRequest -Uri $mediamtxUrl -OutFile $mediamtxZip

    Write-Host "Extracting MediaMTX..."
    Expand-Archive -Path $mediamtxZip -DestinationPath (Join-Path $tempDir "mediamtx") -Force
    $mediamtxExe = Join-Path $tempDir "mediamtx\mediamtx.exe"
    if (Test-Path $mediamtxExe) {
        Copy-Item -Path $mediamtxExe -Destination $mediamtxTarget -Force
        $versionOutput = & $mediamtxExe --version 2>&1
        $versionExitCode = $LASTEXITCODE
        $versionText = ($versionOutput | Out-String).Trim()
        if ($versionExitCode -ne 0 -or $versionText -notmatch [regex]::Escape($expectedMediaMtxVersion)) {
            throw "Downloaded MediaMTX version mismatch. Expected $expectedMediaMtxVersion, got: $versionText"
        }
        Write-Host "MediaMTX successfully staged at $mediamtxTarget"
    } else {
        throw "Could not find mediamtx.exe in downloaded archive."
    }
} finally {
    if (Test-Path $tempDir) {
        Remove-Item -Path $tempDir -Recurse -Force
    }
}

Write-Host "Media tools fetching completed successfully."

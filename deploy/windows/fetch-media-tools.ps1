[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\.."))
$mediaRoot = Join-Path $root "deploy\windows\installer-resources\media"
$ffmpegTarget = Join-Path $mediaRoot "ffmpeg"
$mediamtxTarget = Join-Path $mediaRoot "mediamtx"

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
    # Using a fixed version to ensure stability
    $mediamtxUrl = "https://github.com/bluenviron/mediamtx/releases/download/v1.9.3/mediamtx_v1.9.3_windows_amd64.zip"
    $mediamtxZip = Join-Path $tempDir "mediamtx.zip"
    Invoke-WebRequest -Uri $mediamtxUrl -OutFile $mediamtxZip

    Write-Host "Extracting MediaMTX..."
    Expand-Archive -Path $mediamtxZip -DestinationPath (Join-Path $tempDir "mediamtx") -Force
    $mediamtxExe = Join-Path $tempDir "mediamtx\mediamtx.exe"
    if (Test-Path $mediamtxExe) {
        Copy-Item -Path $mediamtxExe -Destination $mediamtxTarget -Force
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

[CmdletBinding()]
param(
    [switch]$SkipMediaCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "      Relay Link: Windows 安装包一键编译与打包脚本        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$missing = @()
if (-not (Get-Command "cargo" -ErrorAction SilentlyContinue)) { $missing += "cargo (Rust)" }
if (-not (Get-Command "npm" -ErrorAction SilentlyContinue)) { $missing += "npm (Node.js)" }

if ($missing.Count -gt 0) {
    throw "缺少必要构建工具: $($missing -join ', ')，请先安装 Rust 与 Node.js 环境。"
}

$mediaBinWin = Join-Path $root "deploy\media-bin\windows-amd64"
$mediaTargetRoot = Join-Path $root "deploy\windows\installer-resources\media"

$ffmpegTarget = Join-Path $mediaTargetRoot "ffmpeg\ffmpeg.exe"
$mediamtxTarget = Join-Path $mediaTargetRoot "mediamtx\mediamtx.exe"

if (-not (Test-Path $ffmpegTarget) -or -not (Test-Path $mediamtxTarget)) {
    Write-Host "[+] 正在从 deploy\media-bin\windows-amd64 载入预置媒体工具..." -ForegroundColor Yellow
    $ffmpegSource = Join-Path $mediaBinWin "ffmpeg.exe"
    $mediamtxSource = Join-Path $mediaBinWin "mediamtx.exe"

    if ((Test-Path $ffmpegSource) -and (Test-Path $mediamtxSource)) {
        New-Item -ItemType Directory -Force -Path (Split-Path $ffmpegTarget) | Out-Null
        New-Item -ItemType Directory -Force -Path (Split-Path $mediamtxTarget) | Out-Null
        Copy-Item -Path $ffmpegSource -Destination $ffmpegTarget -Force
        Copy-Item -Path $mediamtxSource -Destination $mediamtxTarget -Force
        Write-Host "[+] 媒体工具加载完成。" -ForegroundColor Green
    } else {
        Write-Host "[!] deploy\media-bin\windows-amd64 中缺少文件，尝试自动拉取..." -ForegroundColor Yellow
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root "deploy\windows\fetch-media-tools.ps1")
    }
}

Write-Host ""
Write-Host "[+] 开始构建 Windows NSIS 安装包..." -ForegroundColor Green
$installerScript = Join-Path $root "deploy\windows\build-relay-link-installer.ps1"
$installerParams = @("-RequireMediaTools")
if ($SkipMediaCheck) { $installerParams = @() }

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installerScript @installerParams
if ($LASTEXITCODE -ne 0) {
    throw "Windows 安装包打包失败！"
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "              Windows 安装包打包成功！                    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
$outputExe = Join-Path $root "apps\relay-link-ui\src-tauri\target\release\bundle\nsis"
if (Test-Path $outputExe) {
    Get-ChildItem -Path $outputExe -Filter "*.exe" | ForEach-Object {
        Write-Host "产物文件: $($_.FullName)" -ForegroundColor Green
        Write-Host "文件大小: $([math]::Round($_.Length / 1MB, 2)) MB" -ForegroundColor Green
    }
}
Write-Host "==========================================================" -ForegroundColor Cyan
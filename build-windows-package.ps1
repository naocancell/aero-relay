[CmdletBinding()]
param(
    [switch]$SkipMediaCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "      Relay Link: Windows 纯Web服务一键编译与打包脚本      " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$missing = @()
if (-not (Get-Command "cargo" -ErrorAction SilentlyContinue)) { $missing += "cargo (Rust)" }
if (-not (Get-Command "npm" -ErrorAction SilentlyContinue)) { $missing += "npm (Node.js)" }

if ($missing.Count -gt 0) {
    throw "缺少必要构建工具: $($missing -join ', ')，请先安装 Rust 与 Node.js 环境。"
}

# 1. 编译前端 Web SPA
Write-Host ""
Write-Host "[1/3] 正在编译前端 Web SPA 管理界面..." -ForegroundColor Yellow
Push-Location (Join-Path $root "apps\relay-link-ui")
try {
    if (-not (Test-Path "node_modules")) {
        npm.cmd install
    }
    npm.cmd run build
    if ($LASTEXITCODE -ne 0) {
        throw "前端 Web 构建失败！"
    }
} finally {
    Pop-Location
}
Write-Host "[+] 前端 Web 资源编译完成。" -ForegroundColor Green

# 2. 检查媒体转发工具
Write-Host ""
Write-Host "[2/3] 检查预置媒体工具 (FFmpeg / MediaMTX)..." -ForegroundColor Yellow
$mediaBinWin = Join-Path $root "deploy\media-bin\windows-amd64"
$ffmpegSource = Join-Path $mediaBinWin "ffmpeg.exe"
$mediamtxSource = Join-Path $mediaBinWin "mediamtx.exe"

$hasMedia = (Test-Path $ffmpegSource) -and (Test-Path $mediamtxSource)
if (-not $hasMedia -and -not $SkipMediaCheck) {
    Write-Host "[!] deploy\media-bin\windows-amd64 中未找到完整媒体工具，尝试拉取..." -ForegroundColor Yellow
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root "deploy\windows\fetch-media-tools.ps1")
    $hasMedia = (Test-Path $ffmpegSource) -and (Test-Path $mediamtxSource)
}

# 3. 编译二进制
Write-Host ""
Write-Host "[3/3] 正在编译 relay-linkd 服务端二进制 (嵌入Web前端)..." -ForegroundColor Yellow
Push-Location $root
try {
    cargo build --release -p relay-linkd
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build --release 失败！"
    }
} finally {
    Pop-Location
}

# 4. 组装发布包目录
$distWindows = Join-Path $root "dist-windows"
$bundleDir = Join-Path $distWindows "relay-link-windows-amd64"
if (Test-Path $bundleDir) {
    Remove-Item -Path $bundleDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $bundleDir | Out-Null

$configTargetDir = Join-Path $bundleDir "config"
New-Item -ItemType Directory -Force -Path $configTargetDir | Out-Null
Copy-Item (Join-Path $root "config\config.example.toml") (Join-Path $configTargetDir "config.toml") -Force
if (Test-Path (Join-Path $root "deploy\windows\installer-resources\config\mediamtx.yml")) {
    Copy-Item (Join-Path $root "deploy\windows\installer-resources\config\mediamtx.yml") (Join-Path $configTargetDir "mediamtx.yml") -Force
} elseif (Test-Path (Join-Path $root "config\mediamtx.yml")) {
    Copy-Item (Join-Path $root "config\mediamtx.yml") (Join-Path $configTargetDir "mediamtx.yml") -Force
}

$exeSource = Join-Path $root "target\release\relay-linkd.exe"
Copy-Item $exeSource (Join-Path $bundleDir "relay-linkd.exe") -Force

if ($hasMedia) {
    $mediaTargetDir = Join-Path $bundleDir "media"
    New-Item -ItemType Directory -Force -Path $mediaTargetDir | Out-Null
    Copy-Item $ffmpegSource (Join-Path $mediaTargetDir "ffmpeg.exe") -Force
    Copy-Item $mediamtxSource (Join-Path $mediaTargetDir "mediamtx.exe") -Force
    Write-Host "[+] 媒体转发工具已打包入 media/ 目录。" -ForegroundColor Green
}

# 创建启动批处理
Set-Content -Path (Join-Path $bundleDir "start-server.bat") -Value "@echo off`r`ncd /d `"%~dp0`"`r`necho Starting Relay Link Server mode...`r`nstart http://localhost:7480`r`nrelay-linkd.exe server`r`npause"
Set-Content -Path (Join-Path $bundleDir "start-relay.bat") -Value "@echo off`r`ncd /d `"%~dp0`"`r`necho Starting Relay Link Relay mode...`r`nstart http://localhost:7480`r`nrelay-linkd.exe relay`r`npause"

# 5. 打包为 zip
$zipPath = Join-Path $distWindows "relay-link-windows-amd64.zip"
if (Test-Path $zipPath) {
    Remove-Item -Path $zipPath -Force
}
Compress-Archive -Path "$bundleDir\*" -DestinationPath $zipPath -Force

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "            Windows 独立免安装包打包成功！                " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "产物文件: $zipPath" -ForegroundColor Green
Write-Host "解压目录: $bundleDir" -ForegroundColor Green
Write-Host "文件大小: $([math]::Round((Get-Item $zipPath).Length / 1MB, 2)) MB" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "使用方法："
Write-Host "1. 解压 relay-link-windows-amd64.zip 到任意目录"
Write-Host "2. 双击 start-server.bat (地面站服务器) 或 start-relay.bat (中继机)"
Write-Host "3. 浏览器自动打开 http://localhost:7480 进行图形化配置"
Write-Host "==========================================================" -ForegroundColor Cyan


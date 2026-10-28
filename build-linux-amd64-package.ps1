[CmdletBinding()]
param(
    [switch]$Full,
    [string]$DistroName = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   Relay Link: Linux (amd64) deb 一键打包脚本 (WSL)        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

if (-not (Get-Command "wsl" -ErrorAction SilentlyContinue)) {
    throw "系统中未找到 wsl 命令。在 Windows 上打包 Linux 安装包需要 WSL 2 环境支持。"
}

$drive = $root.Substring(0, 1).ToLower()
$pathWithoutDrive = $root.Substring(2).Replace('\', '/')
$wslPath = "/mnt/$drive$pathWithoutDrive"

Write-Host "[+] 项目工作目录 (WSL): $wslPath" -ForegroundColor Gray

$wslArgs = @()
if ($DistroName) {
    $wslArgs += @("-d", $DistroName)
}

$scriptToRun = if ($Full) {
    "deploy/linux/build-all-on-device.sh"
} else {
    "deploy/linux/build-relay-link-deb.sh"
}

Write-Host "[+] 正在调用 WSL 环境执行构建打包: $scriptToRun ..." -ForegroundColor Yellow
$wslArgs += @("bash", "-lc", "cd '$wslPath' && bash '$scriptToRun'")

& wsl.exe @wslArgs
if ($LASTEXITCODE -ne 0) {
    throw "WSL 打包命令执行失败 (退出代码: $LASTEXITCODE)"
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "          Linux (amd64) deb 安装包打包完成！              " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
$distLinux = Join-Path $root "dist-linux"
if (Test-Path $distLinux) {
    Get-ChildItem -Path $distLinux -Filter "*amd64*.deb" | ForEach-Object {
        Write-Host "产物文件: $($_.FullName)" -ForegroundColor Green
        Write-Host "文件大小: $([math]::Round($_.Length / 1MB, 2)) MB" -ForegroundColor Green
        Write-Host "更新时间: $($_.LastWriteTime)" -ForegroundColor Gray
    }
}
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "在 Ubuntu/Debian amd64 服务器上安装命令："
Write-Host "  sudo apt install ./dist-linux/relay-linkd_0.1.0_amd64.deb" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan
<#
.SYNOPSIS
    一键打包适用于 ARM 开发板的源码及预编译资源包。
.DESCRIPTION
    精简打包开发板编译打包所需的所有组件（源码、配置、deploy 脚本、ARM64 预置 ffmpeg/mediamtx），
    自动剔除 target/ 编译缓存、.git/、非 ARM 的二进制等冗余文件，
    输出体积仅约 51MB 的干净部署包，便于快速传输到开发板上。
#>

[CmdletBinding()]
param(
    [string]$OutputPath = "",
    [switch]$IncludeNodeModules
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = $PSScriptRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "      Relay Link: ARM 开发板工程资源一键打包工具          " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. 检查必备的 ARM64 媒体工具是否存在
$armMediaDir = Join-Path $root "deploy\media-bin\linux-arm64"
$ffmpegArm = Join-Path $armMediaDir "ffmpeg"
$mediamtxArm = Join-Path $armMediaDir "mediamtx"

if (-not (Test-Path $ffmpegArm) -or -not (Test-Path $mediamtxArm)) {
    throw "缺少 ARM64 预编译媒体工具 ($armMediaDir)！请确保已放入 ARM 版 ffmpeg 与 mediamtx。"
}

# 2. 确定输出路径
$distDir = Join-Path $root "dist-arm"
New-Item -ItemType Directory -Force -Path $distDir | Out-Null

$archiveName = "aero-relay-arm64-deploy.tar.gz"
$finalArchivePath = if ($OutputPath) { $OutputPath } else { Join-Path $distDir $archiveName }

Write-Host "[+] 输出目标文件: $finalArchivePath" -ForegroundColor Yellow

# 3. 准备临时打包目录，确保结构绝对干净
$tempStage = Join-Path $env:TEMP "aero-relay-arm-stage-$([guid]::NewGuid().ToString('N'))"
$projectStage = Join-Path $tempStage "aero-relay"
New-Item -ItemType Directory -Force -Path $projectStage | Out-Null

try {
    Write-Host "[+] 正在收集开发板所需源码与依赖资源..." -ForegroundColor Gray

    # 复制顶层文件
    foreach ($file in @("Cargo.toml", "Cargo.lock", "README.md", "setup-service-role.sh", "start-ui.sh")) {
        $src = Join-Path $root $file
        if (Test-Path $src) {
            Copy-Item -Path $src -Destination (Join-Path $projectStage $file) -Force
        }
    }

    # 复制 crates 和 config 目录
    Copy-Item -Path (Join-Path $root "crates") -Destination (Join-Path $projectStage "crates") -Recurse -Force
    Copy-Item -Path (Join-Path $root "config") -Destination (Join-Path $projectStage "config") -Recurse -Force

    # 复制 apps 目录（排除 target 和 node_modules）
    $appsDest = Join-Path $projectStage "apps"
    New-Item -ItemType Directory -Force -Path $appsDest | Out-Null
    Copy-Item -Path (Join-Path $root "apps\relay-linkd") -Destination (Join-Path $appsDest "relay-linkd") -Recurse -Force
    
    $uiDest = Join-Path $appsDest "relay-link-ui"
    New-Item -ItemType Directory -Force -Path $uiDest | Out-Null
    foreach ($item in (Get-ChildItem -Path (Join-Path $root "apps\relay-link-ui") -Force)) {
        if ($item.Name -eq "node_modules" -and -not $IncludeNodeModules) { continue }
        if ($item.Name -eq "target") { continue }
        Copy-Item -Path $item.FullName -Destination (Join-Path $uiDest $item.Name) -Recurse -Force
    }

    # 复制 deploy 目录（保留 linux 脚本、mediamtx.yml 配置模板和仅针对 linux-arm64 的预编译工具）
    $deployDest = Join-Path $projectStage "deploy"
    New-Item -ItemType Directory -Force -Path $deployDest | Out-Null
    Copy-Item -Path (Join-Path $root "deploy\linux") -Destination (Join-Path $deployDest "linux") -Recurse -Force

    $winCfgDest = Join-Path $deployDest "windows\installer-resources\config"
    New-Item -ItemType Directory -Force -Path $winCfgDest | Out-Null
    Copy-Item -Path (Join-Path $root "deploy\windows\installer-resources\config\mediamtx.yml") -Destination (Join-Path $winCfgDest "mediamtx.yml") -Force

    $mediaBinDest = Join-Path $deployDest "media-bin\linux-arm64"
    New-Item -ItemType Directory -Force -Path $mediaBinDest | Out-Null
    Copy-Item -Path $ffmpegArm -Destination (Join-Path $mediaBinDest "ffmpeg") -Force
    Copy-Item -Path $mediamtxArm -Destination (Join-Path $mediaBinDest "mediamtx") -Force

    # 4. 执行高压缩打包
    Write-Host "[+] 正在生成 .tar.gz 压缩包..." -ForegroundColor Yellow
    if (Test-Path $finalArchivePath) {
        Remove-Item -Path $finalArchivePath -Force
    }

    Push-Location $tempStage
    try {
        & tar.exe -czf $finalArchivePath aero-relay
        if ($LASTEXITCODE -ne 0) {
            throw "tar 压缩失败，退出代码: $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }

} finally {
    if (Test-Path $tempStage) {
        Remove-Item -Path $tempStage -Recurse -Force
    }
}

$fileItem = Get-Item -LiteralPath $finalArchivePath
$sizeMB = [math]::Round($fileItem.Length / 1MB, 2)

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host "           ARM 开发板资源包打包完成！                     " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
Write-Host "生成文件: $finalArchivePath" -ForegroundColor Green
Write-Host "文件大小: $sizeMB MB (已精简剔除无用缓存，包含 ARM64 媒体工具)" -ForegroundColor Green
Write-Host ""
Write-Host "使用指引：" -ForegroundColor Cyan
Write-Host "1. 上传至 ARM 开发板 (通过 SCP 或 U盘)：" -ForegroundColor White
Write-Host "   scp `"$finalArchivePath`" user@<开发板IP>:~/ " -ForegroundColor Yellow
Write-Host "2. 在开发板上解压：" -ForegroundColor White
Write-Host "   tar -xzf $(Split-Path $finalArchivePath -Leaf) ; cd aero-relay" -ForegroundColor Yellow
Write-Host "3. 在开发板上一键编译并打包 deb：" -ForegroundColor White
Write-Host "   bash deploy/linux/build-all-on-device.sh" -ForegroundColor Yellow
Write-Host "4. 在开发板上一键安装并设置开机自启：" -ForegroundColor White
Write-Host "   sudo apt install ./dist-linux/relay-linkd_0.1.0_arm64.deb" -ForegroundColor Yellow
Write-Host "   sudo ./setup-service-role.sh" -ForegroundColor Yellow
Write-Host "5. 浏览器直接访问 Web 管理界面：" -ForegroundColor White
Write-Host "   http://<开发板IP>:7480" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Green
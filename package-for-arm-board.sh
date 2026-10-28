#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
dist_dir="$repo_root/dist-arm"
mkdir -p "$dist_dir"

archive_name="aero-relay-arm64-deploy.tar.gz"
output_path="${1:-$dist_dir/$archive_name}"

echo "=========================================================="
echo "      Relay Link: ARM 开发板工程资源一键打包工具          "
echo "=========================================================="

arm_media_dir="$repo_root/deploy/media-bin/linux-arm64"
if [[ ! -f "$arm_media_dir/ffmpeg" || ! -f "$arm_media_dir/mediamtx" ]]; then
  echo "[-] 缺少 ARM64 预编译媒体工具 ($arm_media_dir)！" >&2
  exit 1
fi

echo "[+] 输出目标: $output_path"

temp_stage="$(mktemp -d "/tmp/aero-relay-arm.XXXXXX")"
trap 'rm -rf -- "$temp_stage"' EXIT

project_stage="$temp_stage/aero-relay"
mkdir -p "$project_stage"

echo "[+] 正在收集开发板所需源码与依赖资源..."

# 复制顶层文件
for f in Cargo.toml Cargo.lock README.md setup-service-role.sh start-ui.sh; do
  if [[ -f "$repo_root/$f" ]]; then
    cp -f "$repo_root/$f" "$project_stage/"
  fi
done

# 复制 crates 与 config
cp -rf "$repo_root/crates" "$project_stage/"
cp -rf "$repo_root/config" "$project_stage/"

# 复制 apps (排除 target 与 node_modules)
mkdir -p "$project_stage/apps"
cp -rf "$repo_root/apps/relay-linkd" "$project_stage/apps/"

mkdir -p "$project_stage/apps/relay-link-ui"
for item in "$repo_root/apps/relay-link-ui"/*; do
  name="$(basename "$item")"
  if [[ "$name" != "node_modules" && "$name" != "target" ]]; then
    cp -rf "$item" "$project_stage/apps/relay-link-ui/"
  fi
done

# 复制 deploy (保留 linux 脚本、mediamtx.yml 配置模板与针对 arm64 的媒体工具)
mkdir -p "$project_stage/deploy"
cp -rf "$repo_root/deploy/linux" "$project_stage/deploy/"

mkdir -p "$project_stage/deploy/windows/installer-resources/config"
cp -f "$repo_root/deploy/windows/installer-resources/config/mediamtx.yml" "$project_stage/deploy/windows/installer-resources/config/"

mkdir -p "$project_stage/deploy/media-bin/linux-arm64"
cp -f "$arm_media_dir/ffmpeg" "$project_stage/deploy/media-bin/linux-arm64/"
cp -f "$arm_media_dir/mediamtx" "$project_stage/deploy/media-bin/linux-arm64/"

echo "[+] 正在生成 .tar.gz 压缩包..."
rm -f "$output_path"
tar -czf "$output_path" -C "$temp_stage" aero-relay

size_mb="$(du -h "$output_path" | cut -f1)"

echo ""
echo "=========================================================="
echo "           ARM 开发板资源包打包完成！                     "
echo "=========================================================="
echo "生成文件: $output_path"
echo "文件大小: $size_mb (已精简剔除无用缓存，包含 ARM64 媒体工具)"
echo ""
echo "使用指引："
echo "1. 上传至 ARM 开发板 (通过 SCP 或 U盘)："
echo "   scp \"$output_path\" user@<开发板IP>:~/ "
echo "2. 在开发板上解压："
echo "   tar -xzf $(basename "$output_path") && cd aero-relay"
echo "3. 在开发板上一键编译并打包 deb："
echo "   bash deploy/linux/build-all-on-device.sh --backend-only"
echo "4. 在开发板上一键安装并设置开机自启："
echo "   sudo apt install ./dist-linux/relay-linkd_0.1.0_arm64.deb"
echo "   sudo ./setup-service-role.sh"
echo "=========================================================="

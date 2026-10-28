#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=========================================================="
echo "      Relay Link: 板载前后端安装包一键编译与打包脚本       "
echo "=========================================================="

backend_only=0
for arg in "$@"; do
  case "$arg" in
    --backend-only|-b)
      backend_only=1
      ;;
  esac
done

if ! command -v cargo >/dev/null 2>&1 && [[ -x "${HOME:-}/.cargo/bin/cargo" ]]; then
  export PATH="${HOME:-}/.cargo/bin:$PATH"
fi

# 1. 检查基础环境依赖
missing_tools=()
check_tools=(cargo dpkg-deb)
if [[ $backend_only -eq 0 ]]; then
  check_tools+=(node npm)
fi

for tool in "${check_tools[@]}"; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    missing_tools+=("$tool")
  fi
done

if (( ${#missing_tools[@]} > 0 )); then
  echo "[-] 缺少必要构建工具: ${missing_tools[*]}"
  echo "    请先安装工具链后再运行此脚本："
  echo "    sudo apt update && sudo apt install -y build-essential curl nodejs npm"
  echo "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
  exit 1
fi

# 检查 GUI 库依赖（Tauri 所需，仅全量打包模式检查）
if [[ $backend_only -eq 0 ]]; then
  if ! pkg-config --exists webkit2gtk-4.1 gtk+-3.0 ayatana-appindicator3-0.1 2>/dev/null; then
    echo "[!] 警告: 未检测到完整的 WebKitGTK / GTK3 开发库，Tauri 前端打包可能会失败。"
    echo "    若只需机载中继后台，可运行: bash deploy/linux/build-all-on-device.sh --backend-only"
    echo "    或先安装 GUI 开发库: sudo apt update && sudo apt install -y libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev librsvg2-dev patchelf pkg-config"
    read -r -p "是否尝试继续构建? [y/N] " confirm
    if [[ "$confirm" != [yY] ]]; then
      exit 1
    fi
  fi
fi

arch="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "$arch" in
  aarch64) arch="arm64" ;;
  x86_64) arch="amd64" ;;
esac
echo "[+] 当前系统架构: $arch"

dist_dir="$repo_root/target/dist-packages"
mkdir -p "$dist_dir"

# 2. 构建后端 deb 包
echo ""
echo "[1/2] 开始编译并打包后端 (relay-linkd)..."
bash "$repo_root/deploy/linux/build-relay-link-deb.sh"

backend_deb="$(find "$repo_root/target/linux-package" -maxdepth 1 -name "relay-linkd_*_${arch}.deb" -print -quit)"
if [[ -z "$backend_deb" || ! -f "$backend_deb" ]]; then
  echo "[-] 后端 deb 包打包失败！"
  exit 1
fi
cp -f "$backend_deb" "$dist_dir/"
echo "[+] 后端 deb 打包成功: $(basename "$backend_deb")"

if [[ $backend_only -eq 1 ]]; then
  echo ""
  echo "=========================================================="
  echo "              机载后端 (relay-linkd) 打包完成！           "
  echo "=========================================================="
  echo "生成安装包位于: $dist_dir/$(basename "$backend_deb")"
  echo ""
  echo "在当前板子上安装命令："
  echo "  sudo apt install $dist_dir/$(basename "$backend_deb")"
  echo "=========================================================="
  exit 0
fi

# 3. 构建前端 deb 包
echo ""
echo "[2/2] 开始编译并打包前端 (relay-link-ui)..."
cd "$repo_root/apps/relay-link-ui"

if [[ ! -d "node_modules" ]]; then
  echo "    正在安装前端 npm 依赖..."
  npm install
fi

echo "    正在构建 Tauri 桌面应用及 deb 包..."
npm run tauri build -- --bundles deb --ignore-version-mismatches

frontend_deb="$(find "$repo_root/target/release/bundle/deb" -maxdepth 1 -name "*.deb" -print -quit 2>/dev/null || true)"
if [[ -z "$frontend_deb" || ! -f "$frontend_deb" ]]; then
  frontend_deb="$(find "$repo_root/apps/relay-link-ui/src-tauri/target/release/bundle/deb" -maxdepth 1 -name "*.deb" -print -quit 2>/dev/null || true)"
fi

if [[ -n "$frontend_deb" && -f "$frontend_deb" ]]; then
  cp -f "$frontend_deb" "$dist_dir/"
  echo "[+] 前端 deb 打包成功: $(basename "$frontend_deb")"
else
  echo "[-] 未找到生成的前端 deb 包，请检查上方编译输出！"
  exit 1
fi

echo ""
echo "=========================================================="
echo "                  打包全部完成！                          "
echo "=========================================================="
echo "生成安装包位于: $dist_dir"
ls -lh "$dist_dir"/*.deb
echo ""
echo "在当前板子上安装命令："
echo "  sudo apt install $dist_dir/$(basename "$backend_deb")"
echo "  sudo apt install \"$dist_dir/$(basename "$frontend_deb")\""
echo "=========================================================="

#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=========================================================="
echo "      Relay Link: 板载服务与Web管理界面一键编译打包        "
echo "=========================================================="

if ! command -v cargo >/dev/null 2>&1 && [[ -x "${HOME:-}/.cargo/bin/cargo" ]]; then
  export PATH="${HOME:-}/.cargo/bin:$PATH"
fi

# 1. 检查基础环境依赖
missing_tools=()
for tool in cargo dpkg-deb node npm; do
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

arch="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "$arch" in
  aarch64) arch="arm64" ;;
  x86_64) arch="amd64" ;;
esac
echo "[+] 当前系统架构: $arch"

dist_dir="$repo_root/dist-linux"
mkdir -p "$dist_dir"

# 2. 构建前端纯 Web 静态资源
echo ""
echo "[1/2] 构建前端 Web SPA 管理界面..."
cd "$repo_root/apps/relay-link-ui"
if [[ ! -d "node_modules" ]]; then
  echo "    正在安装前端 npm 依赖..."
  npm install
fi
npm run build
echo "[+] 前端 Web 资源构建完成 (已输出至 apps/relay-link-ui/dist)"

# 3. 构建内置 Web 服务的 relay-linkd deb 包
echo ""
echo "[2/2] 编译并打包自包含 Web 服务的 relay-linkd deb..."
cd "$repo_root"
bash "$repo_root/deploy/linux/build-relay-link-deb.sh"

backend_deb="$(find "$dist_dir" -maxdepth 1 -name "relay-linkd_*_${arch}.deb" -print -quit)"
if [[ -z "$backend_deb" || ! -f "$backend_deb" ]]; then
  echo "[-] 后端 deb 包打包失败！"
  exit 1
fi

echo ""
echo "=========================================================="
echo "                  打包全部完成！                          "
echo "=========================================================="
echo "生成安装包: $backend_deb"
echo "文件大小: $(ls -lh "$backend_deb" | awk '{print $5}')"
echo ""
echo "在当前设备上安装命令："
echo "  sudo apt install $backend_deb"
echo "  sudo /usr/local/bin/relay-link-setup"
echo ""
echo "安装后使用浏览器直接访问管理界面: http://<设备IP>:8080"
echo "=========================================================="

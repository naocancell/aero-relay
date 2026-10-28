#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=========================================================="
echo "    Relay Link: 桌面管理前端启动脚本 (Linux / Ubuntu)     "
echo "=========================================================="

# 1. 查找前端可执行文件
ui_bin=""
search_paths=(
  "/usr/bin/relay-link-ui"
  "$repo_root/target/release/relay-link-ui"
  "$repo_root/apps/relay-link-ui/src-tauri/target/release/relay-link-ui"
)

for path in "${search_paths[@]}"; do
  if [[ -x "$path" ]]; then
    ui_bin="$path"
    break
  fi
done

if [[ -z "$ui_bin" ]]; then
  echo "[-] 未找到前端可执行文件 (relay-link-ui)！"
  echo "    请先安装前端 deb 包或执行编译打包："
  echo "    sudo apt install ./dist-linux/\"Relay Link Control_0.1.0_*.deb\""
  echo "    或者在板子上编译: bash deploy/linux/build-all-on-device.sh"
  exit 1
fi

echo "[+] 使用前端程序: $ui_bin"

# 2. 检查桌面显示环境变量
if [[ -z "${DISPLAY:-}" && -z "${WAYLAND_DISPLAY:-}" ]]; then
  if [[ -e /tmp/.X11-unix/X0 ]]; then
    export DISPLAY=:0
    echo "[!] 检测到 DISPLAY 环境变量为空，已自动设置为 :0"
  else
    echo "[!] 警告: 未检测到图形桌面显示环境 (DISPLAY/WAYLAND_DISPLAY)，GUI 窗口可能无法显示。"
  fi
fi

# 3. 针对轻量化桌面及 ARM GPU 驱动的环境保护（防止 WebKitGTK 硬件加速白屏/崩溃）
export WEBKIT_DISABLE_COMPOSITING_MODE=1
export WEBKIT_DISABLE_DMABUF_RENDERER=1

# 4. 检查后台服务及 IPC 通信 Socket
ipc_socket="/run/relay-link/control.sock"
if [[ ! -e "$ipc_socket" ]]; then
  echo "[!] 提示: 后台管理套接字不存在: $ipc_socket"
  echo "    请确保 relay-linkd 后台服务已经启动："
  echo "    sudo bash deploy/linux/setup-service-role.sh"
  echo "    (若仅测试前端界面，程序将尝试后台重连...)"
fi

# 5. 在桌面创建双击快捷方式（可选/首次自动创建）
create_shortcut=0
if [[ "${1:-}" == "--create-shortcut" || "${1:-}" == "-c" ]]; then
  create_shortcut=1
fi

desktop_dir=""
for dir in "$HOME/Desktop" "$HOME/桌面"; do
  if [[ -d "$dir" ]]; then
    desktop_dir="$dir"
    break
  fi
done

if [[ -n "$desktop_dir" ]]; then
  shortcut_file="$desktop_dir/Relay-Link-Control.desktop"
  if [[ $create_shortcut -eq 1 || ! -f "$shortcut_file" ]]; then
    icon_path="/usr/share/icons/hicolor/128x128/apps/relay-link-ui.png"
    if [[ ! -f "$icon_path" ]]; then
      icon_path="$repo_root/apps/relay-link-ui/src-tauri/icons/128x128.png"
    fi

    cat > "$shortcut_file" <<EOF
[Desktop Entry]
Name=Relay Link Control
Comment=无人机多中继管理前端
Exec=$repo_root/deploy/linux/start-ui.sh
Icon=$icon_path
Terminal=false
Type=Application
Categories=Network;Utility;
StartupNotify=true
EOF
    chmod +x "$shortcut_file"
    # 若在支持 gio 的轻量桌面（如 XFCE / GNOME / MATE），标记为受信任启动文件
    gio set "$shortcut_file" metadata::trusted true 2>/dev/null || true
    echo "[+] 已在用户桌面创建启动快捷方式: $shortcut_file"
  fi
fi

# 6. IPC Socket 权限检查与启动
echo "[+] 正在启动管理前端界面..."
# 如果当前用户不在 relay-link 组且没有 socket 权限，尝试使用 sg 组会话启动
if [[ -e "$ipc_socket" && ! -r "$ipc_socket" ]]; then
  if getent group relay-link >/dev/null 2>&1 && id -nG | grep -qw "relay-link"; then
    exec "$ui_bin" "$@"
  elif getent group relay-link >/dev/null 2>&1; then
    echo "[!] 当前登录会话尚未生效 relay-link 组权限，尝试切换组会话启动..."
    exec sg relay-link -c "$ui_bin $*"
  fi
fi

exec "$ui_bin" "$@"

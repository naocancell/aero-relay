#!/usr/bin/env bash
set -Eeuo pipefail

echo "=========================================================="
echo "          Relay Link: Web 控制面板快捷打开工具           "
echo "=========================================================="

url="http://127.0.0.1:8080"
if [[ $# -ge 1 && -n "$1" ]]; then
  url="$1"
fi

echo "[+] 正在打开浏览器访问: $url"

if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$url"
elif command -v sensible-browser >/dev/null 2>&1; then
  sensible-browser "$url"
else
  echo "[!] 未检测到图形桌面浏览器启动器，请直接在任何设备的浏览器中访问: $url"
fi

#!/usr/bin/env bash
set -Eeuo pipefail

# 必须使用 root 权限运行
if [[ $EUID -ne 0 ]]; then
  echo "[-] 请使用 sudo 或 root 权限运行此脚本！例如："
  echo "    sudo bash $0 [server|relay|status|logs|stop]"
  exit 1
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "=========================================================="
echo "    Relay Link: 后台服务角色配置与开机自启动管理脚本      "
echo "=========================================================="

target_role="${1:-}"

# 如果没有传参，显示交互式菜单
if [[ -z "$target_role" ]]; then
  echo "请选择当前 Ubuntu 机器的运行角色："
  echo "  1) 服务端 (Server - 起降站/中心调度端，监听中继与地面站连接)"
  echo "  2) 中继站 (Relay  - 机载/地面中继端，连接服务端并转发数据)"
  echo "  3) 查看状态 (Status - 查看当前运行角色及服务状态)"
  echo "  4) 查看日志 (Logs - 跟踪后台最新输出日志)"
  echo "  5) 停止服务 (Stop - 停止后台运行并取消开机自启)"
  echo "  q) 退出"
  echo ""
  read -r -p "请输入选项编号 [1-5]: " choice
  case "$choice" in
    1) target_role="server" ;;
    2) target_role="relay" ;;
    3) target_role="status" ;;
    4) target_role="logs" ;;
    5) target_role="stop" ;;
    q|Q) exit 0 ;;
    *) echo "[-] 无效选项，退出。"; exit 1 ;;
  esac
fi

case "$target_role" in
  server|1)
    echo ""
    echo "[+] 正在将本机配置为 >>> 服务端 (Server) <<< ..."
    
    # 互斥停止中继站服务
    if systemctl is-active --quiet relay-linkd-relay.service 2>/dev/null; then
      echo "    检测到中继站服务正在运行，正在停止..."
      systemctl stop relay-linkd-relay.service
    fi
    systemctl disable relay-linkd-relay.service >/dev/null 2>&1 || true

    # 确保配置目录与文件存在
    install -d -o relay-link -g relay-link -m 0750 /usr/local/bin/config /usr/local/bin/log
    if [[ ! -f /usr/local/bin/config/config.toml ]]; then
      if [[ -f "$repo_root/config/config.example.toml" ]]; then
        install -o relay-link -g relay-link -m 0640 "$repo_root/config/config.example.toml" /usr/local/bin/config/config.toml
      elif [[ -f /usr/local/share/relay-link/config.example.toml ]]; then
        install -o relay-link -g relay-link -m 0640 /usr/local/share/relay-link/config.example.toml /usr/local/bin/config/config.toml
      fi
    fi

    # 确保调用用户加入 relay-link 组，使桌面 GUI 具备 IPC 通信权限
    target_user="${SUDO_USER:-}"
    if [[ -n "$target_user" && "$target_user" != "root" ]]; then
      usermod -aG relay-link "$target_user" 2>/dev/null || true
      echo "    已将当前桌面用户 [$target_user] 加入 relay-link 组 (具备 GUI 通信权限)。"
    fi

    # 重载 systemd 并启动服务端服务（设置开机自启）
    systemctl daemon-reload
    systemctl enable --now relay-linkd-server.service

    sleep 1
    if systemctl is-active --quiet relay-linkd-server.service; then
      echo ""
      echo "=========================================================="
      echo " [√] 服务端启动成功！并已成功设置开机自启动。"
      echo "=========================================================="
      echo " 服务名称: relay-linkd-server.service"
      echo " 配置文件: /usr/local/bin/config/config.toml"
      echo " 运行日志: /usr/local/bin/log/relay-linkd.log"
      echo " 状态查询: sudo systemctl status relay-linkd-server.service"
      echo " 实时日志: sudo tail -f /usr/local/bin/log/relay-linkd.log"
      echo "=========================================================="
    else
      echo "[-] 服务端启动失败，请检查配置与系统日志："
      systemctl status relay-linkd-server.service --no-pager
      exit 1
    fi
    ;;

  relay|2)
    echo ""
    echo "[+] 正在将本机配置为 >>> 中继站 (Relay) <<< ..."
    
    # 互斥停止服务端服务
    if systemctl is-active --quiet relay-linkd-server.service 2>/dev/null; then
      echo "    检测到服务端服务正在运行，正在停止..."
      systemctl stop relay-linkd-server.service
    fi
    systemctl disable relay-linkd-server.service >/dev/null 2>&1 || true

    # 确保配置目录与文件存在
    install -d -o relay-link -g relay-link -m 0750 /usr/local/bin/config /usr/local/bin/log
    if [[ ! -f /usr/local/bin/config/config.toml ]]; then
      if [[ -f "$repo_root/config/config.example.toml" ]]; then
        install -o relay-link -g relay-link -m 0640 "$repo_root/config/config.example.toml" /usr/local/bin/config/config.toml
      elif [[ -f /usr/local/share/relay-link/config.example.toml ]]; then
        install -o relay-link -g relay-link -m 0640 /usr/local/share/relay-link/config.example.toml /usr/local/bin/config/config.toml
      fi
    fi

    # 确保调用用户加入 relay-link 组，使桌面 GUI 具备 IPC 通信权限
    target_user="${SUDO_USER:-}"
    if [[ -n "$target_user" && "$target_user" != "root" ]]; then
      usermod -aG relay-link "$target_user" 2>/dev/null || true
      echo "    已将当前桌面用户 [$target_user] 加入 relay-link 组 (具备 GUI 通信权限)。"
    fi

    # 重载 systemd 并启动中继站服务（设置开机自启）
    systemctl daemon-reload
    systemctl enable --now relay-linkd-relay.service

    sleep 1
    if systemctl is-active --quiet relay-linkd-relay.service; then
      echo ""
      echo "=========================================================="
      echo " [√] 中继站启动成功！并已成功设置开机自启动。"
      echo "=========================================================="
      echo " 服务名称: relay-linkd-relay.service"
      echo " 配置文件: /usr/local/bin/config/config.toml"
      echo " 运行日志: /usr/local/bin/log/relay-linkd.log"
      echo " 状态查询: sudo systemctl status relay-linkd-relay.service"
      echo " 实时日志: sudo tail -f /usr/local/bin/log/relay-linkd.log"
      echo "=========================================================="
    else
      echo "[-] 中继站启动失败，请检查配置与系统日志："
      systemctl status relay-linkd-relay.service --no-pager
      exit 1
    fi
    ;;

  status|3)
    echo ""
    echo "=== 后台服务状态 ==="
    server_status="$(systemctl is-active relay-linkd-server.service 2>/dev/null || echo 'inactive')"
    server_enabled="$(systemctl is-enabled relay-linkd-server.service 2>/dev/null || echo 'disabled')"
    relay_status="$(systemctl is-active relay-linkd-relay.service 2>/dev/null || echo 'inactive')"
    relay_enabled="$(systemctl is-enabled relay-linkd-relay.service 2>/dev/null || echo 'disabled')"

    echo "服务端 (Server): 状态 = $server_status | 开机自启 = $server_enabled"
    echo "中继站 (Relay) : 状态 = $relay_status | 开机自启 = $relay_enabled"
    echo ""
    if [[ "$server_status" == "active" ]]; then
      systemctl status relay-linkd-server.service --no-pager -l
    elif [[ "$relay_status" == "active" ]]; then
      systemctl status relay-linkd-relay.service --no-pager -l
    else
      echo "[!] 当前未运行任何后台角色。"
    fi
    ;;

  logs|4)
    log_file="/usr/local/bin/log/relay-linkd.log"
    if [[ -f "$log_file" ]]; then
      echo "正在跟踪日志: $log_file (按 Ctrl+C 退出)"
      tail -n 50 -f "$log_file"
    else
      echo "[-] 日志文件尚未生成: $log_file"
    fi
    ;;

  stop|5)
    echo "正在停止所有 Relay Link 后台服务..."
    systemctl stop relay-linkd-server.service 2>/dev/null || true
    systemctl disable relay-linkd-server.service 2>/dev/null || true
    systemctl stop relay-linkd-relay.service 2>/dev/null || true
    systemctl disable relay-linkd-relay.service 2>/dev/null || true
    echo "[√] 已停止后台服务并取消开机自启。"
    ;;

  *)
    echo "[-] 未知角色/参数: $target_role"
    echo "    用法: sudo bash $0 [server|relay|status|logs|stop]"
    exit 1
    ;;
esac

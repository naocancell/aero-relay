# relay-link

`relay-link` 是按照《无人机多中继通信系统：Tauri 界面与后台服务架构》实现的第一版工程：一架无人机、一个起降站地面站、多个无人中继站。通信核心和管理界面分进程运行，关闭 Tauri 不会停止 `relay-linkd`。Windows 发布使用一个 NSIS 安装包，安装时只选择“服务端”或“中继站”。

## 工程结构

```text
crates/relay-core       配置、PathId、链路健康、粘性 active_path 选择
crates/relay-ipc        长度前缀 JSON、本机 Named Pipe / Unix Socket
apps/relay-linkd        relay/server 后台服务
apps/relay-link-ui      Tauri 2 + Vue 3 管理界面
config                  Windows/Linux 配置示例
deploy                  systemd 和 Windows Service 模板
```

服务端选择的单位始终是 `relay_id + link_id`。当前路径满足 TCP 连接、心跳、适配器就绪和飞机数据新鲜度时保持不变；只有失效后才选择最近收到飞机数据的候选路径。没有候选时不发送控制数据，也不在切换后自动重放旧控制包。服务器会把当前活动链路通知给对应中继，因此中继界面也能显示本机链路是否被选中。上游飞机 payload 和地面站控制 payload 不做业务改写；服务端向地面站只转发 CRC 有效的 32 字节飞机帧，地面站心跳由独立周期定时器发送，不在每个飞机帧后追加。

## 构建与测试

需要 Rust stable、Node.js/npm；Windows 构建 Tauri 还需要 WebView2，Linux 构建需要 WebKitGTK 开发依赖。

```powershell
cargo fmt --all -- --check
cargo test --workspace
cargo check --workspace
cargo build -p relay-linkd
```

后台始终从可执行文件旁边的固定路径读取配置：`<exe目录>/config/config.toml`。开发时先把统一本地配置复制到 debug 输出目录：

```powershell
New-Item -ItemType Directory -Force target/debug/config | Out-Null
Copy-Item config/config.local.toml target/debug/config/config.toml -Force
```

中继站和服务端后台都将日志追加到可执行文件旁边的固定文件：`<exe目录>/log/relay-linkd.log`。目录不存在时后台会自动创建；默认记录 `info` 及以上级别，仍可通过 `RUST_LOG` 调整级别。

不需要生成证书。启动模式只决定读取统一文件中的 `[relay]` 或 `[server]` 段。

然后在对应机器上启动后台和 Tauri 界面：

```powershell
# 服务端机器
cargo run -p relay-linkd -- server

# 中继站机器
cargo run -p relay-linkd -- relay

# 对应机器上的窗口
cd apps/relay-link-ui
npm install
npm run tauri dev
```

本地配置使用 `127.0.0.1:7443` 的中继 TCP 服务、`127.0.0.1:7500` 的地面站 TCP 服务和 `127.0.0.1:9001` 的 UDP 中继链路，不需要串口或真实无人机硬件。服务器监听 7443 和 7500；中继站连接 7443，地面站连接 7500。由于 IPC 地址固定，同一台机器不能同时启动 relay 和 server 两个后台进程；本地端到端测试应使用两台机器或自动化 loopback 测试。

Tauri 前端：

```powershell
cd apps/relay-link-ui
npm install
npm run build
npm run tauri dev
```

UI 和后台使用固定 IPC 地址：Windows `\\.\pipe\relay-link-control`，Linux `/run/relay-link/control.sock`。该地址不属于 TOML 配置，也不提供环境变量或前端覆盖入口。前端通过本机 IPC 获取快照、配置和串口列表，并通过后台校验后保存配置；显式 command 为 `get_snapshot`、`get_config`、`save_config`、`get_serial_ports`、`restart_backend` 和 `subscribe_snapshot`，没有 shell、任意文件系统或任意远端网络权限。

前端会根据后台角色显示不同的配置编辑器：中继站可以编辑服务端地址、串口/UDP 链路；服务端可以编辑监听地址、地面站地址和超时参数。串口号通过后台 IPC 实时枚举并在下拉框中刷新，串口只提供串口号和波特率，其他参数固定为 8 数据位、1 停止位、无校验、无流控制。保存由后台写回安装目录下的统一 `config/config.toml`，写入前会执行与启动相同的配置校验；点击“重启后台服务”后，后台在进程内热重载通信运行时，管理 IPC 不会断开。

## 配置

中继配置支持多个 `[[relay.links]]`：

- `serial`：只配置 `path` 和 `baud_rate`；运行时固定使用 8-N-1、无流控制，串口打开/读写失败独立重试。Vue 前端通过后台 IPC 获取当前主机的串口列表并周期刷新。
- `udp_unicast`：`bind_addr`、固定 `tx_target`，可选 `allowed_source`。
- `udp_multicast`：显式 `group_addr`、`interface_addr`、`tx_target`、TTL 和本地回环。

配置文件统一为 `config/config.toml`，可以同时包含 `[relay]` 和 `[server]`。`relay-linkd relay` 只读取 `[relay]`，`relay-linkd server` 只读取 `[server]`；启动模式由服务安装参数固定，不从配置文件切换。配置加载时会拒绝重复 `link_id`、非法 socket 地址、非组播地址、无效 TTL、零心跳周期，以及超过地面站固定 32 字节心跳帧容量的路径 ID。每条路径都必须满足 `UTF-8([relay].id) + UTF-8(link.id) <= 22` 字节。`server.listen_addr` 是中继 TCP 服务监听地址，`server.ground_station_listen_addr` 是地面站飞机数据/控制 TCP 服务监听地址；旧配置中的 `ground_station_addr` 仍可作为兼容别名加载。UDP 套接字 ready 只表示本地适配器已经绑定，不表示飞机在线；服务器仍要求在 `data_timeout_ms` 内收到飞机数据。

中继通过 TCP 主动连接服务器，适配 Starlink IPv4 CGNAT。中继与服务端之间使用 4 字节长度前缀的明文 TCP 帧：`AircraftData` 和 `ControlData` 的 `relay_id`、`link_id` 放在短二进制头中，实际 `payload` 原始字节直接跟在帧内，不再转换成 JSON 数字数组；Hello、Heartbeat、ActivePath、LinkStatus 等低频控制/状态消息仍使用 JSON 帧。这不是加密协议，跨不可信网络时需要由外部 VPN 或网络隧道提供保护；升级时中继和服务端都要使用同一版二进制数据帧实现。地面站 7500 端口同样是明文 TCP；客户端连接后必须先发送不带换行的 ASCII `dqiang`，服务端在 1 秒内未收到完整匹配内容就关闭连接，握手成功后才进入原始数据流。服务端会持续缓存上游字节，按 `EB90` 找到完整 32 字节帧并执行 `MakeCheckCRC(frame + 2, 28)` 等价校验，只转发有效飞机帧。服务端按 `ground_station_heartbeat_interval_ms` 独立发送 `EB90 + 32字节 + CRC` 心跳帧，心跳类型为 `0xFE`，不会在每个飞机帧后追加心跳；地面站必须按持久 TCP 缓存逐帧解析，识别心跳后不要送入飞控遥测解码器。两个 TOML 示例不包含 IPC 地址；后台和 UI 使用代码内固定的 Windows Named Pipe 或 Linux Unix Socket。

## 受管视频转发

视频不复用上述遥测/控制 TCP 协议。中继站由同一个 `relay-linkd` 直接启动并监管 FFmpeg：从飞机摄像头拉取 RTSP（强制 TCP），以 `-c copy` 不转码推送 RTMP；服务端由同一个 `relay-linkd` 启动并监管 MediaMTX，接收 RTMP，并向需要观看的一侧提供 RTSP 读取入口。两个第三方可执行文件不是第二个常驻服务：Windows 上它们挂在后台进程的 Job Object 中，后台异常退出时 Job 会终止子进程；Linux systemd 服务使用 `KillMode=control-group`。配置热重载、后台停止和服务重启都会先通知并回收媒体子进程。

未配置 `[relay.video]` / `[server.video]` 时，视频功能完全关闭，原有通信行为不变。启用时两台机器分别填写各自角色的段，且 RTMP 路径必须相同：

```toml
# relay host
[relay.video]
source_url = "rtsp://camera.example/live"
publish_url = "rtmp://video-server.example:1935/uav/relay-a"
startup_timeout_ms = 15000
stall_timeout_ms = 10000
restart_max_delay_ms = 30000

# server host
[server.video]
stream_path = "uav/relay-a"
# 固定为本机 MediaMTX API，缺省值也是该地址。
control_api_addr = "127.0.0.1:9997"
startup_timeout_ms = 15000
stall_timeout_ms = 10000
restart_max_delay_ms = 30000
poll_interval_ms = 1000
```

中继快照中的 `publishing` 表示 FFmpeg 持续报告推流进度；服务端的 `online` 表示本机 MediaMTX API 已确认指定路径可用。`backoff`、`stalled` 和 `last_error` 会显示缺少工具、进程退出、无进度或 API 不可达等原因；快照和界面不返回摄像头/RTMP URL。服务端接收成功后可用 `rtsp://<server>:8554/uav/relay-a` 读取。MediaMTX 的本机 API 固定为 `127.0.0.1:9997`，不对网络暴露；服务器防火墙只应按实际需要开放 RTMP TCP 1935（中继到服务端）和 RTSP TCP 8554（观看端到服务端）。

发布视频版前，将已审核、与目标平台匹配的 FFmpeg 和 MediaMTX 放入固定位置；Windows 可以先执行：

```powershell
.\deploy\windows\stage-media-tools.ps1 `
  -FfmpegExecutable C:\approved\ffmpeg.exe `
  -MediaMtxExecutable C:\approved\mediamtx.exe
.\deploy\windows\build-relay-link-installer.ps1 -RequireMediaTools
```

安装后的布局为 `<安装目录>/media/ffmpeg/ffmpeg.exe`、`<安装目录>/media/mediamtx/mediamtx.exe` 和 `<安装目录>/config/mediamtx.yml`。Linux 使用同样的可执行文件相对布局（无 `.exe` 后缀），并复制 `deploy/windows/installer-resources/config/mediamtx.yml` 到 `<exe目录>/config/mediamtx.yml`。工具缺失不会停止飞机遥测转发，但视频状态会进入退避重试并给出原因。

Windows staging 脚本会一起复制指定 `.exe` 同目录下的 DLL 依赖；应将已审核的、完整解压后的工具目录作为输入，避免只复制一个依赖外部 DLL 的可执行文件。

第一版只做 H.264 兼容视频的封装转发，不会降低码率；因此必须在摄像头侧按中继上行可用带宽设置码率、关键帧间隔和分辨率。RTSP/RTMP 均不是加密传输，和现有明文通信一样，跨不可信网络时应使用外部 VPN 或专用网络保护它们。

## 运行与部署

中继无界面运行：

```text
relay-linkd relay
```

服务器无界面运行：

```text
relay-linkd server
```

Linux 将统一配置放在 `/usr/local/bin/config/config.toml`，并在启动服务前创建日志目录：

```sh
sudo install -d -o relay-link -g relay-link -m 0750 /usr/local/bin/log
```

然后将 `deploy/linux/relay-linkd-server.service` 或 `relay-linkd-relay.service` 复制到 `/etc/systemd/system/`，创建 `relay-link` 用户/组并执行 `systemctl enable --now relay-linkd-server`。`RuntimeDirectory=relay-link` 会创建受限的 `/run/relay-link`，不使用本机 TCP 管理端口。两个角色都会追加 `/usr/local/bin/log/relay-linkd.log`。

Windows 构建安装包：

```powershell
.\deploy\windows\build-relay-link-installer.ps1
```

构建产物位于 `target/release/bundle/nsis/`。现场人员双击安装包后只选择“是=服务端”或“否=中继站”；安装器会把统一配置放在安装目录的 `config/config.toml`，并以 `relay-linkd.exe server` 或 `relay-linkd.exe relay` 的启动参数注册和启动 `RelayLinkD` 服务，服务设置为开机自启和异常重启。后台服务不依赖 UI，关闭窗口不会停止通信。现场人员不需要手工运行后台程序或输入命令行；配置路径和 IPC 地址都不支持外部覆盖。

Windows 服务首次启动时会在安装目录创建 `log` 文件夹，并将中继站或服务端运行日志追加到 `log\\relay-linkd.log`。

## IPC 行为

消息是 4 字节大端长度 + JSON，单帧上限 1 MiB。UI 连接后请求完整 `Snapshot`，再发送 `Subscribe` 接收 `StateChanged`。断开后的重试间隔为 1s、2s、5s，重连成功后重新请求完整快照；不会重放断线期间的 UI 请求。Unix Socket 创建为 `0660`，Windows Named Pipe 限制为本机并允许系统、管理员和交互用户访问。后台 IPC 出错只会影响监控界面，不会停止飞机数据转发任务。

## 验证边界

自动化验证覆盖配置校验、活动路径保持/故障切换、长度分帧、协议 loopback、受管视频的参数/API 响应/退避规则、后台和 Tauri Rust 编译，以及 Windows 安装器资源校验。真实串口、Windows/Linux 多网卡组播入组、Starlink 网络、RTSP 摄像头编码兼容性、RTMP/RTSP 防火墙、实际 FFmpeg/MediaMTX 二进制、明文 TCP 网络连通性、systemd/Windows Service 权限、安装后重启和 Tauri 窗口关闭行为仍需要目标硬件与目标操作系统上的现场确认。

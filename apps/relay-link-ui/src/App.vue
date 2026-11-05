<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { shouldLoadConfigAfterIpcReconnect } from "./startup";
import {
  apiGetSnapshot,
  apiGetConfig,
  apiSaveConfig,
  apiGetSerialPorts,
  apiRestartBackend,
  apiSubscribe,
  apiSetIoChannel,
  apiSetIoAll,
  resolveHttpBase,
  type SnapshotPayload,
  type Config,
  type RelayLink,
  type LinkType,
  type VideoSnapshot,
  type IoControllerSnapshot,
} from "./api";


type Link = {
  path: { relay_id: string; link_id: string };
  kind: string;
  adapter_ready: boolean;
  last_rx_at?: string;
  last_tx_at?: string;
  rx_bytes: number;
  tx_bytes: number;
  active: boolean;
  last_error?: string;
};

const queryEndpoint =
  typeof window !== "undefined"
    ? new URLSearchParams(window.location.search).get("endpoint")
    : null;
const snapshot = ref<SnapshotPayload | null>(null);
const configDraft = ref<Config | null>(null);
const connected = ref(false);
const targetEndpoint = ref(queryEndpoint ?? (localStorage.getItem("relay_target_endpoint") || ""));
const error = ref("");
const configError = ref("");
const configMessage = ref("");
const configLoading = ref(false);
const savingConfig = ref(false);
const serialPorts = ref<string[]>([]);
const restartingBackend = ref(false);
const restartError = ref("");
const restartMessage = ref("");
let serialPortTimer: number | undefined;
let configLoadInFlight = false;
let unmounted = false;
let stopSubscription: (() => void) | undefined;
const form = computed(() => (configDraft.value ?? {}) as Record<string, any>);
const relayLinks = computed(() => (configDraft.value?.relay?.links ?? []) as RelayLink[]);
const activeRole = computed(() => snapshot.value?.role ?? (configDraft.value?.relay ? "relay" : configDraft.value?.server ? "server" : null));
const isProxiedRelayView = computed(() => targetEndpoint.value.includes("/proxy/relay/"));
const role = computed(() => {
  if (activeRole.value === "relay") {
    return isProxiedRelayView.value ? "中继站 (服务端远程代理)" : "中继站";
  }
  if (activeRole.value === "server") return "服务器";
  return "等待后台角色";
});
const state = computed(() => (snapshot.value?.state ?? {}) as Record<string, any>);
const links = computed(() => (state.value.links ?? []) as Link[]);
const relays = computed(() => (state.value.relays ?? []) as Array<{ relay_id: string; connected: boolean; links: Link[] }>);
const video = computed<VideoSnapshot>(() => {
  const value = (state.value.video ?? {}) as Partial<VideoSnapshot>;
  return {
    state: value.state ?? "disabled",
    worker_running: value.worker_running ?? false,
    stream_ready: value.stream_ready ?? false,
    restart_count: value.restart_count ?? 0,
    last_progress_at: value.last_progress_at,
    last_error: value.last_error,
  };
});

const ioController = computed<IoControllerSnapshot>(() => {
  const value = (state.value.io_controller ?? {}) as Partial<IoControllerSnapshot>;
  return {
    enabled: value.enabled ?? false,
    connected: value.connected ?? false,
    channels: value.channels ?? Array(16).fill(false),
    last_update_at: value.last_update_at,
    last_error: value.last_error,
  };
});
const ioOperating = ref<number | "all" | null>(null);
const ioError = ref("");

async function toggleChannel(channelIndex: number) {
  const channel = channelIndex + 1;
  const currentState = ioController.value.channels[channelIndex] ?? false;
  const nextState = !currentState;
  ioOperating.value = channel;
  ioError.value = "";
  try {
    await apiSetIoChannel(targetEndpoint.value, channel, nextState);
  } catch (err: any) {
    ioError.value = err?.message || String(err);
  } finally {
    ioOperating.value = null;
  }
}

async function setAllChannels(nextState: boolean) {
  ioOperating.value = "all";
  ioError.value = "";
  try {
    await apiSetIoAll(targetEndpoint.value, nextState);
  } catch (err: any) {
    ioError.value = err?.message || String(err);
  } finally {
    ioOperating.value = null;
  }
}

function enableRelayIo() {
  if (!configDraft.value?.relay || configDraft.value.relay.io_controller) return;
  configDraft.value.relay.io_controller = {
    port: "/dev/ttyS7",
    baud_rate: 9600,
    slave_id: 1,
    poll_interval_ms: 1000,
  };
}

function disableRelayIo() {
  if (configDraft.value?.relay) configDraft.value.relay.io_controller = null;
}

function openRelayWeb(relayId: string) {
  const clean = targetEndpoint.value.trim();
  const proxyEndpoint = clean
    ? `${resolveHttpBase(clean)}/proxy/relay/${encodeURIComponent(relayId)}`
    : `/proxy/relay/${encodeURIComponent(relayId)}`;
  window.open(`/?endpoint=${encodeURIComponent(proxyEndpoint)}`, "_blank");
}

function openRelayLLink(relayId: string) {
  const base = resolveHttpBase(targetEndpoint.value);
  window.open(`${base}/proxy/llink/${encodeURIComponent(relayId)}/`, "_blank");
}

function openCurrentRelayLLink() {
  const clean = targetEndpoint.value.trim();
  if (clean.includes("/proxy/relay/")) {
    const llinkUrl = resolveHttpBase(clean).replace("/proxy/relay/", "/proxy/llink/") + "/";
    window.open(llinkUrl, "_blank");
    return;
  }
  const base = resolveHttpBase(clean);
  window.open(`${base}/proxy/llink/local/`, "_blank");
}

function applySnapshot(value: SnapshotPayload) {
  snapshot.value = value;
  connected.value = true;
  error.value = "";
}

function cloneConfig(value: Config): Config {
  return JSON.parse(JSON.stringify(value)) as Config;
}

function normalizedConfig(value: Config): Config {
  const candidate = cloneConfig(value);
  for (const link of candidate.relay?.links ?? []) {
    if (link.type === "udp_unicast" && typeof link.allowed_source === "string" && !link.allowed_source.trim()) {
      link.allowed_source = null;
    }
  }
  return candidate;
}

async function loadConfig() {
  if (configLoadInFlight) return;
  configLoadInFlight = true;
  configLoading.value = true;
  configError.value = "";
  let attempt = 0;
  try {
    while (configDraft.value === null && !unmounted) {
      try {
        const value = await apiGetConfig(targetEndpoint.value);
        if (unmounted) return;
        configDraft.value = cloneConfig(value);
        configError.value = "";
      } catch (cause) {
        if (unmounted) break;
        configError.value = String(cause);
        await new Promise<void>((resolve) => window.setTimeout(resolve, retryDelay(attempt)));
        if (unmounted) break;
        attempt += 1;
      }
    }
  } finally {
    configLoading.value = false;
    configLoadInFlight = false;
  }
}

async function saveConfig() {
  if (!configDraft.value) return;
  savingConfig.value = true;
  configError.value = "";
  configMessage.value = "";
  try {
    const candidate = normalizedConfig(configDraft.value);
    await apiSaveConfig(targetEndpoint.value, candidate);
    configDraft.value = candidate;
    configMessage.value = "配置已保存，请点击“重启后台服务”使其生效";
  } catch (cause) {
    configError.value = String(cause);
  } finally {
    savingConfig.value = false;
  }
}

async function loadSerialPorts() {
  try {
    const ports = await apiGetSerialPorts(targetEndpoint.value);
    if (!unmounted) serialPorts.value = ports;
  } catch {
    // Keep the last successful list while the backend reconnects.
  }
}

function isUnavailableSerialPort(path: unknown): boolean {
  return typeof path === "string" && Boolean(path) && !serialPorts.value.includes(path);
}

async function restartBackend() {
  restartingBackend.value = true;
  restartError.value = "";
  restartMessage.value = "";
  try {
    await apiRestartBackend(targetEndpoint.value);
    restartMessage.value = "后台服务已重启，配置已热重载";
  } catch (cause) {
    restartError.value = String(cause);
  } finally {
    restartingBackend.value = false;
  }
}

function createLink(type: LinkType, id: string): RelayLink {
  if (type === "serial") {
    return {
      id,
      type,
      path: "COM1",
      baud_rate: 115200,
    };
  }
  if (type === "udp_unicast") {
    return {
      id,
      type,
      bind_addr: "0.0.0.0:9001",
      tx_target: "127.0.0.1:9002",
      allowed_source: null,
    };
  }
  return {
    id,
    type,
    bind_addr: "0.0.0.0:10001",
    group_addr: "239.10.10.10",
    interface_addr: "192.168.1.10",
    tx_target: "239.10.10.10:10001",
    multicast_ttl: 1,
    multicast_loop: false,
  };
}

function addLink(type: LinkType) {
  if (!configDraft.value?.relay) return;
  const index = configDraft.value.relay.links.length + 1;
  configDraft.value.relay.links.push(createLink(type, `${type}-${index}`));
}

function changeLinkType(index: number, event: globalThis.Event) {
  if (!configDraft.value?.relay) return;
  const type = (event.target as HTMLSelectElement).value as LinkType;
  const current = configDraft.value.relay.links[index];
  configDraft.value.relay.links[index] = createLink(type, current?.id || `${type}-${index + 1}`);
}

function removeLink(index: number) {
  configDraft.value?.relay?.links.splice(index, 1);
}

function enableRelayVideo() {
  if (!configDraft.value?.relay || configDraft.value.relay.video) return;
  configDraft.value.relay.video = {
    source_url: "",
    publish_url: "",
    startup_timeout_ms: 15000,
    stall_timeout_ms: 10000,
    restart_max_delay_ms: 30000,
  };
}

function disableRelayVideo() {
  if (configDraft.value?.relay) configDraft.value.relay.video = null;
}

function enableRelayTurntable() {
  if (!configDraft.value?.relay || configDraft.value.relay.turntable) return;
  configDraft.value.relay.turntable = {
    bind_addr: "0.0.0.0:0",
    target_addr: "192.168.1.50:9003",
  };
}

function disableRelayTurntable() {
  if (configDraft.value?.relay) configDraft.value.relay.turntable = null;
}

function enableServerVideo() {
  if (!configDraft.value?.server || configDraft.value.server.video) return;
  configDraft.value.server.video = {
    stream_path: "uav-1",
    control_api_addr: "127.0.0.1:9997",
    startup_timeout_ms: 15000,
    stall_timeout_ms: 10000,
    restart_max_delay_ms: 30000,
    poll_interval_ms: 1000,
  };
}

function disableServerVideo() {
  if (configDraft.value?.server) configDraft.value.server.video = null;
}

function videoStateLabel(value: string): string {
  const labels: Record<string, string> = {
    disabled: "未启用",
    standby: "待机，等待服务端选中",
    starting: "启动中",
    publishing: "正在推流",
    waiting_for_publisher: "等待中继推流",
    online: "流已到达服务端",
    backoff: "重试等待",
    stalled: "无进度",
  };
  return labels[value] ?? value;
}

function formatTimestamp(value?: string | number | null, fallback = "—"): string {
  if (!value) return fallback;
  const ms = typeof value === "string" ? parseInt(value, 10) : value;
  if (isNaN(ms) || ms <= 0) {
    return typeof value === "string" && isNaN(Number(value)) ? value : fallback;
  }
  const date = new Date(ms);
  if (isNaN(date.getTime())) return fallback;
  const pad = (n: number) => n.toString().padStart(2, "0");
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  return `${hours}:${minutes}:${seconds}`;
}

function retryDelay(attempt: number): number {
  if (attempt === 0) return 1000;
  if (attempt === 1) return 2000;
  return 5000;
}

async function restartSubscription() {
  stopSubscription?.();
  stopSubscription = undefined;

  const endpoint = targetEndpoint.value.trim();

  stopSubscription = await apiSubscribe(endpoint, {
    onSnapshot: (snap) => {
      if (unmounted) return;
      applySnapshot(snap);
      if (shouldLoadConfigAfterIpcReconnect(true, configDraft.value !== null, configLoadInFlight)) {
        void loadConfig();
      }
    },
    onStatusChange: (status) => {
      if (unmounted) return;
      connected.value = status;
      if (!status) {
        error.value = "后台服务已断开";
      } else {
        error.value = "";
        if (shouldLoadConfigAfterIpcReconnect(true, configDraft.value !== null, configLoadInFlight)) {
          void loadConfig();
        }
      }
    },
  });

  try {
    const snap = await apiGetSnapshot(endpoint);
    if (!unmounted) applySnapshot(snap);
  } catch {}

  await loadConfig();
}

async function applyEndpoint() {
  const clean = targetEndpoint.value.trim();
  if (!clean.includes("/proxy/relay/")) {
    localStorage.setItem("relay_target_endpoint", clean);
  }
  configDraft.value = null;
  connected.value = false;
  error.value = clean ? `正在连接 ${clean}...` : "正在连接本地服务...";
  await restartSubscription();
  await loadSerialPorts();
}

onMounted(async () => {
  await restartSubscription();
  await loadSerialPorts();
  if (!unmounted) serialPortTimer = window.setInterval(loadSerialPorts, 2000);
});

onUnmounted(() => {
  unmounted = true;
  if (serialPortTimer !== undefined) window.clearInterval(serialPortTimer);
  stopSubscription?.();
});
</script>

<template>
  <main class="shell">
    <header class="topbar">
      <div class="title">
        <h1>中继链路管理</h1>
        <span class="role-tag" :class="{ 'proxy-tag': isProxiedRelayView }">{{ role }}</span>
        <button
          v-if="snapshot?.role === 'relay'"
          class="button secondary-button"
          type="button"
          @click="openCurrentRelayLLink"
        >
          打开本站 L 链配置页
        </button>
      </div>
      <div class="endpoint-control">
        <label>设备地址:</label>
        <input v-model="targetEndpoint" placeholder="留空自动连接当前主机，或 192.168.0.2:7480" @keyup.enter="applyEndpoint" />
        <button type="button" @click="applyEndpoint">连接</button>
      </div>
      <div class="status" :class="{ online: connected }">
        <span class="status-dot" />
        {{ connected ? "后台已连接" : "后台已断开" }}
      </div>
    </header>

    <section v-if="error || configError || restartError" class="alert">
      {{ error || configError || restartError }}
    </section>

    <section v-if="snapshot?.role === 'relay'" class="summary-grid">
      <article class="metric"><span>Relay ID</span><strong>{{ state.relay_id || "—" }}</strong></article>
      <article class="metric"><span>服务器连接</span><strong>{{ state.server_connected ? "已连接" : "断开" }}</strong></article>
      <article class="metric"><span>运行时间</span><strong>{{ state.uptime_seconds ?? 0 }} s</strong></article>
    </section>

    <section v-if="snapshot?.role === 'relay'" class="panel">
      <div class="panel-heading"><h2>视频转发状态</h2><span class="pill" :class="{ good: video.stream_ready }">{{ videoStateLabel(video.state) }}</span></div>
      <div class="summary-grid">
        <article class="metric"><span>子进程</span><strong>{{ video.worker_running ? "运行中" : "未运行" }}</strong></article>
        <article class="metric"><span>推流就绪</span><strong>{{ video.stream_ready ? "是" : "否" }}</strong></article>
        <article class="metric"><span>重启次数</span><strong>{{ video.restart_count }}</strong></article>
        <article class="metric"><span>最后进度</span><strong class="mono">{{ formatTimestamp(video.last_progress_at) }}</strong></article>
      </div>
      <p v-if="video.last_error" class="danger">{{ video.last_error }}</p>
    </section>

    <section v-if="snapshot?.role === 'relay'" class="panel">
      <div class="panel-heading">
        <div class="title">
          <h2>IO 继电器控制器 (LH-IO1600)</h2>
          <span class="pill" :class="{ good: ioController.connected }">
            {{ !ioController.enabled ? "未配置启用" : ioController.connected ? "已连接 (RS485/Modbus)" : "未连接 / 离线" }}
          </span>
        </div>
        <div class="io-actions" v-if="ioController.enabled">
          <button
            class="button secondary-button"
            type="button"
            :disabled="!ioController.connected || ioOperating !== null"
            @click="setAllChannels(true)"
          >
            一键全开
          </button>
          <button
            class="button secondary-button"
            type="button"
            :disabled="!ioController.connected || ioOperating !== null"
            @click="setAllChannels(false)"
          >
            一键全关
          </button>
        </div>
      </div>
      <p v-if="ioError || ioController.last_error" class="danger" style="margin: 0; padding: 10px 16px;">
        {{ ioError || ioController.last_error }}
      </p>
      <div v-if="ioController.enabled" class="io-grid">
        <div
          v-for="(isActive, index) in ioController.channels"
          :key="index"
          class="io-card"
          :class="{ active: isActive }"
        >
          <div class="io-card-header">
            <span class="io-card-name">DO{{ index + 1 }}</span>
            <span class="io-status-indicator" />
          </div>
          <span class="io-status-text">{{ isActive ? "导通 (闭合)" : "断开 (释放)" }}</span>
          <button
            class="io-btn"
            type="button"
            :disabled="!ioController.connected || ioOperating !== null"
            @click="toggleChannel(index)"
          >
            {{ ioOperating === index + 1 ? "操作中..." : (isActive ? "关闭" : "开启") }}
          </button>
        </div>
      </div>
      <div v-else class="config-body muted">
        未在系统配置中启用继电器控制器。可在下方“中继站配置”中开启。
      </div>
    </section>

    <section v-if="snapshot?.role === 'relay'" class="panel">
      <div class="panel-heading"><h2>本地通信链路</h2><span class="note">{{ links.length }} 条</span></div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Link ID</th><th>类型</th><th>适配器</th><th>活动路径</th><th>最后接收</th><th>RX / TX</th><th>错误</th></tr></thead>
          <tbody>
            <tr v-for="link in links" :key="link.path.link_id">
              <td class="mono">{{ link.path.link_id }}</td><td>{{ link.kind }}</td>
              <td><span class="pill" :class="{ good: link.adapter_ready }">{{ link.adapter_ready ? "就绪" : "断开" }}</span></td>
              <td><span v-if="link.active" class="active-label">活动</span><span v-else class="muted">—</span></td>
              <td class="muted">{{ formatTimestamp(link.last_rx_at, "尚无飞机数据") }}</td>
              <td class="mono">{{ link.rx_bytes }} / {{ link.tx_bytes }}</td><td class="danger">{{ link.last_error || "—" }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-if="activeRole === 'relay' && form.relay" class="panel config-panel">
      <div class="panel-heading">
        <h2>中继站配置</h2>
        <span class="note">保存后点击“重启后台服务”生效</span>
      </div>
      <div v-if="configLoading" class="config-body muted">正在读取配置文件…</div>
      <div v-else class="config-body">
        <div class="form-grid">
          <label class="field"><span>中继站 ID</span><input v-model="form.relay.id" /></label>
          <label class="field"><span>服务端地址（IP/主机名:端口）</span><input v-model="form.relay.server_addr" placeholder="192.168.1.10:7443" /></label>
          <label class="field"><span>心跳周期（毫秒）</span><input v-model.number="form.relay.heartbeat_interval_ms" type="number" min="1" /></label>
          <label class="field"><span>数据超时（毫秒）</span><input v-model.number="form.relay.data_timeout_ms" type="number" min="1" /></label>
        </div>

        <div class="config-section-heading"><h3>视频转发</h3><button v-if="!form.relay.video" class="button secondary-button" type="button" @click="enableRelayVideo">启用视频转发</button><button v-else class="button danger-button" type="button" @click="disableRelayVideo">关闭视频转发</button></div>
        <div v-if="form.relay.video" class="form-grid nested-grid">
          <label class="field"><span>摄像头 RTSP 地址</span><input v-model="form.relay.video.source_url" type="text" autocomplete="off" placeholder="rtsp://camera.example/live" /></label>
          <label class="field"><span>服务端 RTMP 地址</span><input v-model="form.relay.video.publish_url" type="text" autocomplete="off" placeholder="rtmp://server.example:1935/uav-1" /></label>
          <label class="field"><span>启动超时（毫秒）</span><input v-model.number="form.relay.video.startup_timeout_ms" type="number" min="1" /></label>
          <label class="field"><span>无进度超时（毫秒）</span><input v-model.number="form.relay.video.stall_timeout_ms" type="number" min="1" /></label>
          <label class="field"><span>最大重试延迟（毫秒）</span><input v-model.number="form.relay.video.restart_max_delay_ms" type="number" min="1" /></label>
        </div>
        <p v-if="form.relay.video" class="muted">后台受控启动 FFmpeg，采用 RTSP/TCP 输入与不转码的 RTMP 推流；摄像头地址不会出现在状态快照中。</p>

        <div class="config-section-heading"><h3>伺服转台（固定 UDP 点播）</h3><button v-if="!form.relay.turntable" class="button secondary-button" type="button" @click="enableRelayTurntable">启用转台发送</button><button v-else class="button danger-button" type="button" @click="disableRelayTurntable">关闭转台发送</button></div>
        <div v-if="form.relay.turntable" class="form-grid nested-grid">
          <label class="field"><span>转台目标地址（IP:端口）</span><input v-model="form.relay.turntable.target_addr" type="text" autocomplete="off" placeholder="192.168.1.50:9003" /></label>
          <label class="field"><span>本地绑定地址（默认 0.0.0.0:0）</span><input v-model="form.relay.turntable.bind_addr" type="text" autocomplete="off" placeholder="0.0.0.0:0" /></label>
        </div>
        <p v-if="form.relay.turntable" class="muted">单向发给伺服转台；当从服务端收到符合 0xEB 0x90 0x02 0x01 0xC2 协议且 CRC16 正确的控制数据时，取出 32 字节发往该目标。</p>

        <div class="config-section-heading">
          <h3>IO 继电器控制器 (Modbus-RTU)</h3>
          <button v-if="!form.relay.io_controller" class="button secondary-button" type="button" @click="enableRelayIo">启用继电器控制</button>
          <button v-else class="button danger-button" type="button" @click="disableRelayIo">关闭继电器控制</button>
        </div>
        <div v-if="form.relay.io_controller" class="form-grid nested-grid">
          <label class="field">
            <span>串口号（实时检测）</span>
            <select v-model="form.relay.io_controller.port">
              <option v-if="isUnavailableSerialPort(form.relay.io_controller.port)" :value="form.relay.io_controller.port">
                {{ form.relay.io_controller.port }}（当前配置，未检测到）
              </option>
              <option v-for="port in serialPorts" :key="port" :value="port">{{ port }}</option>
            </select>
          </label>
          <label class="field"><span>波特率</span><input v-model.number="form.relay.io_controller.baud_rate" type="number" min="1200" /></label>
          <label class="field"><span>从站地址 (Slave ID)</span><input v-model.number="form.relay.io_controller.slave_id" type="number" min="1" max="255" /></label>
          <label class="field"><span>轮询间隔（毫秒）</span><input v-model.number="form.relay.io_controller.poll_interval_ms" type="number" min="100" /></label>
        </div>
        <p v-if="form.relay.io_controller" class="muted">连接朗汉德 LH-IO1600 等 16 路 Modbus-RTU 继电器模块，支持实时状态监听与远程开闭控制。</p>

        <div class="config-section-heading"><h3>飞机数据链路</h3><span>{{ relayLinks.length }} 条</span></div>
        <article v-for="(link, index) in relayLinks" :key="`${link.id}-${index}`" class="link-card">
          <div class="link-card-heading">
            <label class="field compact"><span>链路 ID</span><input v-model="link.id" /></label>
            <label class="field compact"><span>链路类型</span>
              <select :value="link.type" @change="changeLinkType(index, $event)">
                <option value="serial">串口</option>
                <option value="udp_unicast">UDP 单播</option>
                <option value="udp_multicast">UDP 组播</option>
              </select>
            </label>
            <button class="button danger-button" type="button" @click="removeLink(index)">删除链路</button>
          </div>

          <div v-if="link.type === 'serial'" class="form-grid nested-grid">
            <label class="field"><span>串口号（实时检测）</span>
              <select v-model="link.path">
                <option v-if="isUnavailableSerialPort(link.path)" :value="link.path">{{ link.path }}（当前配置，未检测到）</option>
                <option v-for="port in serialPorts" :key="port" :value="port">{{ port }}</option>
                <option v-if="serialPorts.length === 0 && !link.path" disabled value="">未检测到可用串口</option>
              </select>
            </label>
            <label class="field"><span>波特率</span><input v-model.number="link.baud_rate" type="number" min="1" /></label>
          </div>

          <div v-else-if="link.type === 'udp_unicast'" class="form-grid nested-grid">
            <label class="field"><span>绑定地址</span><input v-model="link.bind_addr" placeholder="0.0.0.0:9001" /></label>
            <label class="field"><span>发送目标</span><input v-model="link.tx_target" placeholder="192.168.1.20:9002" /></label>
            <label class="field"><span>允许来源（可选）</span><input v-model="link.allowed_source" placeholder="留空表示不限制" /></label>
          </div>

          <div v-else class="form-grid nested-grid">
            <label class="field"><span>绑定地址</span><input v-model="link.bind_addr" placeholder="0.0.0.0:10001" /></label>
            <label class="field"><span>组播地址</span><input v-model="link.group_addr" placeholder="239.10.10.10" /></label>
            <label class="field"><span>网卡地址</span><input v-model="link.interface_addr" placeholder="192.168.1.10" /></label>
            <label class="field"><span>发送目标</span><input v-model="link.tx_target" placeholder="239.10.10.10:10001" /></label>
            <label class="field"><span>组播 TTL</span><input v-model.number="link.multicast_ttl" type="number" min="1" max="255" /></label>
            <label class="check-field"><input v-model="link.multicast_loop" type="checkbox" /><span>启用本机组播回环</span></label>
          </div>
        </article>
        <div class="button-row">
          <button class="button secondary-button" type="button" @click="addLink('serial')">添加串口</button>
          <button class="button secondary-button" type="button" @click="addLink('udp_unicast')">添加 UDP 单播</button>
          <button class="button secondary-button" type="button" @click="addLink('udp_multicast')">添加 UDP 组播</button>
        </div>

        <div class="button-row save-row">
          <button class="button primary-button" type="button" :disabled="savingConfig || configLoading" @click="saveConfig">{{ savingConfig ? "保存中…" : "保存中继站配置" }}</button>
          <button class="button secondary-button" type="button" :disabled="restartingBackend || configLoading || !connected" @click="restartBackend">{{ restartingBackend ? "重启中…" : "重启后台服务" }}</button>
          <span v-if="configMessage" class="success-text">{{ configMessage }}</span>
          <span v-if="restartMessage" class="success-text">{{ restartMessage }}</span>
        </div>
      </div>
    </section>

    <template v-if="snapshot?.role === 'server'">
      <section class="summary-grid">
        <article class="metric"><span>地面站</span><strong>{{ state.ground_station_connected ? "已连接" : "断开" }}</strong></article>
        <article class="metric active-metric"><span>活动路径</span><strong>{{ state.active_path ? `${state.active_path.relay_id} / ${state.active_path.link_id}` : "无" }}</strong></article>
        <article class="metric"><span>中继数量</span><strong>{{ relays.length }}</strong></article>
        <article class="metric"><span>切换记录</span><strong>{{ (state.switches ?? []).length }}</strong></article>
        <article class="metric"><span>运行时间</span><strong>{{ state.uptime_seconds ?? 0 }} s</strong></article>
        <article class="metric"><span>最近错误</span><strong class="danger">{{ state.recent_error || "—" }}</strong></article>
      </section>
      <section class="panel">
        <div class="panel-heading">
          <h2>中继站列表与远程访问</h2>
          <span class="note">点击右侧按钮可通过服务端远程打开对应中继站的管理页或 L 链配置页</span>
        </div>
        <div v-if="relays.length === 0" class="config-body muted">
          当前暂无已连接的中继站，等待中继站上线…
        </div>
        <div v-for="relay in relays" :key="relay.relay_id" class="relay-block">
          <div class="relay-heading">
            <div class="relay-title">
              <span class="mono">{{ relay.relay_id }}</span>
              <span class="pill" :class="{ good: relay.connected }">{{ relay.connected ? "在线" : "离线" }}</span>
            </div>
            <div class="relay-actions">
              <button
                class="button primary-button"
                type="button"
                :disabled="!relay.connected"
                @click="openRelayWeb(relay.relay_id)"
              >
                打开中继管理页
              </button>
              <button
                class="button secondary-button"
                type="button"
                :disabled="!relay.connected"
                @click="openRelayLLink(relay.relay_id)"
              >
                打开 L 链配置页
              </button>
            </div>
          </div>
          <div v-for="link in relay.links" :key="link.path.link_id" class="path-row" :class="{ active: link.active }">
            <span class="mono">{{ link.path.link_id }}</span><span>{{ link.kind }}</span><span class="muted">RX {{ link.rx_bytes }} / TX {{ link.tx_bytes }}</span><span>{{ formatTimestamp(link.last_rx_at, "尚无数据") }}</span><span v-if="link.active" class="active-label">活动</span>
          </div>
        </div>
      </section>

      <section class="panel">
        <div class="panel-heading"><h2>服务端视频状态</h2><span class="pill" :class="{ good: video.stream_ready }">{{ videoStateLabel(video.state) }}</span></div>
        <div class="summary-grid">
          <article class="metric"><span>MediaMTX</span><strong>{{ video.worker_running ? "运行中" : "未运行" }}</strong></article>
          <article class="metric"><span>目标流</span><strong>{{ video.stream_ready ? "已发布" : "等待发布" }}</strong></article>
          <article class="metric"><span>重启次数</span><strong>{{ video.restart_count }}</strong></article>
          <article class="metric"><span>最后检查</span><strong class="mono">{{ formatTimestamp(video.last_progress_at) }}</strong></article>
        </div>
        <p v-if="video.last_error" class="danger">{{ video.last_error }}</p>
      </section>

      <section v-if="activeRole === 'server' && form.server" class="panel config-panel">
        <div class="panel-heading">
          <h2>服务端配置</h2>
          <span class="note">保存后点击“重启后台服务”生效</span>
        </div>
        <div v-if="configLoading" class="config-body muted">正在读取配置文件…</div>
        <div v-else class="config-body">
          <div class="form-grid">
            <label class="field"><span>服务端 ID</span><input v-model="form.server.id" /></label>
            <label class="field"><span>中继监听地址（IP:端口）</span><input v-model="form.server.listen_addr" placeholder="0.0.0.0:7443" /></label>
            <label class="field"><span>地面站监听地址（IP:端口）</span><input v-model="form.server.ground_station_listen_addr" placeholder="0.0.0.0:7500" /></label>
            <label class="field"><span>中继心跳超时（毫秒）</span><input v-model.number="form.server.heartbeat_timeout_ms" type="number" min="1" /></label>
            <label class="field"><span>飞机数据超时（毫秒）</span><input v-model.number="form.server.data_timeout_ms" type="number" min="1" /></label>
            <label class="field"><span>地面站心跳周期（毫秒）</span><input v-model.number="form.server.ground_station_heartbeat_interval_ms" type="number" min="1" /></label>
          </div>

          <div class="config-section-heading"><h3>视频接收</h3><button v-if="!form.server.video" class="button secondary-button" type="button" @click="enableServerVideo">启用视频接收</button><button v-else class="button danger-button" type="button" @click="disableServerVideo">关闭视频接收</button></div>
          <div v-if="form.server.video" class="form-grid nested-grid">
            <label class="field"><span>预期 RTMP 流路径</span><input v-model="form.server.video.stream_path" placeholder="uav-1" /></label>
            <label class="field"><span>本机 MediaMTX API</span><input v-model="form.server.video.control_api_addr" readonly /></label>
            <label class="field"><span>启动超时（毫秒）</span><input v-model.number="form.server.video.startup_timeout_ms" type="number" min="1" /></label>
            <label class="field"><span>无响应超时（毫秒）</span><input v-model.number="form.server.video.stall_timeout_ms" type="number" min="1" /></label>
            <label class="field"><span>最大重试延迟（毫秒）</span><input v-model.number="form.server.video.restart_max_delay_ms" type="number" min="1" /></label>
            <label class="field"><span>状态轮询周期（毫秒）</span><input v-model.number="form.server.video.poll_interval_ms" type="number" min="1" /></label>
          </div>
          <p v-if="form.server.video" class="muted">MediaMTX API 只绑定本机；“预期 RTMP 流路径”必须与中继站 RTMP 地址中端口后的路径一致。</p>

          <div class="button-row save-row">
            <button class="button primary-button" type="button" :disabled="savingConfig || configLoading" @click="saveConfig">{{ savingConfig ? "保存中…" : "保存服务端配置" }}</button>
            <button class="button secondary-button" type="button" :disabled="restartingBackend || configLoading || !connected" @click="restartBackend">{{ restartingBackend ? "重启中…" : "重启后台服务" }}</button>
            <span v-if="configMessage" class="success-text">{{ configMessage }}</span>
            <span v-if="restartMessage" class="success-text">{{ restartMessage }}</span>
          </div>
        </div>
      </section>
    </template>

    <footer>关闭窗口不会停止 relay-linkd 后台服务</footer>
  </main>
</template>

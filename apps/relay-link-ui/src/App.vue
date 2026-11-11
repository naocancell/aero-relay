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
  apiGetAuthInfo,
  getAuthHeader,
  setAuthCredentials,
  clearAuthCredentials,
  type AuthInfo,
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
const authInfo = ref<AuthInfo | null>(null);
const showLoginModal = ref(false);
const loginUsername = ref("");
const loginPassword = ref("");
const loginError = ref("");
const loginLoading = ref(false);

function openLoginModal() {
  loginUsername.value = "";
  loginPassword.value = "";
  loginError.value = "";
  showLoginModal.value = true;
}

function closeLoginModal() {
  showLoginModal.value = false;
  loginError.value = "";
}

function stopAllBackgroundTasks() {
  stopSubscription?.();
  stopSubscription = undefined;
  if (serialPortTimer !== undefined) {
    window.clearInterval(serialPortTimer);
    serialPortTimer = undefined;
  }
  configLoadToken += 1;
  configLoadInFlight = false;
  configLoading.value = false;
}

function clearSessionData() {
  snapshot.value = null;
  configDraft.value = null;
  savedConfigJson.value = "";
  serialPorts.value = [];
  connected.value = false;
  pendingRestart.value = false;
  configError.value = "";
  configMessage.value = "";
  restartError.value = "";
  restartMessage.value = "";
  ioError.value = "";
}

async function handleUnauthorized() {
  stopAllBackgroundTasks();
  clearAuthCredentials();
  clearSessionData();
  activeTab.value = "status";
  await refreshAuthInfo();
  error.value = "当前服务需要登录认证，请先登录";
  openLoginModal();
}

async function startAuthenticatedSession() {
  if (unmounted) return;
  if (serialPortTimer !== undefined) {
    window.clearInterval(serialPortTimer);
    serialPortTimer = undefined;
  }
  await restartSubscription();
  if (unmounted || (authInfo.value?.auth_enabled && !isSignedIn.value)) return;
  await loadSerialPorts();
  if (!unmounted && serialPortTimer === undefined) {
    serialPortTimer = window.setInterval(loadSerialPorts, 2000);
  }
}

async function handleLogin() {
  if (!loginUsername.value.trim()) {
    loginError.value = "请输入账号";
    return;
  }
  loginLoading.value = true;
  loginError.value = "";
  try {
    setAuthCredentials(loginUsername.value, loginPassword.value);
    const info = await apiGetAuthInfo(targetEndpoint.value);
    if (info.role === "anonymous") {
      clearAuthCredentials();
      loginError.value = "登录失败：用户名或密码错误";
      return;
    }
    authInfo.value = info;
    showLoginModal.value = false;
    error.value = "";
    await startAuthenticatedSession();
  } catch {
    clearAuthCredentials();
    loginError.value = "登录失败：用户名或密码错误";
  } finally {
    loginLoading.value = false;
  }
}

async function handleLogout() {
  stopAllBackgroundTasks();
  clearAuthCredentials();
  clearSessionData();
  activeTab.value = "status";
  const info = await refreshAuthInfo();
  if (!info || info.auth_enabled) {
    error.value = "已退出登录，请重新登录以访问设备";
    openLoginModal();
  } else {
    error.value = "";
    await startAuthenticatedSession();
  }
}


const targetAuth = computed(() => {
  if (form.value?.relay) {
    if (!form.value.relay.auth) {
      form.value.relay.auth = { enabled: true, username: "", password: "" };
    }
    return form.value.relay.auth;
  }
  if (form.value?.server) {
    if (!form.value.server.auth) {
      form.value.server.auth = { enabled: true, username: "", password: "" };
    }
    return form.value.server.auth;
  }
  return { enabled: true, username: "", password: "" };
});

async function refreshAuthInfo(): Promise<AuthInfo | null> {
  try {
    const info = await apiGetAuthInfo(targetEndpoint.value);
    authInfo.value = info;
    return info;
  } catch (err: any) {
    if (getAuthHeader() && (err?.status === 401 || String(err?.message || "").includes("401"))) {
      clearAuthCredentials();
      try {
        const fallbackInfo = await apiGetAuthInfo(targetEndpoint.value);
        authInfo.value = fallbackInfo;
        return fallbackInfo;
      } catch {
        // 忽略二次探测异常
      }
    }
    return authInfo.value;
  }
}

const restartError = ref("");
const savedConfigJson = ref("");
const pendingRestart = ref(false);
const activeTab = ref<"status" | "io" | "config">("status");
const configDirty = computed(() => {
  if (!configDraft.value || !savedConfigJson.value) return false;
  return JSON.stringify(normalizedConfig(configDraft.value)) !== savedConfigJson.value;
});
const tabs = computed(() => {
  const items: Array<{ id: "status" | "io" | "config"; label: string }> = [{ id: "status", label: "运行状态" }];
  if (activeRole.value === "relay") items.push({ id: "io", label: "IO 控制" });
  if (activeRole.value) items.push({ id: "config", label: "配置" });
  return items;
});
const nodeId = computed(() => {
  if (activeRole.value === "relay") return (state.value.relay_id as string) || configDraft.value?.relay?.id || "";
  if (activeRole.value === "server") return configDraft.value?.server?.id || "";
  return "";
});
const accountLabel = computed(() => {
  if (authInfo.value?.is_super_admin) return "admin";
  if (authInfo.value?.role === "custom_user") return authInfo.value.custom_username;
  return "访客";
});
const accountRole = computed(() => {
  if (authInfo.value?.is_super_admin) return "超级管理员";
  if (authInfo.value?.role === "custom_user") return "运维人员";
  return "未登录";
});
const isSignedIn = computed(() => Boolean(authInfo.value?.is_super_admin || authInfo.value?.role === "custom_user"));
const readyLinkCount = computed(() => links.value.filter((link) => link.adapter_ready).length);
const activeLocalLink = computed(() => links.value.find((link) => link.active)?.path.link_id ?? "");
const onlineRelayCount = computed(() => relays.value.filter((relay) => relay.connected).length);
const closedChannelCount = computed(() => ioController.value.channels.filter(Boolean).length);

const currentTab = computed(() => (tabs.value.some((tab) => tab.id === activeTab.value) ? activeTab.value : "status"));

function toggleModule(enabled: boolean, enable: () => void, disable: () => void) {
  if (enabled) enable();
  else disable();
}

function onModuleSwitch(event: globalThis.Event, enable: () => void, disable: () => void) {
  toggleModule((event.target as HTMLInputElement).checked, enable, disable);
}
const restartMessage = ref("");
let serialPortTimer: number | undefined;
let configLoadInFlight = false;
let configLoadToken = 0;
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
  if (!authInfo.value) refreshAuthInfo();
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
  if (authInfo.value?.auth_enabled && !isSignedIn.value) return;
  const currentToken = ++configLoadToken;
  configLoadInFlight = true;
  configLoading.value = true;
  configError.value = "";
  let attempt = 0;
  try {
    while (configDraft.value === null && !unmounted && currentToken === configLoadToken) {
      try {
        const value = await apiGetConfig(targetEndpoint.value);
        if (unmounted || currentToken !== configLoadToken) return;
        configDraft.value = cloneConfig(value);
        savedConfigJson.value = JSON.stringify(normalizedConfig(value));
        void refreshAuthInfo();
        configError.value = "";
      } catch (cause: any) {
        if (unmounted || currentToken !== configLoadToken) break;
        const isAuthErr = cause?.status === 401 || String(cause?.message || cause).includes("401");
        if (isAuthErr) {
          void handleUnauthorized();
          break;
        }
        configError.value = String(cause);
        await new Promise<void>((resolve) => window.setTimeout(resolve, retryDelay(attempt)));
        if (unmounted || currentToken !== configLoadToken) break;
        attempt += 1;
      }
    }
  } finally {
    if (currentToken === configLoadToken) {
      configLoading.value = false;
      configLoadInFlight = false;
    }
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
    if (candidate.relay?.auth) candidate.relay.auth.password = "";
    if (candidate.server?.auth) candidate.server.auth.password = "";
    configDraft.value = candidate;
    savedConfigJson.value = JSON.stringify(candidate);
    pendingRestart.value = true;
    restartMessage.value = "";
    configMessage.value = "配置已保存，请点击“重启后台服务”使其生效";
    await refreshAuthInfo();
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
    pendingRestart.value = false;
    configMessage.value = "";
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

function formatBytes(value?: number | null): string {
  const n = Number(value ?? 0);
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${i === 0 ? v : v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

function formatUptime(value?: number | null): string {
  const total = Math.max(0, Math.floor(Number(value ?? 0)));
  const days = Math.floor(total / 86400);
  const pad = (n: number) => n.toString().padStart(2, "0");
  const clock = `${pad(Math.floor((total % 86400) / 3600))}:${pad(Math.floor((total % 3600) / 60))}:${pad(total % 60)}`;
  return days > 0 ? `${days} 天 ${clock}` : clock;
}

function linkKindLabel(kind: string): string {
  const labels: Record<string, string> = {
    serial: "串口",
    udp_unicast: "UDP 单播",
    udp_multicast: "UDP 组播",
  };
  return labels[kind] ?? kind;
}

function videoTone(value: string): "ok" | "warn" | "bad" | "idle" {
  if (value === "publishing" || value === "online") return "ok";
  if (value === "backoff" || value === "stalled") return "bad";
  if (value === "starting" || value === "waiting_for_publisher") return "warn";
  return "idle";
}

function retryDelay(attempt: number): number {
  if (attempt === 0) return 1000;
  if (attempt === 1) return 2000;
  return 5000;
}

async function restartSubscription() {
  stopSubscription?.();
  stopSubscription = undefined;

  if (authInfo.value?.auth_enabled && !isSignedIn.value) {
    return;
  }

  const endpoint = targetEndpoint.value.trim();

  stopSubscription = await apiSubscribe(endpoint, {
    onSnapshot: (snap) => {
      if (unmounted) return;
      applySnapshot(snap);
      if (shouldLoadConfigAfterIpcReconnect(true, configDraft.value !== null, configLoadInFlight)) {
        void loadConfig();
      }
    },
    onStatusChange: (status, reason) => {
      if (unmounted) return;
      if (!status && reason === "401_unauthorized") {
        void handleUnauthorized();
        return;
      }
      connected.value = status;
      if (!status) {
        error.value = reason || "后台服务已断开";
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
  } catch (err: any) {
    const isAuthErr = err?.status === 401 || String(err?.message || "").includes("401");
    if (isAuthErr) {
      await handleUnauthorized();
      return;
    }
  }

  await loadConfig();
}

async function initializeEndpointSession() {
  stopAllBackgroundTasks();
  clearSessionData();
  const clean = targetEndpoint.value.trim();
  error.value = clean ? `正在连接 ${clean}...` : "正在连接本地服务...";
  const info = await refreshAuthInfo();
  if (info?.auth_enabled && !isSignedIn.value) {
    error.value = "当前服务需要登录认证，请先登录";
    openLoginModal();
    return;
  }
  await startAuthenticatedSession();
}

async function applyEndpoint() {
  const clean = targetEndpoint.value.trim();
  if (!clean.includes("/proxy/relay/")) {
    localStorage.setItem("relay_target_endpoint", clean);
  }
  await initializeEndpointSession();
}

onMounted(async () => {
  await initializeEndpointSession();
});

onUnmounted(() => {
  unmounted = true;
  stopAllBackgroundTasks();
});
</script>

<template>
  <div class="app">
    <header class="appbar">
      <div class="appbar-row">
        <div class="brand">
          <svg class="brand-mark" viewBox="0 0 20 20" aria-hidden="true">
            <circle cx="4" cy="14" r="2.2" />
            <circle cx="10" cy="5" r="2.2" />
            <circle cx="16" cy="14" r="2.2" />
            <path d="M5.2 12.2 8.8 6.8M11.2 6.8l3.6 5.4M6.4 14h7.2" />
          </svg>
          <span class="brand-name">relay-link</span>
          <span class="brand-divider" aria-hidden="true" />
          <span class="brand-role">{{ role }}</span>
          <span v-if="nodeId" class="brand-node mono">{{ nodeId }}</span>
        </div>

        <form class="endpoint" @submit.prevent="applyEndpoint">
          <label for="device-endpoint">设备地址</label>
          <input id="device-endpoint" v-model="targetEndpoint" placeholder="当前主机，或 192.168.0.2:7480" autocomplete="off" spellcheck="false" />
          <button class="btn" type="submit">连接</button>
        </form>

        <div class="appbar-meta">
          <span class="conn" :class="connected ? 'is-ok' : 'is-bad'" role="status">
            <span class="dot" aria-hidden="true" />{{ connected ? "后台已连接" : "后台已断开" }}
          </span>
          <div class="account">
            <button type="button" class="account-btn" title="登录或切换账号" @click="openLoginModal">
              <span class="account-name">{{ accountLabel }}</span>
              <span class="account-role">{{ accountRole }}</span>
            </button>
            <button v-if="isSignedIn" type="button" class="btn-text" title="退出当前登录" @click="handleLogout">退出</button>
          </div>
        </div>
      </div>

      <nav v-if="activeRole" class="tabs" role="tablist" aria-label="页面">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          type="button"
          role="tab"
          class="tab"
          :class="{ 'is-current': currentTab === tab.id }"
          :aria-selected="currentTab === tab.id"
          @click="activeTab = tab.id"
        >
          {{ tab.label }}
          <span v-if="tab.id === 'config' && (configDirty || pendingRestart)" class="tab-flag" :title="configDirty ? '有未保存的更改' : '已保存，待重启生效'" />
        </button>
      </nav>
    </header>

    <main class="page">
      <div v-if="error || configError || restartError" class="banner" role="alert">
        <span class="banner-mark" aria-hidden="true">!</span>
        <span>{{ error || configError || restartError }}</span>
      </div>

      <section v-if="!activeRole" class="waiting" aria-live="polite">
        <span class="waiting-bar" aria-hidden="true" />
        <h2>{{ authInfo?.auth_enabled && !isSignedIn ? "等待登录认证" : "正在连接后台" }}</h2>
        <p>{{ authInfo?.auth_enabled && !isSignedIn ? "当前设备已启用访问控制，请先完成账号登录。" : "正在获取设备角色与运行状态。" }}</p>
      </section>

      <!-- 中继站 · 运行状态 -->
      <template v-if="snapshot?.role === 'relay' && currentTab === 'status'">
        <dl class="readout">
          <div class="readout-item">
            <dt>服务器连接</dt>
            <dd><span class="state" :class="state.server_connected ? 'is-ok' : 'is-bad'"><span class="dot" />{{ state.server_connected ? "已连接" : "断开" }}</span></dd>
          </div>
          <div class="readout-item">
            <dt>活动链路</dt>
            <dd class="mono">{{ activeLocalLink || "—" }}</dd>
          </div>
          <div class="readout-item">
            <dt>适配器就绪</dt>
            <dd class="num">{{ readyLinkCount }}<span class="readout-sub"> / {{ links.length }}</span></dd>
          </div>
          <div class="readout-item">
            <dt>运行时间</dt>
            <dd class="num" :title="`${state.uptime_seconds ?? 0} s`">{{ formatUptime(state.uptime_seconds) }}</dd>
          </div>
        </dl>

        <section class="block">
          <header class="block-head">
            <h2>本地通信链路 <span class="count">{{ links.length }}</span></h2>
            <button class="btn" type="button" @click="openCurrentRelayLLink">打开本站 L 链配置页 <span aria-hidden="true">↗</span></button>
          </header>
          <div class="table-scroll">
            <table class="grid-table">
              <thead>
                <tr><th>Link ID</th><th>类型</th><th>适配器</th><th>最后接收</th><th class="right">RX</th><th class="right">TX</th><th>错误</th></tr>
              </thead>
              <tbody>
                <tr v-if="links.length === 0">
                  <td colspan="7" class="table-empty">暂无通信链路，可在“配置”中添加。</td>
                </tr>
                <tr v-for="link in links" :key="link.path.link_id" :class="{ 'is-active': link.active }">
                  <td class="mono strong">
                    {{ link.path.link_id }}
                    <span v-if="link.active" class="tag tag-accent">活动</span>
                  </td>
                  <td>{{ linkKindLabel(link.kind) }}</td>
                  <td><span class="state" :class="link.adapter_ready ? 'is-ok' : 'is-bad'"><span class="dot" />{{ link.adapter_ready ? "就绪" : "断开" }}</span></td>
                  <td class="mono soft">{{ formatTimestamp(link.last_rx_at, "尚无飞机数据") }}</td>
                  <td class="mono right" :title="`${link.rx_bytes} B`">{{ formatBytes(link.rx_bytes) }}</td>
                  <td class="mono right" :title="`${link.tx_bytes} B`">{{ formatBytes(link.tx_bytes) }}</td>
                  <td class="error-cell" :class="{ 'is-empty': !link.last_error }" :title="link.last_error || '—'">{{ link.last_error || "—" }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <div class="split">
          <section class="block">
            <header class="block-head">
              <h2>视频转发</h2>
              <span class="state" :class="`is-${videoTone(video.state)}`"><span class="dot" />{{ videoStateLabel(video.state) }}</span>
            </header>
            <dl class="facts">
              <div><dt>FFmpeg 子进程</dt><dd>{{ video.worker_running ? "运行中" : "未运行" }}</dd></div>
              <div><dt>推流就绪</dt><dd>{{ video.stream_ready ? "是" : "否" }}</dd></div>
              <div><dt>重启次数</dt><dd class="mono">{{ video.restart_count }}</dd></div>
              <div><dt>最后进度</dt><dd class="mono">{{ formatTimestamp(video.last_progress_at) }}</dd></div>
            </dl>
            <p v-if="video.last_error" class="inline-error">{{ video.last_error }}</p>
          </section>

          <section class="block">
            <header class="block-head">
              <h2>IO 继电器</h2>
              <span class="state" :class="!ioController.enabled ? 'is-idle' : ioController.connected ? 'is-ok' : 'is-bad'">
                <span class="dot" />{{ !ioController.enabled ? "未启用" : ioController.connected ? "已连接" : "离线" }}
              </span>
            </header>
            <template v-if="ioController.enabled">
              <div class="led-strip" :aria-label="`${closedChannelCount} 路闭合`">
                <span v-for="(isActive, index) in ioController.channels" :key="index" class="led" :class="{ on: isActive }" :title="`DO${index + 1} ${isActive ? '闭合' : '断开'}`" />
              </div>
              <div class="block-foot">
                <span class="soft"><span class="num strong">{{ closedChannelCount }}</span> / {{ ioController.channels.length }} 路闭合</span>
                <button class="btn" type="button" @click="activeTab = 'io'">前往控制</button>
              </div>
            </template>
            <p v-else class="soft">未启用继电器控制器，可在“配置”中开启。</p>
          </section>
        </div>
      </template>

      <!-- 中继站 · IO 控制 -->
      <template v-if="snapshot?.role === 'relay' && currentTab === 'io'">
        <section class="block">
          <header class="block-head">
            <div>
              <h2>IO 继电器控制器 <span class="soft">LH-IO1600</span></h2>
              <p class="block-desc">
                <span class="state" :class="!ioController.enabled ? 'is-idle' : ioController.connected ? 'is-ok' : 'is-bad'">
                  <span class="dot" />{{ !ioController.enabled ? "未配置启用" : ioController.connected ? "已连接 (RS485/Modbus)" : "未连接 / 离线" }}
                </span>
                <span v-if="ioController.last_update_at" class="soft"> · 更新于 <span class="mono">{{ formatTimestamp(ioController.last_update_at) }}</span></span>
              </p>
            </div>
            <div v-if="ioController.enabled" class="btn-group">
              <button class="btn" type="button" :disabled="!ioController.connected || ioOperating !== null" @click="setAllChannels(true)">全部闭合</button>
              <button class="btn" type="button" :disabled="!ioController.connected || ioOperating !== null" @click="setAllChannels(false)">全部断开</button>
            </div>
          </header>
          <p v-if="ioError || ioController.last_error" class="inline-error">{{ ioError || ioController.last_error }}</p>
          <div v-if="ioController.enabled" class="relay-bank">
            <button
              v-for="(isActive, index) in ioController.channels"
              :key="index"
              type="button"
              class="channel"
              :class="{ on: isActive, busy: ioOperating === index + 1 }"
              :aria-pressed="isActive"
              :disabled="!ioController.connected || ioOperating !== null"
              :title="isActive ? '点击断开' : '点击闭合'"
              @click="toggleChannel(index)"
            >
              <span class="channel-top">
                <span class="channel-name mono">DO{{ index + 1 }}</span>
                <span class="led" :class="{ on: isActive }" aria-hidden="true" />
              </span>
              <span class="channel-state">{{ ioOperating === index + 1 ? "操作中…" : isActive ? "闭合" : "断开" }}</span>
            </button>
          </div>
          <p v-else class="soft">未在系统配置中启用继电器控制器。可在“配置 › IO 继电器控制器”中开启。</p>
        </section>
      </template>

      <!-- 中继站 · 配置 -->
      <template v-if="activeRole === 'relay' && form.relay && currentTab === 'config'">
        <div v-if="configLoading" class="waiting compact"><span class="waiting-bar" aria-hidden="true" /><p>正在读取配置文件…</p></div>
        <div v-else class="settings">
          <section class="settings-group">
            <header class="settings-head">
              <h3>基本</h3>
              <p>中继站身份与服务端连接参数。</p>
            </header>
            <div class="fields">
              <label class="field"><span>中继站 ID</span><input v-model="form.relay.id" spellcheck="false" /></label>
              <label class="field"><span>服务端地址</span><input v-model="form.relay.server_addr" placeholder="192.168.1.10:7443" spellcheck="false" /><small>IP 或主机名:端口</small></label>
              <label class="field"><span>心跳周期</span><span class="unit-input"><input v-model.number="form.relay.heartbeat_interval_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>数据超时</span><span class="unit-input"><input v-model.number="form.relay.data_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
            </div>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <h3>飞机数据链路 <span class="count">{{ relayLinks.length }}</span></h3>
              <p>串口固定 8-N-1、无流控制；UDP 就绪仅表示本地已绑定。</p>
            </header>
            <div class="stack">
              <article v-for="(link, index) in relayLinks" :key="`${link.id}-${index}`" class="link-item">
                <div class="link-item-head">
                  <span class="link-index mono">{{ String(index + 1).padStart(2, "0") }}</span>
                  <label class="field"><span>链路 ID</span><input v-model="link.id" spellcheck="false" /></label>
                  <label class="field"><span>链路类型</span>
                    <select :value="link.type" @change="changeLinkType(index, $event)">
                      <option value="serial">串口</option>
                      <option value="udp_unicast">UDP 单播</option>
                      <option value="udp_multicast">UDP 组播</option>
                    </select>
                  </label>
                  <button class="btn-text danger" type="button" @click="removeLink(index)">删除</button>
                </div>

                <div v-if="link.type === 'serial'" class="fields">
                  <label class="field"><span>串口号</span>
                    <select v-model="link.path">
                      <option v-if="isUnavailableSerialPort(link.path)" :value="link.path">{{ link.path }}（当前配置，未检测到）</option>
                      <option v-for="port in serialPorts" :key="port" :value="port">{{ port }}</option>
                      <option v-if="serialPorts.length === 0 && !link.path" disabled value="">未检测到可用串口</option>
                    </select>
                    <small>列表实时检测</small>
                  </label>
                  <label class="field"><span>波特率</span><input v-model.number="link.baud_rate" type="number" min="1" /></label>
                </div>

                <div v-else-if="link.type === 'udp_unicast'" class="fields">
                  <label class="field"><span>绑定地址</span><input v-model="link.bind_addr" placeholder="0.0.0.0:9001" spellcheck="false" /></label>
                  <label class="field"><span>发送目标</span><input v-model="link.tx_target" placeholder="192.168.1.20:9002" spellcheck="false" /></label>
                  <label class="field"><span>允许来源</span><input v-model="link.allowed_source" placeholder="留空表示不限制" spellcheck="false" /><small>可选</small></label>
                </div>

                <div v-else class="fields">
                  <label class="field"><span>绑定地址</span><input v-model="link.bind_addr" placeholder="0.0.0.0:10001" spellcheck="false" /></label>
                  <label class="field"><span>组播地址</span><input v-model="link.group_addr" placeholder="239.10.10.10" spellcheck="false" /></label>
                  <label class="field"><span>网卡地址</span><input v-model="link.interface_addr" placeholder="192.168.1.10" spellcheck="false" /></label>
                  <label class="field"><span>发送目标</span><input v-model="link.tx_target" placeholder="239.10.10.10:10001" spellcheck="false" /></label>
                  <label class="field"><span>组播 TTL</span><input v-model.number="link.multicast_ttl" type="number" min="1" max="255" /></label>
                  <label class="check"><input v-model="link.multicast_loop" type="checkbox" /><span>启用本机组播回环</span></label>
                </div>
              </article>
              <div class="add-row">
                <span class="soft">添加链路</span>
                <button class="btn" type="button" @click="addLink('serial')">+ 串口</button>
                <button class="btn" type="button" @click="addLink('udp_unicast')">+ UDP 单播</button>
                <button class="btn" type="button" @click="addLink('udp_multicast')">+ UDP 组播</button>
              </div>
            </div>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>视频转发</h3>
                <label class="switch"><input type="checkbox" :checked="!!form.relay.video" @change="onModuleSwitch($event, enableRelayVideo, disableRelayVideo)" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用视频转发</span></label>
              </div>
              <p>后台受控启动 FFmpeg，RTSP/TCP 输入，不转码推送 RTMP。摄像头地址不会出现在状态快照中。</p>
            </header>
            <div v-if="form.relay.video" class="fields">
              <label class="field wide"><span>摄像头 RTSP 地址</span><input v-model="form.relay.video.source_url" type="text" autocomplete="off" placeholder="rtsp://camera.example/live" spellcheck="false" /></label>
              <label class="field wide"><span>服务端 RTMP 地址</span><input v-model="form.relay.video.publish_url" type="text" autocomplete="off" placeholder="rtmp://server.example:1935/uav-1" spellcheck="false" /></label>
              <label class="field"><span>启动超时</span><span class="unit-input"><input v-model.number="form.relay.video.startup_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>无进度超时</span><span class="unit-input"><input v-model.number="form.relay.video.stall_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>最大重试延迟</span><span class="unit-input"><input v-model.number="form.relay.video.restart_max_delay_ms" type="number" min="1" /><i>ms</i></span></label>
            </div>
            <p v-else class="off-note">已关闭</p>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>伺服转台</h3>
                <label class="switch"><input type="checkbox" :checked="!!form.relay.turntable" @change="onModuleSwitch($event, enableRelayTurntable, disableRelayTurntable)" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用转台发送</span></label>
              </div>
              <p>固定 UDP 单向发送。收到 <code>EB 90 02 01 C2</code> 且 CRC16 正确的控制数据时，取 32 字节发往目标。</p>
            </header>
            <div v-if="form.relay.turntable" class="fields">
              <label class="field"><span>转台目标地址</span><input v-model="form.relay.turntable.target_addr" type="text" autocomplete="off" placeholder="192.168.1.50:9003" spellcheck="false" /><small>IP:端口</small></label>
              <label class="field"><span>本地绑定地址</span><input v-model="form.relay.turntable.bind_addr" type="text" autocomplete="off" placeholder="0.0.0.0:0" spellcheck="false" /><small>默认 0.0.0.0:0</small></label>
            </div>
            <p v-else class="off-note">已关闭</p>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>IO 继电器控制器</h3>
                <label class="switch"><input type="checkbox" :checked="!!form.relay.io_controller" @change="onModuleSwitch($event, enableRelayIo, disableRelayIo)" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用继电器控制</span></label>
              </div>
              <p>Modbus-RTU，适配朗汉德 LH-IO1600 等 16 路继电器模块。</p>
            </header>
            <div v-if="form.relay.io_controller" class="fields">
              <label class="field">
                <span>串口号</span>
                <select v-model="form.relay.io_controller.port">
                  <option v-if="isUnavailableSerialPort(form.relay.io_controller.port)" :value="form.relay.io_controller.port">
                    {{ form.relay.io_controller.port }}（当前配置，未检测到）
                  </option>
                  <option v-for="port in serialPorts" :key="port" :value="port">{{ port }}</option>
                </select>
                <small>列表实时检测</small>
              </label>
              <label class="field"><span>波特率</span><input v-model.number="form.relay.io_controller.baud_rate" type="number" min="1200" /></label>
              <label class="field"><span>从站地址</span><input v-model.number="form.relay.io_controller.slave_id" type="number" min="1" max="255" /><small>Slave ID，1–255</small></label>
              <label class="field"><span>轮询间隔</span><span class="unit-input"><input v-model.number="form.relay.io_controller.poll_interval_ms" type="number" min="100" /><i>ms</i></span></label>
            </div>
            <p v-else class="off-note">已关闭</p>
          </section>

          <section v-if="authInfo?.is_super_admin" class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>访问控制</h3>
                <label class="switch"><input v-model="targetAuth.enabled" type="checkbox" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用中继站 Web 页面与 API 访问登录认证</span></label>
              </div>
              <p>开启后访问 Web 页面与 API 需要登录，可使用 admin 或下方运维账号。仅超级管理员可见。</p>
            </header>
            <div v-if="targetAuth.enabled" class="fields">
              <label class="field"><span>运维账号</span><input v-model="targetAuth.username" placeholder="operator" autocomplete="off" /><small>留空则仅超级管理员可登录</small></label>
              <label class="field"><span>运维密码</span><input v-model="targetAuth.password" type="password" placeholder="留空保持原密码不变，输入则重置密码" autocomplete="new-password" /><small>出于安全考虑不回显原密码</small></label>
            </div>
            <p v-else class="off-note">已关闭，所有人免密访问</p>
          </section>
        </div>
      </template>

      <!-- 服务端 · 运行状态 -->
      <template v-if="snapshot?.role === 'server' && currentTab === 'status'">
        <dl class="readout">
          <div class="readout-item">
            <dt>地面站</dt>
            <dd><span class="state" :class="state.ground_station_connected ? 'is-ok' : 'is-bad'"><span class="dot" />{{ state.ground_station_connected ? "已连接" : "断开" }}</span></dd>
          </div>
          <div class="readout-item readout-wide">
            <dt>活动路径</dt>
            <dd class="mono">{{ state.active_path ? `${state.active_path.relay_id} / ${state.active_path.link_id}` : "无" }}</dd>
          </div>
          <div class="readout-item">
            <dt>在线中继</dt>
            <dd class="num">{{ onlineRelayCount }}<span class="readout-sub"> / {{ relays.length }}</span></dd>
          </div>
          <div class="readout-item">
            <dt>切换次数</dt>
            <dd class="num">{{ (state.switches ?? []).length }}</dd>
          </div>
          <div class="readout-item readout-tail">
            <dt>运行时间</dt>
            <dd class="num" :title="`${state.uptime_seconds ?? 0} s`">{{ formatUptime(state.uptime_seconds) }}</dd>
          </div>
        </dl>
        <p v-if="state.recent_error" class="inline-error boxed"><strong>最近错误</strong>{{ state.recent_error }}</p>

        <section class="block">
          <header class="block-head">
            <h2>中继站 <span class="count">{{ relays.length }}</span></h2>
            <span class="soft hint">可经服务端远程打开中继站管理页或 L 链配置页</span>
          </header>
          <p v-if="relays.length === 0" class="table-empty">当前暂无已连接的中继站，等待中继站上线…</p>
          <div v-for="relay in relays" :key="relay.relay_id" class="relay" :class="{ offline: !relay.connected }">
            <div class="relay-head">
              <div class="relay-name">
                <span class="state" :class="relay.connected ? 'is-ok' : 'is-bad'"><span class="dot" />{{ relay.connected ? "在线" : "离线" }}</span>
                <span class="mono strong">{{ relay.relay_id }}</span>
                <span class="soft">{{ relay.links.length }} 条链路</span>
              </div>
              <div class="btn-group">
                <button class="btn" type="button" :disabled="!relay.connected" @click="openRelayWeb(relay.relay_id)">管理页 <span aria-hidden="true">↗</span></button>
                <button class="btn" type="button" :disabled="!relay.connected" @click="openRelayLLink(relay.relay_id)">L 链配置 <span aria-hidden="true">↗</span></button>
              </div>
            </div>
            <div v-if="relay.links.length === 0" class="relay-empty soft">该中继站暂无已配置的通信链路。</div>
            <div v-else class="table-scroll">
              <table class="grid-table sub">
                <colgroup><col style="width: 30%" /><col style="width: 16%" /><col style="width: 22%" /><col style="width: 16%" /><col style="width: 16%" /></colgroup>
                <thead><tr><th>Link ID</th><th>类型</th><th>最后接收</th><th class="right">RX</th><th class="right">TX</th></tr></thead>
                <tbody>
                  <tr v-for="link in relay.links" :key="link.path.link_id" :class="{ 'is-active': link.active }">
                    <td class="mono strong">{{ link.path.link_id }} <span v-if="link.active" class="tag tag-accent">活动</span></td>
                    <td>{{ linkKindLabel(link.kind) }}</td>
                    <td class="mono soft">{{ formatTimestamp(link.last_rx_at, "尚无数据") }}</td>
                    <td class="mono right" :title="`${link.rx_bytes} B`">{{ formatBytes(link.rx_bytes) }}</td>
                    <td class="mono right" :title="`${link.tx_bytes} B`">{{ formatBytes(link.tx_bytes) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </section>

        <section class="block">
          <header class="block-head">
            <h2>视频接收</h2>
            <span class="state" :class="`is-${videoTone(video.state)}`"><span class="dot" />{{ videoStateLabel(video.state) }}</span>
          </header>
          <dl class="facts row">
            <div><dt>MediaMTX</dt><dd>{{ video.worker_running ? "运行中" : "未运行" }}</dd></div>
            <div><dt>目标流</dt><dd>{{ video.stream_ready ? "已发布" : "等待发布" }}</dd></div>
            <div><dt>重启次数</dt><dd class="mono">{{ video.restart_count }}</dd></div>
            <div><dt>最后检查</dt><dd class="mono">{{ formatTimestamp(video.last_progress_at) }}</dd></div>
          </dl>
          <p v-if="video.last_error" class="inline-error">{{ video.last_error }}</p>
        </section>
      </template>

      <!-- 服务端 · 配置 -->
      <template v-if="activeRole === 'server' && form.server && currentTab === 'config'">
        <div v-if="configLoading" class="waiting compact"><span class="waiting-bar" aria-hidden="true" /><p>正在读取配置文件…</p></div>
        <div v-else class="settings">
          <section class="settings-group">
            <header class="settings-head">
              <h3>基本</h3>
              <p>中继站接入与地面站服务的监听地址。</p>
            </header>
            <div class="fields">
              <label class="field"><span>服务端 ID</span><input v-model="form.server.id" spellcheck="false" /></label>
              <label class="field"><span>中继监听地址</span><input v-model="form.server.listen_addr" placeholder="0.0.0.0:7443" spellcheck="false" /><small>IP:端口</small></label>
              <label class="field"><span>地面站监听地址</span><input v-model="form.server.ground_station_listen_addr" placeholder="0.0.0.0:7500" spellcheck="false" /><small>IP:端口</small></label>
            </div>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <h3>超时与心跳</h3>
              <p>当前路径失效后，才会切换到最近收到飞机数据的候选路径。</p>
            </header>
            <div class="fields">
              <label class="field"><span>中继心跳超时</span><span class="unit-input"><input v-model.number="form.server.heartbeat_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>飞机数据超时</span><span class="unit-input"><input v-model.number="form.server.data_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>地面站心跳周期</span><span class="unit-input"><input v-model.number="form.server.ground_station_heartbeat_interval_ms" type="number" min="1" /><i>ms</i></span></label>
            </div>
          </section>

          <section class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>视频接收</h3>
                <label class="switch"><input type="checkbox" :checked="!!form.server.video" @change="onModuleSwitch($event, enableServerVideo, disableServerVideo)" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用视频接收</span></label>
              </div>
              <p>MediaMTX API 只绑定本机。流路径必须与中继站 RTMP 地址中端口后的路径一致。</p>
            </header>
            <div v-if="form.server.video" class="fields">
              <label class="field"><span>预期 RTMP 流路径</span><input v-model="form.server.video.stream_path" placeholder="uav-1" spellcheck="false" /></label>
              <label class="field"><span>本机 MediaMTX API</span><input v-model="form.server.video.control_api_addr" readonly /><small>只读</small></label>
              <label class="field"><span>启动超时</span><span class="unit-input"><input v-model.number="form.server.video.startup_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>无响应超时</span><span class="unit-input"><input v-model.number="form.server.video.stall_timeout_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>最大重试延迟</span><span class="unit-input"><input v-model.number="form.server.video.restart_max_delay_ms" type="number" min="1" /><i>ms</i></span></label>
              <label class="field"><span>状态轮询周期</span><span class="unit-input"><input v-model.number="form.server.video.poll_interval_ms" type="number" min="1" /><i>ms</i></span></label>
            </div>
            <p v-else class="off-note">已关闭</p>
          </section>

          <section v-if="authInfo?.is_super_admin" class="settings-group">
            <header class="settings-head">
              <div class="settings-title">
                <h3>访问控制</h3>
                <label class="switch"><input v-model="targetAuth.enabled" type="checkbox" /><span class="switch-track" aria-hidden="true" /><span class="sr-only">启用服务端 Web 页面与 API 访问登录认证</span></label>
              </div>
              <p>开启后访问 Web 页面与 API 需要登录，可使用 admin 或下方运维账号。仅超级管理员可见。</p>
            </header>
            <div v-if="targetAuth.enabled" class="fields">
              <label class="field"><span>运维账号</span><input v-model="targetAuth.username" placeholder="operator" autocomplete="off" /><small>留空则仅超级管理员可登录</small></label>
              <label class="field"><span>运维密码</span><input v-model="targetAuth.password" type="password" placeholder="留空保持原密码不变，输入则重置密码" autocomplete="new-password" /><small>出于安全考虑不回显原密码</small></label>
            </div>
            <p v-else class="off-note">已关闭，所有人免密访问</p>
          </section>
        </div>
      </template>
    </main>

    <div v-if="currentTab === 'config' && configDraft && !configLoading" class="savebar">
      <div class="savebar-inner">
        <span class="savebar-status" :class="{ 'is-warn': configDirty || pendingRestart }" role="status">
          <template v-if="restartMessage && !configDirty">{{ restartMessage }}</template>
          <template v-else-if="configDirty">有未保存的更改</template>
          <template v-else-if="pendingRestart">配置已保存，重启后台服务后生效</template>
          <template v-else>配置与文件一致</template>
        </span>
        <div class="btn-group">
          <button class="btn" type="button" :disabled="restartingBackend || configLoading || !connected" @click="restartBackend">{{ restartingBackend ? "重启中…" : "重启后台服务" }}</button>
          <button class="btn btn-primary" type="button" :disabled="savingConfig || configLoading" @click="saveConfig">{{ savingConfig ? "保存中…" : activeRole === "relay" ? "保存中继站配置" : "保存服务端配置" }}</button>
        </div>
      </div>
    </div>

    <footer class="foot">关闭窗口不会停止 relay-linkd 后台服务</footer>

    <div v-if="showLoginModal" class="modal-backdrop">
      <form class="modal" role="dialog" aria-modal="true" aria-labelledby="login-modal-title" @submit.prevent="handleLogin">
        <header class="modal-head">
          <h3 id="login-modal-title">账号登录</h3>
          <button class="modal-close" type="button" aria-label="关闭登录窗口" @click="closeLoginModal">
            <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" /></svg>
          </button>
        </header>
        <div class="modal-body">
          <p class="soft">超级管理员可解锁全部权限与访问控制配置，也可以使用运维账号登录。</p>
          <div v-if="loginError" class="inline-error boxed" role="alert">{{ loginError }}</div>
          <label class="field">
            <span>账号</span>
            <input v-model="loginUsername" autocomplete="off" autofocus placeholder="请输入账号" />
          </label>
          <label class="field">
            <span>密码</span>
            <input v-model="loginPassword" type="password" autocomplete="current-password" placeholder="请输入密码" />
          </label>
        </div>
        <footer class="modal-foot">
          <button class="btn" type="button" @click="closeLoginModal">取消</button>
          <button class="btn btn-primary" type="submit" :disabled="loginLoading">{{ loginLoading ? "正在验证…" : "登录" }}</button>
        </footer>
      </form>
    </div>
  </div>
</template>

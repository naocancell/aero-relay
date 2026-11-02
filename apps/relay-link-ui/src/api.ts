// 统一前后端通信 API 适配层（支持 Tauri 原生桌面与纯浏览器 Web 访问）

export type SnapshotPayload = {
  role: "relay" | "server";
  state?: Record<string, unknown>;
};

export type LinkType = "serial" | "udp_unicast" | "udp_multicast";

export type RelayLink = {
  id: string;
  type: LinkType;
  [key: string]: string | number | boolean | null;
};

export type RelayVideoConfig = {
  source_url: string;
  publish_url: string;
  startup_timeout_ms: number;
  stall_timeout_ms: number;
  restart_max_delay_ms: number;
};

export type ServerVideoConfig = {
  stream_path: string;
  control_api_addr: string;
  startup_timeout_ms: number;
  stall_timeout_ms: number;
  restart_max_delay_ms: number;
  poll_interval_ms: number;
};

export type VideoSnapshot = {
  state: string;
  worker_running: boolean;
  stream_ready: boolean;
  restart_count: number;
  last_progress_at?: string | null;
  last_error?: string | null;
};

export type RelayTurntableConfig = {
  bind_addr: string;
  target_addr: string;
};

export type RelayConfig = {
  id: string;
  server_addr: string;
  heartbeat_interval_ms: number;
  data_timeout_ms: number;
  links: RelayLink[];
  video: RelayVideoConfig | null;
  turntable?: RelayTurntableConfig | null;
};

export type ServerConfig = {
  id: string;
  listen_addr: string;
  ground_station_listen_addr: string;
  ground_station_heartbeat_interval_ms: number;
  heartbeat_timeout_ms: number;
  data_timeout_ms: number;
  video: ServerVideoConfig | null;
};

export type Config = {
  relay: RelayConfig | null;
  server: ServerConfig | null;
};

export function isTauri(): boolean {
  return typeof window !== "undefined" && ("__TAURI_INTERNALS__" in window || "__TAURI__" in window);
}

// 标准化目标 HTTP 服务基准地址
export function resolveHttpBase(target: string): string {
  const clean = target.trim();
  if (!clean) {
    if (typeof window !== "undefined" && window.location && window.location.origin && window.location.origin.startsWith("http")) {
      return window.location.origin;
    }
    return "http://127.0.0.1:8080";
  }
  if (clean.startsWith("http://") || clean.startsWith("https://")) {
    return clean.replace(/\/+$/, "");
  }
  // 如果输入纯 IP 或 IP:端口
  if (!clean.includes(":")) {
    return `http://${clean}:8080`;
  }
  return `http://${clean}`;
}

// 获取快照
export async function apiGetSnapshot(target: string): Promise<SnapshotPayload> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    return await invoke<SnapshotPayload>("get_snapshot");
  }
  const base = resolveHttpBase(target);
  const resp = await fetch(`${base}/api/snapshot`);
  if (!resp.ok) {
    throw new Error(`获取快照失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 获取配置
export async function apiGetConfig(target: string): Promise<Config> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    return await invoke<Config>("get_config");
  }
  const base = resolveHttpBase(target);
  const resp = await fetch(`${base}/api/config`);
  if (!resp.ok) {
    throw new Error(`获取配置失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 保存配置
export async function apiSaveConfig(target: string, config: Config): Promise<void> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    return await invoke("save_config", { config });
  }
  const base = resolveHttpBase(target);
  const resp = await fetch(`${base}/api/config`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!resp.ok) {
    throw new Error(`保存配置失败 (${resp.status}): ${await resp.text()}`);
  }
}

// 获取可用串口列表
export async function apiGetSerialPorts(target: string): Promise<string[]> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    return await invoke<string[]>("get_serial_ports");
  }
  const base = resolveHttpBase(target);
  const resp = await fetch(`${base}/api/serial-ports`);
  if (!resp.ok) {
    throw new Error(`获取串口列表失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 重启后台服务
export async function apiRestartBackend(target: string): Promise<void> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    return await invoke("restart_backend");
  }
  const base = resolveHttpBase(target);
  const resp = await fetch(`${base}/api/restart`, {
    method: "POST",
  });
  if (!resp.ok) {
    throw new Error(`重启服务失败 (${resp.status}): ${await resp.text()}`);
  }
}

// 设置 Tauri 目标端点（仅桌面环境调用）
export async function apiSetEndpoint(endpoint: string): Promise<void> {
  if (isTauri()) {
    const { invoke } = await import("@tauri-apps/api/core");
    await invoke("set_endpoint", { endpoint });
  }
}

// 订阅实时状态变化
export async function apiSubscribe(
  target: string,
  callbacks: {
    onSnapshot: (snapshot: SnapshotPayload) => void;
    onStatusChange: (connected: boolean) => void;
  }
): Promise<() => void> {
  if (isTauri()) {
    const { listen } = await import("@tauri-apps/api/event");
    const { invoke } = await import("@tauri-apps/api/core");

    const stopIpcStatus = await listen<boolean>("ipc-status", (event) => {
      callbacks.onStatusChange(event.payload);
    });
    const stopStateChanged = await listen<SnapshotPayload>("state-changed", (event) => {
      callbacks.onSnapshot(event.payload);
    });

    try {
      await invoke("subscribe_snapshot");
    } catch {}

    return () => {
      stopIpcStatus();
      stopStateChanged();
    };
  }

  // 纯 Web 浏览器环境：采用 Server-Sent Events (SSE) + 优雅重试机制
  let active = true;
  let es: EventSource | null = null;
  let pollTimer: number | undefined;

  const base = resolveHttpBase(target);

  function connectSSE() {
    if (!active) return;
    try {
      es = new EventSource(`${base}/api/events`);
      es.onopen = () => {
        if (!active) return;
        callbacks.onStatusChange(true);
      };
      es.onmessage = (event) => {
        if (!active) return;
        try {
          const snapshot = JSON.parse(event.data);
          callbacks.onSnapshot(snapshot);
        } catch {}
      };
      es.onerror = () => {
        if (!active) return;
        callbacks.onStatusChange(false);
      };
    } catch {
      callbacks.onStatusChange(false);
    }
  }

  connectSSE();

  // 辅助周期性状态兜底刷新（每 3 秒一次）
  pollTimer = window.setInterval(async () => {
    if (!active) return;
    try {
      const snap = await apiGetSnapshot(target);
      if (active) {
        callbacks.onSnapshot(snap);
        callbacks.onStatusChange(true);
      }
    } catch {
      if (active && (!es || es.readyState !== EventSource.OPEN)) {
        callbacks.onStatusChange(false);
      }
    }
  }, 3000);

  return () => {
    active = false;
    if (pollTimer) clearInterval(pollTimer);
    if (es) {
      es.close();
      es = null;
    }
  };
}

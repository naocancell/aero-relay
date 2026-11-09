// 纯 Web 前端 HTTP REST API 与 SSE 通信客户端

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

export type RelayIoControllerConfig = {
  port: string;
  baud_rate: number;
  slave_id: number;
  poll_interval_ms: number;
};

export type IoControllerSnapshot = {
  enabled: boolean;
  connected: boolean;
  channels: boolean[];
  last_update_at?: string | null;
  last_error?: string | null;
};

export type AuthConfig = {
  username?: string;
  password?: string;
};

export type RelayConfig = {
  auth?: AuthConfig | null;
  id: string;
  server_addr: string;
  heartbeat_interval_ms: number;
  data_timeout_ms: number;
  links: RelayLink[];
  video: RelayVideoConfig | null;
  turntable?: RelayTurntableConfig | null;
  io_controller?: RelayIoControllerConfig | null;
};

export type ServerConfig = {
  auth?: AuthConfig | null;
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

// 标准化目标 HTTP 服务基准地址

let currentAuthHeader: string =
  typeof localStorage !== "undefined"
    ? localStorage.getItem("relay_auth_header") || ""
    : "";

export function getAuthHeader(): string {
  return currentAuthHeader;
}

export function setAuthCredentials(username: string, password: string): void {
  const cleanUser = username.trim();
  const encoded = btoa(`${cleanUser}:${password}`);
  currentAuthHeader = `Basic ${encoded}`;
  if (typeof localStorage !== "undefined") {
    localStorage.setItem("relay_auth_header", currentAuthHeader);
    localStorage.setItem("relay_auth_user", cleanUser);
  }
}

export function clearAuthCredentials(): void {
  currentAuthHeader = "";
  if (typeof localStorage !== "undefined") {
    localStorage.removeItem("relay_auth_header");
    localStorage.removeItem("relay_auth_user");
  }
}

export function getStoredAuthUser(): string {
  return typeof localStorage !== "undefined"
    ? localStorage.getItem("relay_auth_user") || ""
    : "";
}

async function authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const headers = new Headers(options.headers || {});
  if (currentAuthHeader && !headers.has("Authorization")) {
    headers.set("Authorization", currentAuthHeader);
  }
  return fetch(url, {
    ...options,
    headers,
    credentials: "include",
  });
}

export function resolveHttpBase(target: string): string {
  const clean = target.trim();
  const currentOrigin =
    typeof window !== "undefined" &&
    window.location &&
    window.location.origin &&
    window.location.origin.startsWith("http")
      ? window.location.origin
      : "http://127.0.0.1:7480";
  if (!clean) {
    return currentOrigin;
  }
  if (clean.startsWith("/")) {
    return `${currentOrigin}${clean.replace(/\/+$/, "")}`;
  }
  if (clean.startsWith("http://") || clean.startsWith("https://")) {
    return clean.replace(/\/+$/, "");
  }
  if (!clean.includes(":")) {
    return `http://${clean}:7480`;
  }
  return `http://${clean}`;
}

// 获取快照
export async function apiGetSnapshot(target: string): Promise<SnapshotPayload> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/snapshot`);
  if (!resp.ok) {
    throw new Error(`获取快照失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 获取配置
export async function apiGetConfig(target: string): Promise<Config> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/config`);
  if (!resp.ok) {
    throw new Error(`获取配置失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 保存配置
export async function apiSaveConfig(target: string, config: Config): Promise<void> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/config`, {
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
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/serial-ports`);
  if (!resp.ok) {
    throw new Error(`获取串口列表失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

// 重启后台服务
export async function apiRestartBackend(target: string): Promise<void> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/restart`, {
    method: "POST",
  });
  if (!resp.ok) {
    throw new Error(`重启服务失败 (${resp.status}): ${await resp.text()}`);
  }
}

// 控制继电器单路开关
export async function apiSetIoChannel(target: string, channel: number, state: boolean): Promise<void> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/io/channel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ channel, state }),
  });
  if (!resp.ok) {
    throw new Error(`控制通道失败 (${resp.status}): ${await resp.text()}`);
  }
}

// 批量控制继电器全开/全关
export async function apiSetIoAll(target: string, state: boolean): Promise<void> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/io/all`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ state }),
  });
  if (!resp.ok) {
    throw new Error(`批量控制失败 (${resp.status}): ${await resp.text()}`);
  }
}

// 订阅实时状态变化（SSE 流式推送 + 周期性保底刷新）
export async function apiSubscribe(
  target: string,
  callbacks: {
    onSnapshot: (snapshot: SnapshotPayload) => void;
    onStatusChange: (connected: boolean) => void;
  }
): Promise<() => void> {
  let active = true;
  let es: EventSource | null = null;
  let pollTimer: number | undefined;

  const base = resolveHttpBase(target);
  const isProxiedRelay = base.includes("/proxy/relay/");

  function connectSSE() {
    if (!active || isProxiedRelay) return;
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

  // 周期性状态刷新（远程代理模式下每 1 秒轮询，直连模式下每 3 秒兜底刷新）
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
  }, isProxiedRelay ? 1000 : 3000);

  return () => {
    active = false;
    if (pollTimer) clearInterval(pollTimer);
    if (es) {
      es.close();
      es = null;
    }
  };
}

export type AuthInfo = {
  role: "super_admin" | "custom_user" | "anonymous";
  is_super_admin: boolean;
  auth_enabled: boolean;
  custom_username: string;
};

export async function apiGetAuthInfo(target: string): Promise<AuthInfo> {
  const base = resolveHttpBase(target);
  const resp = await authFetch(`${base}/api/auth/me`);
  if (!resp.ok) {
    throw new Error(`获取认证信息失败 (${resp.status}): ${await resp.text()}`);
  }
  return await resp.json();
}

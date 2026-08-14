use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::config::LinkKind;

pub const VIDEO_OWNERSHIP_LEASE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PathId {
    pub relay_id: String,
    pub link_id: String,
}

impl PathId {
    pub fn new(relay_id: impl Into<String>, link_id: impl Into<String>) -> Self {
        Self {
            relay_id: relay_id.into(),
            link_id: link_id.into(),
        }
    }
}

impl fmt::Display for PathId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.relay_id, self.link_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Relay,
    Server,
}

#[derive(Debug, Clone, Default)]
pub struct LinkHealth {
    pub adapter_ready: bool,
    pub last_rx_at: Option<Instant>,
    pub last_tx_at: Option<Instant>,
    pub last_rx_at_epoch_ms: Option<u64>,
    pub last_tx_at_epoch_ms: Option<u64>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_rate_bytes_per_second: f64,
    pub tx_rate_bytes_per_second: f64,
    pub last_error: Option<String>,
}

impl LinkHealth {
    pub fn mark_rx(&mut self, now: Instant, bytes: usize) {
        if let Some(previous) = self.last_rx_at
            && let Some(seconds) = now
                .checked_duration_since(previous)
                .map(|duration| duration.as_secs_f64())
                .filter(|seconds| *seconds > 0.0)
        {
            self.rx_rate_bytes_per_second = bytes as f64 / seconds;
        }
        self.last_rx_at = Some(now);
        self.last_rx_at_epoch_ms = Some(unix_timestamp_ms());
        self.rx_bytes = self.rx_bytes.saturating_add(bytes as u64);
    }

    pub fn mark_tx(&mut self, now: Instant, bytes: usize) {
        if let Some(previous) = self.last_tx_at
            && let Some(seconds) = now
                .checked_duration_since(previous)
                .map(|duration| duration.as_secs_f64())
                .filter(|seconds| *seconds > 0.0)
        {
            self.tx_rate_bytes_per_second = bytes as f64 / seconds;
        }
        self.last_tx_at = Some(now);
        self.last_tx_at_epoch_ms = Some(unix_timestamp_ms());
        self.tx_bytes = self.tx_bytes.saturating_add(bytes as u64);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkSnapshot {
    pub path: PathId,
    pub kind: String,
    pub adapter_ready: bool,
    pub last_rx_at: Option<String>,
    pub last_tx_at: Option<String>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_rate_bytes_per_second: f64,
    pub tx_rate_bytes_per_second: f64,
    pub last_error: Option<String>,
    pub active: bool,
}

impl LinkSnapshot {
    pub fn from_health(path: PathId, kind: &LinkKind, health: &LinkHealth, active: bool) -> Self {
        Self {
            path,
            kind: kind.name().into(),
            adapter_ready: health.adapter_ready,
            last_rx_at: health.last_rx_at_epoch_ms.map(|value| value.to_string()),
            last_tx_at: health.last_tx_at_epoch_ms.map(|value| value.to_string()),
            rx_bytes: health.rx_bytes,
            tx_bytes: health.tx_bytes,
            rx_rate_bytes_per_second: health.rx_rate_bytes_per_second,
            tx_rate_bytes_per_second: health.tx_rate_bytes_per_second,
            last_error: health.last_error.clone(),
            active,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwitchRecord {
    pub at: String,
    pub from: Option<PathId>,
    pub to: Option<PathId>,
    pub reason: String,
}

/// Operator-visible state for the local video worker. The snapshot deliberately
/// contains health only: camera and publishing URLs can contain credentials and
/// remain in the role configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoState {
    #[default]
    Disabled,
    Standby,
    Starting,
    Publishing,
    WaitingForPublisher,
    Online,
    Backoff,
    Stalled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoSnapshot {
    pub state: VideoState,
    pub worker_running: bool,
    pub stream_ready: bool,
    pub restart_count: u64,
    pub last_progress_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelaySnapshot {
    pub role: Role,
    pub relay_id: String,
    pub server_connected: bool,
    pub links: Vec<LinkSnapshot>,
    pub uptime_seconds: u64,
    pub recent_error: Option<String>,
    #[serde(default)]
    pub video: VideoSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerRelaySnapshot {
    pub relay_id: String,
    pub connected: bool,
    pub last_heartbeat_at: Option<String>,
    pub links: Vec<LinkSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerSnapshot {
    pub role: Role,
    pub ground_station_connected: bool,
    pub active_path: Option<PathId>,
    pub relays: Vec<ServerRelaySnapshot>,
    pub switches: Vec<SwitchRecord>,
    pub uptime_seconds: u64,
    pub recent_error: Option<String>,
    #[serde(default)]
    pub video: VideoSnapshot,
}

fn unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

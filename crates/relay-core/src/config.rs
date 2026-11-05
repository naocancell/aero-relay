use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const GROUND_STATION_HEARTBEAT_PATH_METADATA_LIMIT: usize = 22;
const MEDIAMTX_CONTROL_API_ADDR: &str = "127.0.0.1:9997";

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Config {
    pub relay: Option<RelayConfig>,
    pub server: Option<ServerConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RelayConfig {
    pub id: String,
    pub server_addr: String,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_data_timeout_ms")]
    pub data_timeout_ms: u64,
    #[serde(default)]
    pub links: Vec<LinkConfig>,
    #[serde(default)]
    pub video: Option<RelayVideoConfig>,
    #[serde(default)]
    pub turntable: Option<TurntableConfig>,
    #[serde(default)]
    pub io_controller: Option<IoControllerConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct IoControllerConfig {
    #[serde(default = "default_io_port")]
    pub port: String,
    #[serde(default = "default_io_baud_rate")]
    pub baud_rate: u32,
    #[serde(default = "default_io_slave_id")]
    pub slave_id: u8,
    #[serde(default = "default_io_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TurntableConfig {
    #[serde(default = "default_turntable_bind_addr")]
    pub bind_addr: String,
    #[serde(alias = "tx_target")]
    pub target_addr: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ServerConfig {
    pub id: String,
    pub listen_addr: String,
    #[serde(alias = "ground_station_addr")]
    pub ground_station_listen_addr: String,
    #[serde(default = "default_ground_station_heartbeat_interval_ms")]
    pub ground_station_heartbeat_interval_ms: u64,
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: u64,
    #[serde(default = "default_data_timeout_ms")]
    pub data_timeout_ms: u64,
    #[serde(default)]
    pub video: Option<ServerVideoConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RelayVideoConfig {
    pub source_url: String,
    pub publish_url: String,
    #[serde(default = "default_video_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "default_video_stall_timeout_ms")]
    pub stall_timeout_ms: u64,
    #[serde(default = "default_video_restart_max_delay_ms")]
    pub restart_max_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ServerVideoConfig {
    pub stream_path: String,
    #[serde(default = "default_video_control_api_addr")]
    pub control_api_addr: String,
    #[serde(default = "default_video_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
    #[serde(default = "default_video_stall_timeout_ms")]
    pub stall_timeout_ms: u64,
    #[serde(default = "default_video_restart_max_delay_ms")]
    pub restart_max_delay_ms: u64,
    #[serde(default = "default_video_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LinkConfig {
    pub id: String,
    #[serde(default)]
    pub priority: u32,
    #[serde(flatten)]
    pub kind: LinkKind,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum LinkKind {
    #[serde(rename = "serial")]
    Serial { path: String, baud_rate: u32 },
    #[serde(rename = "udp_unicast")]
    UdpUnicast {
        bind_addr: String,
        tx_target: String,
        #[serde(default)]
        allowed_source: Option<String>,
    },
    #[serde(rename = "udp_multicast")]
    UdpMulticast {
        bind_addr: String,
        group_addr: String,
        interface_addr: String,
        tx_target: String,
        #[serde(default = "default_multicast_ttl")]
        multicast_ttl: u32,
        #[serde(default)]
        multicast_loop: bool,
    },
}

impl LinkKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Serial { .. } => "serial",
            Self::UdpUnicast { .. } => "udp_unicast",
            Self::UdpMulticast { .. } => "udp_multicast",
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("at least one of [relay] or [server] must be configured")]
    InvalidRole,
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("link id `{id}` is duplicated")]
    DuplicateLinkId { id: String },
    #[error(
        "path `{relay_id}/{link_id}` metadata is {bytes} bytes but the heartbeat limit is {limit}"
    )]
    PathMetadataTooLong {
        relay_id: String,
        link_id: String,
        bytes: usize,
        limit: usize,
    },
    #[error("{field} is not a valid socket address: {value}")]
    InvalidSocketAddress { field: &'static str, value: String },
    #[error("multicast link `{link_id}` has invalid multicast settings: {reason}")]
    InvalidMulticast { link_id: String, reason: String },
    #[error("multicast TTL for link `{link_id}` must be between 1 and 255")]
    InvalidMulticastTtl { link_id: String },
    #[error("{field} must use {protocol}")]
    InvalidVideoUrl {
        field: &'static str,
        protocol: &'static str,
    },
    #[error("server.video.control_api_addr must bind to a loopback address: {value}")]
    NonLoopbackVideoControlApi { value: String },
    #[error("server.video.control_api_addr must be {expected}: {value}")]
    InvalidVideoControlApi {
        expected: &'static str,
        value: String,
    },
    #[error("server.video.stream_path is invalid: {value}")]
    InvalidVideoStreamPath { value: String },
    #[error("relay.io_controller is invalid: {reason}")]
    InvalidIoController { reason: String },
    #[error("failed to read config {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize config: {source}")]
    Serialize {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to write config {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to replace config {path}: {source}")]
    Replace {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

pub fn load_config(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let config = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: path.display().to_string(),
        source,
    })?;
    validate_config(&config)?;
    Ok(config)
}

pub fn save_config(path: impl AsRef<Path>, config: &Config) -> Result<(), ConfigError> {
    validate_config(config)?;
    let path = path.as_ref();
    let serialized =
        toml::to_string_pretty(config).map_err(|source| ConfigError::Serialize { source })?;
    let temporary_path = temporary_config_path(path);
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .map_err(|source| ConfigError::Write {
                path: temporary_path.display().to_string(),
                source,
            })?;
        file.write_all(serialized.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|source| ConfigError::Write {
                path: temporary_path.display().to_string(),
                source,
            })?;
        fs::rename(&temporary_path, path).map_err(|source| ConfigError::Replace {
            path: path.display().to_string(),
            source,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn temporary_config_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("relay-link.toml");
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        timestamp
    ))
}

pub fn validate_config(config: &Config) -> Result<(), ConfigError> {
    if config.relay.is_none() && config.server.is_none() {
        return Err(ConfigError::InvalidRole);
    }

    if let Some(relay) = &config.relay {
        validate_relay(relay)?;
    }
    if let Some(server) = &config.server {
        validate_server(server)?;
    }
    Ok(())
}

fn validate_relay(relay: &RelayConfig) -> Result<(), ConfigError> {
    require_non_empty("relay.id", &relay.id)?;
    require_non_empty("relay.server_addr", &relay.server_addr)?;
    if relay.heartbeat_interval_ms == 0 || relay.data_timeout_ms == 0 {
        return Err(ConfigError::EmptyField {
            field: "relay timing values",
        });
    }
    if relay.links.is_empty() {
        return Err(ConfigError::EmptyField {
            field: "relay.links",
        });
    }

    if let Some(video) = &relay.video {
        validate_relay_video(video)?;
    }

    if let Some(turntable) = &relay.turntable {
        validate_turntable(turntable)?;
    }

    if let Some(io) = &relay.io_controller {
        validate_io_controller(io)?;
    }

    let mut ids = HashSet::new();
    for link in &relay.links {
        require_non_empty("links.id", &link.id)?;
        if !ids.insert(&link.id) {
            return Err(ConfigError::DuplicateLinkId {
                id: link.id.clone(),
            });
        }
        validate_path_metadata_length(&relay.id, &link.id)?;
        validate_link(link)?;
    }
    Ok(())
}

fn validate_path_metadata_length(relay_id: &str, link_id: &str) -> Result<(), ConfigError> {
    let bytes = relay_id.len().saturating_add(link_id.len());
    if bytes > GROUND_STATION_HEARTBEAT_PATH_METADATA_LIMIT {
        return Err(ConfigError::PathMetadataTooLong {
            relay_id: relay_id.into(),
            link_id: link_id.into(),
            bytes,
            limit: GROUND_STATION_HEARTBEAT_PATH_METADATA_LIMIT,
        });
    }
    Ok(())
}

fn validate_server(server: &ServerConfig) -> Result<(), ConfigError> {
    require_non_empty("server.id", &server.id)?;
    require_non_empty("server.listen_addr", &server.listen_addr)?;
    require_non_empty(
        "server.ground_station_listen_addr",
        &server.ground_station_listen_addr,
    )?;
    if server.heartbeat_timeout_ms == 0
        || server.data_timeout_ms == 0
        || server.ground_station_heartbeat_interval_ms == 0
    {
        return Err(ConfigError::EmptyField {
            field: "server timing values",
        });
    }
    parse_socket("server.listen_addr", &server.listen_addr)?;
    parse_socket(
        "server.ground_station_listen_addr",
        &server.ground_station_listen_addr,
    )?;
    if let Some(video) = &server.video {
        validate_server_video(video)?;
    }
    Ok(())
}

fn validate_relay_video(video: &RelayVideoConfig) -> Result<(), ConfigError> {
    require_non_empty("relay.video.source_url", &video.source_url)?;
    require_non_empty("relay.video.publish_url", &video.publish_url)?;
    if !video.source_url.starts_with("rtsp://") {
        return Err(ConfigError::InvalidVideoUrl {
            field: "relay.video.source_url",
            protocol: "rtsp://",
        });
    }
    if !video.publish_url.starts_with("rtmp://") {
        return Err(ConfigError::InvalidVideoUrl {
            field: "relay.video.publish_url",
            protocol: "rtmp://",
        });
    }
    validate_video_timing(
        video.startup_timeout_ms,
        video.stall_timeout_ms,
        video.restart_max_delay_ms,
        None,
    )
}

fn validate_server_video(video: &ServerVideoConfig) -> Result<(), ConfigError> {
    require_non_empty("server.video.stream_path", &video.stream_path)?;
    if !is_safe_video_stream_path(&video.stream_path) {
        return Err(ConfigError::InvalidVideoStreamPath {
            value: video.stream_path.clone(),
        });
    }
    let api_addr = parse_socket("server.video.control_api_addr", &video.control_api_addr)?;
    if !api_addr.ip().is_loopback() {
        return Err(ConfigError::NonLoopbackVideoControlApi {
            value: video.control_api_addr.clone(),
        });
    }
    if video.control_api_addr != MEDIAMTX_CONTROL_API_ADDR {
        return Err(ConfigError::InvalidVideoControlApi {
            expected: MEDIAMTX_CONTROL_API_ADDR,
            value: video.control_api_addr.clone(),
        });
    }
    validate_video_timing(
        video.startup_timeout_ms,
        video.stall_timeout_ms,
        video.restart_max_delay_ms,
        Some(video.poll_interval_ms),
    )
}

fn validate_video_timing(
    startup_timeout_ms: u64,
    stall_timeout_ms: u64,
    restart_max_delay_ms: u64,
    poll_interval_ms: Option<u64>,
) -> Result<(), ConfigError> {
    if startup_timeout_ms == 0 || stall_timeout_ms == 0 || restart_max_delay_ms == 0 {
        return Err(ConfigError::EmptyField {
            field: "video timing values",
        });
    }
    if poll_interval_ms.is_some_and(|value| value == 0) {
        return Err(ConfigError::EmptyField {
            field: "server.video.poll_interval_ms",
        });
    }
    Ok(())
}

fn is_safe_video_stream_path(path: &str) -> bool {
    !path.starts_with('/')
        && path.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

fn validate_link(link: &LinkConfig) -> Result<(), ConfigError> {
    match &link.kind {
        LinkKind::Serial {
            path, baud_rate, ..
        } => {
            require_non_empty("serial.path", path)?;
            if *baud_rate == 0 {
                return Err(ConfigError::EmptyField {
                    field: "serial.baud_rate",
                });
            }
        }
        LinkKind::UdpUnicast {
            bind_addr,
            tx_target,
            allowed_source,
        } => {
            parse_socket("udp.bind_addr", bind_addr)?;
            parse_socket("udp.tx_target", tx_target)?;
            if let Some(source) = allowed_source {
                parse_socket("udp.allowed_source", source)?;
            }
        }
        LinkKind::UdpMulticast {
            bind_addr,
            group_addr,
            interface_addr,
            tx_target,
            multicast_ttl,
            ..
        } => {
            parse_socket("udp_multicast.bind_addr", bind_addr)?;
            parse_socket("udp_multicast.tx_target", tx_target)?;
            let group: IpAddr = group_addr
                .parse()
                .map_err(|_| ConfigError::InvalidMulticast {
                    link_id: link.id.clone(),
                    reason: format!("group address `{group_addr}` is not an IP address"),
                })?;
            if !group.is_multicast() {
                return Err(ConfigError::InvalidMulticast {
                    link_id: link.id.clone(),
                    reason: "group address is not multicast".into(),
                });
            }
            let interface: IpAddr =
                interface_addr
                    .parse()
                    .map_err(|_| ConfigError::InvalidMulticast {
                        link_id: link.id.clone(),
                        reason: format!(
                            "interface address `{interface_addr}` is not an IP address"
                        ),
                    })?;
            if !matches!((group, interface), (IpAddr::V4(_), IpAddr::V4(_))) {
                return Err(ConfigError::InvalidMulticast {
                    link_id: link.id.clone(),
                    reason: "the first release requires IPv4 group and interface addresses".into(),
                });
            }
            if *multicast_ttl == 0 || *multicast_ttl > 255 {
                return Err(ConfigError::InvalidMulticastTtl {
                    link_id: link.id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn parse_socket(field: &'static str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::InvalidSocketAddress {
            field,
            value: value.into(),
        })
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::EmptyField { field })
    } else {
        Ok(())
    }
}

fn default_heartbeat_interval_ms() -> u64 {
    1_000
}

fn default_heartbeat_timeout_ms() -> u64 {
    3_000
}

fn default_ground_station_heartbeat_interval_ms() -> u64 {
    1_000
}

fn default_data_timeout_ms() -> u64 {
    3_000
}

fn default_multicast_ttl() -> u32 {
    1
}

fn default_video_startup_timeout_ms() -> u64 {
    15_000
}

fn default_video_stall_timeout_ms() -> u64 {
    10_000
}

fn default_video_restart_max_delay_ms() -> u64 {
    30_000
}

fn default_video_poll_interval_ms() -> u64 {
    1_000
}

fn default_video_control_api_addr() -> String {
    MEDIAMTX_CONTROL_API_ADDR.into()
}

pub fn is_valid_ipv4_multicast(value: &str) -> bool {
    value
        .parse::<Ipv4Addr>()
        .map(|address| address.is_multicast())
        .unwrap_or(false)
}

fn validate_turntable(turntable: &TurntableConfig) -> Result<(), ConfigError> {
    require_non_empty("relay.turntable.bind_addr", &turntable.bind_addr)?;
    require_non_empty("relay.turntable.target_addr", &turntable.target_addr)?;
    parse_socket("relay.turntable.bind_addr", &turntable.bind_addr)?;
    parse_socket("relay.turntable.target_addr", &turntable.target_addr)?;
    Ok(())
}

fn default_turntable_bind_addr() -> String {
    "0.0.0.0:0".into()
}

pub fn default_io_port() -> String {
    "/dev/ttyS7".into()
}

pub fn default_io_baud_rate() -> u32 {
    9600
}

pub fn default_io_slave_id() -> u8 {
    1
}

pub fn default_io_poll_interval_ms() -> u64 {
    1000
}

fn validate_io_controller(io: &IoControllerConfig) -> Result<(), ConfigError> {
    require_non_empty("relay.io_controller.port", &io.port)?;
    if io.baud_rate == 0 {
        return Err(ConfigError::InvalidIoController {
            reason: "baud_rate must be greater than 0".into(),
        });
    }
    if io.slave_id == 0 {
        return Err(ConfigError::InvalidIoController {
            reason: "slave_id must be greater than 0".into(),
        });
    }
    Ok(())
}

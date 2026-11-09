use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use relay_core::config::{
    Config, ConfigError, LinkConfig, LinkKind, RelayConfig, save_config, validate_config,
};

fn valid_relay() -> RelayConfig {
    RelayConfig {
        id: "relay-a".into(),
        server_addr: "server.example:7443".into(),
        heartbeat_interval_ms: 1000,
        data_timeout_ms: 3000,
        links: vec![
            LinkConfig {
                id: "serial-1".into(),
                priority: 0,
                kind: LinkKind::Serial {
                    path: "COM4".into(),
                    baud_rate: 115200,
                },
            },
            LinkConfig {
                id: "udp-1".into(),
                priority: 0,
                kind: LinkKind::UdpUnicast {
                    bind_addr: "0.0.0.0:9001".into(),
                    tx_target: "192.168.10.20:9002".into(),
                    allowed_source: Some("192.168.10.20:9002".into()),
                },
            },
            LinkConfig {
                id: "multicast-1".into(),
                priority: 0,
                kind: LinkKind::UdpMulticast {
                    bind_addr: "0.0.0.0:10001".into(),
                    group_addr: "239.10.10.10".into(),
                    interface_addr: "192.168.20.10".into(),
                    tx_target: "239.10.10.10:10001".into(),
                    multicast_ttl: 1,
                    multicast_loop: false,
                },
            },
        ],
        video: None,
        turntable: None,
        io_controller: None,
        auth: None,
    }
}

#[test]
fn accepts_all_supported_link_kinds_from_toml() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200

[[relay.links]]
id = "udp-1"
type = "udp_unicast"
bind_addr = "0.0.0.0:9001"
tx_target = "192.168.10.20:9002"

[[relay.links]]
id = "multicast-1"
type = "udp_multicast"
bind_addr = "0.0.0.0:10001"
group_addr = "239.10.10.10"
interface_addr = "192.168.20.10"
tx_target = "239.10.10.10:10001"
multicast_ttl = 1
multicast_loop = false
"#;

    let config: Config = toml::from_str(raw).expect("example config should deserialize");
    let relay = config.relay.expect("relay section");
    assert_eq!(relay.links.len(), 3);
    validate_config(&Config {
        relay: Some(relay),
        server: None,
    })
    .expect("config should validate");
}

#[test]
fn serial_config_persists_only_port_and_baud_rate() {
    let config = Config {
        relay: Some(RelayConfig {
            id: "relay-a".into(),
            server_addr: "server.example:7443".into(),
            heartbeat_interval_ms: 1000,
            data_timeout_ms: 3000,
            links: vec![LinkConfig {
                id: "serial-1".into(),
                priority: 0,
                kind: LinkKind::Serial {
                    path: "COM4".into(),
                    baud_rate: 115200,
                },
            }],
            video: None,
            turntable: None,
            io_controller: None,
            auth: None,
        }),
        server: None,
    };

    let serialized = toml::to_string(&config).expect("serial config should serialize");
    assert!(serialized.contains("path = \"COM4\""));
    assert!(serialized.contains("baud_rate = 115200"));
    for obsolete in ["data_bits", "stop_bits", "parity", "flow_control"] {
        assert!(
            !serialized.contains(obsolete),
            "serial config must not persist {obsolete}"
        );
    }
}

#[test]
fn server_uses_ground_station_listen_addr_as_listener() {
    let raw = r#"
[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"
"#;

    let config: Config = toml::from_str(raw).expect("listener config should deserialize");
    let server = config.server.expect("server section");
    let serialized = serde_json::to_value(server).expect("server config should serialize");
    assert_eq!(serialized["ground_station_listen_addr"], "127.0.0.1:7500");
}

#[test]
fn server_accepts_legacy_ground_station_addr_key() {
    let raw = r#"
[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_addr = "127.0.0.1:7500"
"#;

    let config: Config = toml::from_str(raw).expect("legacy config should deserialize");
    let server = config.server.expect("server section");
    let serialized = serde_json::to_value(server).expect("server config should serialize");
    assert_eq!(serialized["ground_station_listen_addr"], "127.0.0.1:7500");
}

#[test]
fn server_uses_ground_station_heartbeat_interval_default() {
    let raw = r#"
[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"
"#;

    let config: Config = toml::from_str(raw).expect("server config should deserialize");
    let server = config.server.expect("server section");
    assert_eq!(server.ground_station_heartbeat_interval_ms, 1000);
}

#[test]
fn server_reads_ground_station_heartbeat_settings() {
    let raw = r#"
[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"
ground_station_heartbeat_interval_ms = 250
"#;

    let config: Config = toml::from_str(raw).expect("server config should deserialize");
    let server = config.server.expect("server section");
    assert_eq!(server.ground_station_heartbeat_interval_ms, 250);
}

#[test]
fn unified_config_accepts_relay_and_server_sections() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200

[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"
"#;

    let config: Config = toml::from_str(raw).expect("unified config should deserialize");
    validate_config(&config).expect("both role sections should validate together");
}

#[test]
fn video_sections_round_trip_with_safe_defaults() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200

[relay.video]
source_url = "rtsp://192.168.10.20:554/live"
publish_url = "rtmp://relay.example.com:1935/uav/relay-a"

[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"

[server.video]
stream_path = "uav/relay-a"
"#;

    let config: Config = toml::from_str(raw).expect("video config should deserialize");
    validate_config(&config).expect("video config should validate");

    let serialized = serde_json::to_value(&config).expect("video config should serialize");
    assert_eq!(
        serialized["relay"]["video"]["source_url"],
        "rtsp://192.168.10.20:554/live"
    );
    assert_eq!(
        serialized["relay"]["video"]["publish_url"],
        "rtmp://relay.example.com:1935/uav/relay-a"
    );
    assert_eq!(serialized["relay"]["video"]["startup_timeout_ms"], 15_000);
    assert_eq!(serialized["relay"]["video"]["stall_timeout_ms"], 10_000);
    assert_eq!(
        serialized["server"]["video"]["control_api_addr"],
        "127.0.0.1:9997"
    );
    assert_eq!(serialized["server"]["video"]["poll_interval_ms"], 1_000);
}

#[test]
fn rejects_invalid_video_protocols_and_server_endpoint() {
    for (label, relay_video, server_video) in [
        (
            "non-rtsp-input",
            r#"
[relay.video]
source_url = "http://192.168.10.20/live"
publish_url = "rtmp://relay.example.com:1935/uav/relay-a"
"#,
            "",
        ),
        (
            "non-rtmp-output",
            r#"
[relay.video]
source_url = "rtsp://192.168.10.20:554/live"
publish_url = "rtsp://relay.example.com:8554/uav/relay-a"
"#,
            "",
        ),
        (
            "non-local-api",
            "",
            r#"
[server.video]
stream_path = "uav/relay-a"
control_api_addr = "192.168.10.10:9997"
"#,
        ),
        (
            "wrong-local-api-port",
            "",
            r#"
[server.video]
stream_path = "uav/relay-a"
control_api_addr = "127.0.0.1:9998"
"#,
        ),
        (
            "unsafe-path",
            "",
            r#"
[server.video]
stream_path = "../uav"
control_api_addr = "127.0.0.1:9997"
"#,
        ),
        (
            "zero-timeout",
            r#"
[relay.video]
source_url = "rtsp://192.168.10.20:554/live"
publish_url = "rtmp://relay.example.com:1935/uav/relay-a"
startup_timeout_ms = 0
"#,
            "",
        ),
    ] {
        let raw = format!(
            r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200
{relay_video}

[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_listen_addr = "127.0.0.1:7500"
{server_video}
"#
        );
        let config: Config = toml::from_str(&raw).expect("candidate should deserialize");
        assert!(
            validate_config(&config).is_err(),
            "{label} video configuration must be rejected"
        );
    }
}

#[test]
fn invalid_video_url_errors_do_not_echo_camera_credentials() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200

[relay.video]
source_url = "http://pilot:camera-secret@camera.example/live"
publish_url = "rtmp://server.example:1935/uav/relay-a"
"#;
    let config: Config = toml::from_str(raw).expect("candidate should deserialize");

    let error = validate_config(&config).expect_err("invalid protocol must be rejected");

    assert!(!error.to_string().contains("camera-secret"));
    assert!(!error.to_string().contains("camera.example"));
}

#[test]
fn ipc_endpoint_in_legacy_config_is_ignored_and_not_saved() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "server.example:7443"
ipc_endpoint = "custom-user-selected-endpoint"

[[relay.links]]
id = "serial-1"
type = "serial"
path = "COM4"
baud_rate = 115200
"#;

    let config: Config = toml::from_str(raw).expect("legacy config should deserialize");
    let serialized = toml::to_string(&config).expect("config should serialize");
    assert!(
        !serialized.contains("ipc_endpoint"),
        "IPC endpoint must not be persisted as a user configuration field"
    );
}

#[test]
fn rejects_duplicate_link_ids() {
    let mut relay = valid_relay();
    relay.links[1].id = relay.links[0].id.clone();
    let error = validate_config(&Config {
        relay: Some(relay),
        server: None,
    })
    .expect_err("duplicate ids must be rejected");

    assert!(matches!(error, ConfigError::DuplicateLinkId { .. }));
}

#[test]
fn rejects_path_ids_that_do_not_fit_ground_station_heartbeat() {
    let mut relay = valid_relay();
    relay.id = "relay-station-east-2026".into();
    relay.links[0].id = "serial-radio-primary".into();

    let error = validate_config(&Config {
        relay: Some(relay),
        server: None,
    })
    .expect_err("path metadata must fit the fixed heartbeat frame");

    assert!(matches!(error, ConfigError::PathMetadataTooLong { .. }));
}

#[test]
fn rejects_invalid_multicast_settings() {
    let mut relay = valid_relay();
    relay.links[2].kind = LinkKind::UdpMulticast {
        bind_addr: "0.0.0.0:10001".into(),
        group_addr: "127.0.0.1".into(),
        interface_addr: "192.168.20.10".into(),
        tx_target: "127.0.0.1:10001".into(),
        multicast_ttl: 0,
        multicast_loop: false,
    };

    let error = validate_config(&Config {
        relay: Some(relay),
        server: None,
    })
    .expect_err("invalid multicast settings must be rejected");

    assert!(matches!(error, ConfigError::InvalidMulticast { .. }));
}

#[test]
fn shipped_example_configs_parse_and_validate() {
    let config: Config = toml::from_str(include_str!("../../../config/config.example.toml"))
        .expect("unified example should deserialize");
    validate_config(&config).expect("unified example should validate");
}

#[test]
fn config_save_writes_valid_toml() {
    let path = temp_config_path("round-trip");
    let config = Config {
        relay: Some(valid_relay()),
        server: None,
    };

    fs::write(&path, "stale = true\n").expect("test config should be created");
    save_config(&path, &config).expect("valid config should be saved");
    let raw = fs::read_to_string(&path).expect("saved config should be readable");
    let restored: Config = toml::from_str(&raw).expect("saved config should be valid TOML");

    assert_eq!(restored, config);
    fs::remove_file(path).expect("test config should be removed");
}

#[test]
fn config_save_rejects_invalid_role_without_creating_file() {
    let path = temp_config_path("invalid-role");
    let config = Config {
        relay: None,
        server: None,
    };

    let error = save_config(&path, &config).expect_err("invalid role must not be saved");

    assert!(matches!(error, ConfigError::InvalidRole));
    assert!(!path.exists());
}

#[test]
fn config_accepts_valid_turntable() {
    let mut relay = valid_relay();
    relay.turntable = Some(relay_core::config::TurntableConfig {
        bind_addr: "0.0.0.0:9003".into(),
        target_addr: "192.168.1.50:9003".into(),
    });
    let config = Config {
        relay: Some(relay),
        server: None,
    };

    assert!(validate_config(&config).is_ok());
}

#[test]
fn accepts_auth_configuration_for_relay_and_server() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "127.0.0.1:7443"

[[relay.links]]
id = "link-1"
type = "serial"
path = "COM1"
baud_rate = 115200

[relay.auth]
username = "admin"
password = "secretpassword"

[server]
id = "server-a"
listen_addr = "0.0.0.0:7443"
ground_station_addr = "0.0.0.0:9876"

[server.auth]
username = "server-admin"
password = "anothersecret"
"#;
    let config: Config = toml::from_str(raw).expect("config should parse");
    assert!(validate_config(&config).is_ok());
    assert!(config.auth_for_role(relay_core::model::Role::Relay).unwrap().is_active());
    assert_eq!(config.auth_for_role(relay_core::model::Role::Relay).unwrap().username, "admin");
    assert_eq!(config.auth_for_role(relay_core::model::Role::Relay).unwrap().password, "secretpassword");
    assert_eq!(config.auth_for_role(relay_core::model::Role::Server).unwrap().username, "server-admin");
    assert_eq!(config.auth_for_role(relay_core::model::Role::Server).unwrap().password, "anothersecret");
}

#[test]
fn config_deserializes_turntable_with_alias_and_default_bind() {
    let raw = r#"
[relay]
id = "relay-a"
server_addr = "127.0.0.1:7443"

[[relay.links]]
id = "link-1"
type = "serial"
path = "COM1"
baud_rate = 115200

[relay.turntable]
tx_target = "192.168.1.50:9003"
"#;
    let config: Config = toml::from_str(raw).expect("config should parse");
    let turntable = config.relay.expect("relay").turntable.expect("turntable");
    assert_eq!(turntable.bind_addr, "0.0.0.0:0");
    assert_eq!(turntable.target_addr, "192.168.1.50:9003");
}

#[test]
fn config_rejects_invalid_turntable_socket() {
    let mut relay = valid_relay();
    relay.turntable = Some(relay_core::config::TurntableConfig {
        bind_addr: "not-a-socket".into(),
        target_addr: "192.168.1.50:9003".into(),
    });
    let config = Config {
        relay: Some(relay),
        server: None,
    };

    assert!(matches!(
        validate_config(&config),
        Err(ConfigError::InvalidSocketAddress { field: "relay.turntable.bind_addr", .. })
    ));
}

#[test]
fn accepts_io_controller_configuration() {
    let mut relay = valid_relay();
    relay.io_controller = Some(relay_core::config::IoControllerConfig {
        port: "/dev/ttyS7".into(),
        baud_rate: 9600,
        slave_id: 1,
        poll_interval_ms: 1000,
    });
    let config = Config {
        relay: Some(relay),
        server: None,
    };
    assert!(validate_config(&config).is_ok());
}

fn temp_config_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "relay-link-config-{label}-{}-{nonce}.toml",
        std::process::id()
    ))
}

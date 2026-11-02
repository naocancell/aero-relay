use std::time::Duration;

use relay_core::config::{LinkConfig, LinkKind, RelayConfig, ServerConfig};
use relay_core::model::{Role, SnapshotPayload};
use relay_linkd::{relay, server};
use tokio::sync::{mpsc, watch};

#[tokio::test]
async fn server_keeps_network_runtime_when_ipc_sender_disconnects() {
    let config = ServerConfig {
        id: "server-test".into(),
        listen_addr: "127.0.0.1:0".into(),
        ground_station_listen_addr: "127.0.0.1:0".into(),
        ground_station_heartbeat_interval_ms: 1_000,
        heartbeat_timeout_ms: 3_000,
        data_timeout_ms: 3_000,
        video: None,
    };
    let (snapshot_tx, _snapshot_rx) =
        watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
    let (reload_tx, reload_rx) = mpsc::channel(1);
    drop(reload_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(server::run(
        config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        None,
    ));

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !task.is_finished(),
        "IPC disconnect stopped the server runtime"
    );

    shutdown_tx.send(true).expect("send server shutdown");
    assert!(task.await.expect("join server runtime").is_ok());
}

#[tokio::test]
async fn relay_keeps_network_runtime_when_ipc_sender_disconnects() {
    let config = RelayConfig {
        id: "relay-test".into(),
        server_addr: "127.0.0.1:1".into(),
        heartbeat_interval_ms: 1_000,
        data_timeout_ms: 3_000,
        links: vec![LinkConfig {
            id: "udp-test".into(),
            kind: LinkKind::UdpUnicast {
                bind_addr: "127.0.0.1:0".into(),
                tx_target: "127.0.0.1:1".into(),
                allowed_source: None,
            },
            priority: 0,
        }],
        video: None,
        turntable: None,
    };
    let (snapshot_tx, _snapshot_rx) =
        watch::channel(SnapshotPayload::RoleOnly { role: Role::Relay });
    let (reload_tx, reload_rx) = mpsc::channel(1);
    drop(reload_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(relay::run(
        config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        None,
    ));

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !task.is_finished(),
        "IPC disconnect stopped the relay runtime"
    );

    shutdown_tx.send(true).expect("send relay shutdown");
    assert!(task.await.expect("join relay runtime").is_ok());
}

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay_core::config::ServerConfig;
use relay_linkd::ground_station_protocol::crc_bytes;
use relay_core::model::{PathId, Role, SnapshotPayload};
use relay_linkd::server;
use relay_linkd::transport::{RemoteCodec, RemoteMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio_util::codec::Framed;

async fn spawn_test_server(data_timeout_ms: u64) -> (String, String, watch::Sender<bool>, watch::Receiver<SnapshotPayload>, tokio::task::JoinHandle<Result<(), server::ServerError>>) {
    let relay_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap().to_string();
    let gs_addr = gs_listener.local_addr().unwrap().to_string();
    drop(relay_listener);
    drop(gs_listener);

    let config = ServerConfig {
        id: "test-server-r345".into(),
        listen_addr: relay_addr.clone(),
        ground_station_listen_addr: gs_addr.clone(),
        ground_station_heartbeat_interval_ms: 100,
        heartbeat_timeout_ms: 2000,
        data_timeout_ms,
        video: None,
    };

    let (snapshot_tx, snapshot_rx) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_reload_tx, reload_rx) = mpsc::channel(1);
    let (ready_tx, ready_rx) = oneshot::channel();

    let server_task = tokio::spawn(server::run(
        config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        Some(ready_tx),
    ));
    ready_rx.await.expect("server ready");

    (relay_addr, gs_addr, shutdown_tx, snapshot_rx, server_task)
}

fn make_valid_turntable_frame() -> [u8; 32] {
    let mut frame = [0u8; 32];
    frame[0] = 0xEB;
    frame[1] = 0x90;
    frame[2] = 0x02; // TURNTABLE_DEST_ADDR
    frame[3] = 0x01; // TURNTABLE_SRC_ADDR
    frame[4] = 0xC2; // TURNTABLE_FRAME_ID
    for i in 5..30 {
        frame[i] = (i * 3) as u8;
    }
    let [c0, c1] = crc_bytes(&frame[2..30]);
    frame[30] = c0;
    frame[31] = c1;
    frame
}

fn make_valid_aircraft_frame() -> [u8; 32] {
    let mut frame = [0u8; 32];
    frame[0] = 0xEB;
    frame[1] = 0x90;
    frame[2] = 0x01; // Data frame
    for i in 3..30 {
        frame[i] = (i * 5) as u8;
    }
    let [c0, c1] = crc_bytes(&frame[2..30]);
    frame[30] = c0;
    frame[31] = c1;
    frame
}

/// 验证 R5：转台控制帧（EB 90 02 01 C2）广播至所有已连接中继，
/// 而普通机载飞控指令仅单播至当前活跃中继链路。
#[tokio::test]
async fn test_turntable_frames_broadcast_to_all_relays_and_flight_commands_unicast() {
    let (relay_addr, gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server(3000).await;

    // 1. 中继 1（主站，优先级 100）连入
    let stream1 = TcpStream::connect(&relay_addr).await.unwrap();
    let mut relay1 = Framed::new(stream1, RemoteCodec::new());
    relay1.send(RemoteMessage::RelayHello { relay_id: "relay-1".into() }).await.unwrap();
    assert!(matches!(relay1.next().await, Some(Ok(RemoteMessage::HelloAck { .. }))));
    relay1.send(RemoteMessage::LinkAnnounce {
        relay_id: "relay-1".into(),
        link_id: "link-1".into(),
        kind: "udp".into(),
        priority: 100,
    }).await.unwrap();

    // 2. 中继 2（备用站，优先级 10）连入
    let stream2 = TcpStream::connect(&relay_addr).await.unwrap();
    let mut relay2 = Framed::new(stream2, RemoteCodec::new());
    relay2.send(RemoteMessage::RelayHello { relay_id: "relay-2".into() }).await.unwrap();
    assert!(matches!(relay2.next().await, Some(Ok(RemoteMessage::HelloAck { .. }))));
    relay2.send(RemoteMessage::LinkAnnounce {
        relay_id: "relay-2".into(),
        link_id: "link-2".into(),
        kind: "udp".into(),
        priority: 10,
    }).await.unwrap();

    // 中继 1 发送一帧有效遥测，确立 relay-1/link-1 为 active_path
    let valid_telemetry = make_valid_aircraft_frame();
    relay1.send(RemoteMessage::AircraftData {
        relay_id: "relay-1".into(),
        link_id: "link-1".into(),
        payload: valid_telemetry.to_vec(),
    }).await.unwrap();

    // 等待 active_path 确立为 relay-1/link-1
    time::timeout(Duration::from_secs(2), async {
        loop {
            if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                if s.active_path == Some(PathId::new("relay-1", "link-1")) {
                    break;
                }
            }
            snapshot_rx.changed().await.unwrap();
        }
    }).await.expect("active_path should be relay-1/link-1");

    // 3. 地面站连入并握手
    let mut gs_stream = TcpStream::connect(&gs_addr).await.unwrap();
    gs_stream.write_all(b"dqiang").await.unwrap();

    // 读出握手后的心跳
    let mut hb = [0u8; 32];
    gs_stream.read_exact(&mut hb).await.unwrap();

    async fn recv_next_control(relay: &mut Framed<TcpStream, RemoteCodec>, timeout: Duration) -> RemoteMessage {
        let deadline = time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            let msg = time::timeout(remaining, relay.next())
                .await
                .expect("timeout waiting for RemoteMessage")
                .expect("stream ended")
                .expect("decode error");
            match &msg {
                RemoteMessage::ControlData { .. } => return msg,
                RemoteMessage::ActivePath { .. } => continue,
                other => panic!("unexpected message: {other:?}"),
            }
        }
    }

    // 4. 地面站发送一个 32 字节伺服转台指令帧
    let turntable_frame = make_valid_turntable_frame();
    gs_stream.write_all(&turntable_frame).await.unwrap();

    // 验证：Relay 1 与 Relay 2 均收到了转台控制帧！
    let msg1 = recv_next_control(&mut relay1, Duration::from_secs(1)).await;
    match msg1 {
        RemoteMessage::ControlData { relay_id, payload, .. } => {
            assert_eq!(relay_id, "relay-1");
            assert_eq!(payload, turntable_frame.to_vec());
        }
        other => panic!("relay 1 unexpected message: {other:?}"),
    }

    let msg2 = recv_next_control(&mut relay2, Duration::from_secs(1)).await;
    match msg2 {
        RemoteMessage::ControlData { relay_id, payload, .. } => {
            assert_eq!(relay_id, "relay-2");
            assert_eq!(payload, turntable_frame.to_vec());
        }
        other => panic!("relay 2 unexpected message: {other:?}"),
    }

    // 5. 地面站发送一条纯飞控透传指令（例如 8 字节自定义控制包）
    let flight_cmd = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    gs_stream.write_all(&flight_cmd).await.unwrap();

    // 验证：Relay 1（活跃链路）收到该指令
    let msg1_cmd = recv_next_control(&mut relay1, Duration::from_secs(1)).await;
    match msg1_cmd {
        RemoteMessage::ControlData { relay_id, link_id, payload } => {
            assert_eq!(relay_id, "relay-1");
            assert_eq!(link_id, "link-1");
            assert_eq!(payload, flight_cmd);
        }
        other => panic!("relay 1 unexpected cmd: {other:?}"),
    }

    // Relay 2（备用链路）绝不应收到飞控指令
    let deadline = time::Instant::now() + Duration::from_millis(300);
    while time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if let Ok(Some(Ok(msg))) = time::timeout(remaining, relay2.next()).await {
            match msg {
                RemoteMessage::ControlData { .. } => panic!("relay 2 unexpectedly received flight control data"),
                RemoteMessage::ActivePath { .. } => continue,
                _ => {}
            }
        } else {
            break;
        }
    }

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

/// 验证 R4：连续噪声或 CRC 损坏数据不会刷新 last_rx_at，
/// 当链路只有噪波时，系统能够按预期超时失活。
#[tokio::test]
async fn test_corrupted_noise_does_not_refresh_last_rx_at_and_times_out() {
    let (relay_addr, _gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server(400).await; // 400ms 数据超时

    let stream = TcpStream::connect(&relay_addr).await.unwrap();
    let mut relay = Framed::new(stream, RemoteCodec::new());
    relay.send(RemoteMessage::RelayHello { relay_id: "relay-noise-test".into() }).await.unwrap();
    assert!(matches!(relay.next().await, Some(Ok(RemoteMessage::HelloAck { .. }))));
    relay.send(RemoteMessage::LinkAnnounce {
        relay_id: "relay-noise-test".into(),
        link_id: "link-noise".into(),
        kind: "serial".into(),
        priority: 0,
    }).await.unwrap();

    // 1. 发送一帧有效遥测，链路正常确立 active_path
    let valid_telemetry = make_valid_aircraft_frame();
    relay.send(RemoteMessage::AircraftData {
        relay_id: "relay-noise-test".into(),
        link_id: "link-noise".into(),
        payload: valid_telemetry.to_vec(),
    }).await.unwrap();

    time::timeout(Duration::from_secs(1), async {
        loop {
            if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                if s.active_path == Some(PathId::new("relay-noise-test", "link-noise")) {
                    break;
                }
            }
            snapshot_rx.changed().await.unwrap();
        }
    }).await.expect("link should become active");

    // 2. 模拟电台失锁/噪波：持续高频发送损坏 CRC 的 EB 90 帧或 > 128 字节乱码噪波
    let mut corrupt_frame = [0u8; 32];
    corrupt_frame[0] = 0xEB;
    corrupt_frame[1] = 0x90;
    corrupt_frame[30] = 0xDE; // 故意破坏 CRC
    corrupt_frame[31] = 0xAD;

    // 持续 600ms 疯狂发送噪波（超过 400ms 超时门限）
    let start = time::Instant::now();
    while start.elapsed() < Duration::from_millis(600) {
        relay.send(RemoteMessage::AircraftData {
            relay_id: "relay-noise-test".into(),
            link_id: "link-noise".into(),
            payload: corrupt_frame.to_vec(),
        }).await.unwrap();
        time::sleep(Duration::from_millis(50)).await;
    }

    // 3. 验证：由于噪波被拦截，last_rx_at 未被刷新，active_path 成功超时变为 None！
    let is_stale = {
        let snap = snapshot_rx.borrow();
        if let SnapshotPayload::Server(ref s) = *snap {
            s.active_path.is_none()
        } else {
            false
        }
    };
    assert!(is_stale, "尽管持续收到噪波，损坏的 CRC 绝不应维持链路活跃，active_path 必须超时置空");

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

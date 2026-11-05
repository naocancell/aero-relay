use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay_core::config::{LinkConfig, LinkKind, RelayConfig, ServerConfig};
use relay_core::model::{Role, SnapshotPayload};
use relay_linkd::relay;
use relay_linkd::server;
use relay_linkd::transport::{RemoteCodec, RemoteMessage};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio_util::codec::{FramedRead, FramedWrite};

/// 测试 1：验证 Relay 在机载遥测发送反压阻塞（对端停止读取 TCP）时，
/// 仍能实时接收服务端下发的控制指令并向机载飞控 UDP 端口分发，证明全双工读写彻底解耦，无死锁。
#[tokio::test]
async fn test_relay_telemetry_backpressure_does_not_block_control_dispatch() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 Mock 服务端监听");
    let server_addr = listener.local_addr().expect("获取本地端口");

    // 绑定模拟飞控接收端口与 UDP 发送端口
    let flight_controller_rx = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("绑定模拟飞控接收端口");
    let fc_addr = flight_controller_rx.local_addr().unwrap();

    let telemetry_tx = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("绑定机载遥测 UDP 发送端");
    let relay_udp = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("获取 Relay UDP 接收端口");
    let relay_udp_addr = relay_udp.local_addr().unwrap();
    drop(relay_udp);

    let relay_config = RelayConfig {
        id: "relay-duplex-test".into(),
        server_addr: server_addr.to_string(),
        heartbeat_interval_ms: 100,
        data_timeout_ms: 2000,
        links: vec![LinkConfig {
            id: "udp-control".into(),
            kind: LinkKind::UdpUnicast {
                bind_addr: relay_udp_addr.to_string(),
                tx_target: fc_addr.to_string(),
                allowed_source: None,
            },
            priority: 0,
        }],
        video: None,
        turntable: None,
        io_controller: None,
    };

    let (snapshot_tx, mut snapshot_rx) =
        watch::channel(SnapshotPayload::RoleOnly { role: Role::Relay });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_reload_tx, reload_rx) = mpsc::channel(1);
    let (ready_tx, ready_rx) = oneshot::channel();

    let relay_task = tokio::spawn(relay::run(
        relay_config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        Some(ready_tx),
        None,
    ));
    ready_rx.await.expect("relay 就绪");

    // Mock 服务端接管连接并握手
    let (server_stream, _) = listener.accept().await.expect("接受 relay 连接");
    server_stream.set_nodelay(true).unwrap();
    assert!(server_stream.nodelay().unwrap());
    let (read_half, write_half) = server_stream.into_split();
    let mut server_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut server_writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 握手
    let hello = server_reader.next().await.unwrap().unwrap();
    assert!(matches!(hello, RemoteMessage::RelayHello { .. }));
    server_writer
        .send(RemoteMessage::HelloAck {
            server_id: "mock-srv".into(),
        })
        .await
        .unwrap();
    let _announce = server_reader.next().await.unwrap().unwrap();

    // 确认 connected 状态
    time::timeout(Duration::from_millis(500), async {
        loop {
            if let SnapshotPayload::Relay(ref snap) = *snapshot_rx.borrow() {
                if snap.server_connected {
                    break;
                }
            }
            snapshot_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("relay 应该汇报 connected");

    // 构造反压：向 Relay 注入 400 个 1KB 数据包（总计 400KB），Mock 服务端完全不读取 TCP 数据流
    let big_payload = vec![0x7A; 1024];
    for _ in 0..400 {
        let _ = telemetry_tx.send_to(&big_payload, relay_udp_addr).await;
        time::sleep(Duration::from_micros(50)).await;
    }
    time::sleep(Duration::from_millis(100)).await;

    // 核心断言触发：下发飞控控制指令
    let emergency_cmd = vec![0xEE, 0xFF, 0x01, 0x02];
    server_writer
        .send(RemoteMessage::ControlData {
            relay_id: "relay-duplex-test".into(),
            link_id: "udp-control".into(),
            payload: emergency_cmd.clone(),
        })
        .await
        .expect("服务端下发控制指令");

    // 验证：模拟飞控端口是否成功收到了解耦后的飞控指令
    let mut fc_buf = [0u8; 128];
    let recv_res = time::timeout(
        Duration::from_millis(500),
        flight_controller_rx.recv(&mut fc_buf),
    )
    .await;
    assert!(
        recv_res.is_ok(),
        "在机载遥测反压阻塞时，飞控控制指令未能在 500ms 内穿透并分发！"
    );
    let n = recv_res.unwrap().expect("收到飞控报文");
    assert_eq!(&fc_buf[..n], &emergency_cmd[..]);

    let _ = shutdown_tx.send(true);
    let _ = relay_task.await;
}

/// 测试 2：验证 Server 在控制下发反压阻塞（中继停止读取控制指令流）时，
/// 中继上报的遥测依然被服务端 reader 协程实时接收与记录，证明服务端读写解耦。
#[tokio::test]
async fn test_server_control_backpressure_does_not_block_telemetry_ingress() {
    let l1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let l2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let relay_addr = l1.local_addr().unwrap().to_string();
    let gs_addr = l2.local_addr().unwrap().to_string();
    drop(l1);
    drop(l2);

    let server_config = ServerConfig {
        id: "server-duplex-test".into(),
        listen_addr: relay_addr.clone(),
        ground_station_listen_addr: gs_addr.clone(),
        ground_station_heartbeat_interval_ms: 50,
        heartbeat_timeout_ms: 1000,
        data_timeout_ms: 1000,
        video: None,
    };

    let (snapshot_tx, mut snapshot_rx) =
        watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_reload_tx, reload_rx) = mpsc::channel(1);
    let (ready_tx, ready_rx) = oneshot::channel();

    let server_task = tokio::spawn(server::run(
        server_config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        Some(ready_tx),
    ));
    ready_rx.await.expect("server 就绪");

    // Relay 连接握手
    let relay_stream = TcpStream::connect(&relay_addr).await.unwrap();
    relay_stream.set_nodelay(true).unwrap();
    assert!(relay_stream.nodelay().unwrap());
    let (read_half, write_half) = relay_stream.into_split();
    let mut relay_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut relay_writer = FramedWrite::new(write_half, RemoteCodec::new());

    relay_writer
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-test-1".into(),
        })
        .await
        .unwrap();
    let _ = relay_reader.next().await.unwrap().unwrap();
    relay_writer
        .send(RemoteMessage::LinkAnnounce {
            relay_id: "relay-test-1".into(),
            link_id: "link-1".into(),
            kind: "udp_unicast".into(),
            priority: 0,
        })
        .await
        .unwrap();

    // 地面站连接与初始活跃链路确立
    let mut gs_stream = TcpStream::connect(&gs_addr).await.unwrap();
    gs_stream.set_nodelay(true).unwrap();
    assert!(gs_stream.nodelay().unwrap());
    tokio::io::AsyncWriteExt::write_all(&mut gs_stream, b"dqiang")
        .await
        .unwrap();

    relay_writer
        .send(RemoteMessage::AircraftData {
            relay_id: "relay-test-1".into(),
            link_id: "link-1".into(),
            payload: vec![1, 2, 3, 4],
        })
        .await
        .unwrap();

    time::timeout(Duration::from_millis(500), async {
        loop {
            if let SnapshotPayload::Server(ref snap) = *snapshot_rx.borrow() {
                if snap.active_path.is_some() {
                    break;
                }
            }
            snapshot_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("server 确立 active_path");

    // 构造反压：地面站灌入大量控制数据，Mock Relay 故意完全不读控制 TCP 流
    let control_cmd = vec![0xBB; 512];
    for _ in 0..200 {
        let _ = tokio::io::AsyncWriteExt::write_all(&mut gs_stream, &control_cmd).await;
        time::sleep(Duration::from_micros(100)).await;
    }
    time::sleep(Duration::from_millis(100)).await;

    // 核心断言：在服务端向中继发送反压阻塞时，中继持续发送遥测
    let telemetry_before = {
        let snap = snapshot_rx.borrow().clone();
        if let SnapshotPayload::Server(s) = snap {
            s.relays
                .iter()
                .find(|r| r.relay_id == "relay-test-1")
                .and_then(|r| r.links.first().map(|l| l.rx_bytes))
                .unwrap_or(0)
        } else {
            0
        }
    };

    relay_writer
        .send(RemoteMessage::AircraftData {
            relay_id: "relay-test-1".into(),
            link_id: "link-1".into(),
            payload: vec![0x33; 64],
        })
        .await
        .expect("relay 发送遥测数据");

    let updated = time::timeout(Duration::from_millis(500), async {
        loop {
            if let SnapshotPayload::Server(ref snap) = *snapshot_rx.borrow() {
                if let Some(r) = snap.relays.iter().find(|r| r.relay_id == "relay-test-1") {
                    if let Some(l) = r.links.first() {
                        if l.rx_bytes > telemetry_before {
                            return true;
                        }
                    }
                }
            }
            if snapshot_rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;

    assert!(
        updated.is_ok(),
        "在控制下发反压时，服务端无法即刻读取并更新遥测接收字节！"
    );
}

/// 测试 3：验证底层真实 TCP 套接字开启 TCP_NODELAY 消除 Nagle 缓冲延迟，
/// 以及双向高吞吐全双工并发无死锁。
#[tokio::test]
async fn test_tcp_stream_nodelay_latency_and_bidirectional_duplex() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    const MSG_COUNT: usize = 200;

    let server_task = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.unwrap();
        server_stream.set_nodelay(true).unwrap();
        assert!(
            server_stream.nodelay().unwrap(),
            "服务端套接字 nodelay 未开启"
        );

        let (read_half, write_half) = server_stream.into_split();
        let mut server_reader = FramedRead::new(read_half, RemoteCodec::new());
        let mut server_writer = FramedWrite::new(write_half, RemoteCodec::new());

        // 先执行 20 次小报文回显，验证极低延迟（零 Nagle 延迟缓冲）
        for _ in 0..20 {
            if let Some(Ok(msg)) = server_reader.next().await {
                server_writer.send(msg).await.expect("服务端回显");
            }
        }

        // 双向高吞吐并发
        let writer_handle = tokio::spawn(async move {
            for i in 0..MSG_COUNT {
                server_writer
                    .send(RemoteMessage::ControlData {
                        relay_id: "r1".into(),
                        link_id: "l1".into(),
                        payload: vec![(i % 256) as u8; 64],
                    })
                    .await
                    .expect("server write error");
            }
        });

        let mut read_count = 0;
        while let Some(msg) = server_reader.next().await {
            let msg = msg.expect("server read error");
            if let RemoteMessage::AircraftData { .. } = msg {
                read_count += 1;
                if read_count >= MSG_COUNT {
                    break;
                }
            }
        }
        assert_eq!(read_count, MSG_COUNT);
        writer_handle.await.expect("server writer done");
    });

    let client_stream = TcpStream::connect(addr).await.expect("connect client");
    client_stream.set_nodelay(true).unwrap();
    assert!(
        client_stream.nodelay().unwrap(),
        "客户端套接字 nodelay 未开启"
    );

    let (read_half, write_half) = client_stream.into_split();
    let mut client_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut client_writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 验证小包无 Nagle 往返时延（20次往返总时延应远低于 Nagle 40ms 延迟确认单次累积）
    let start_time = std::time::Instant::now();
    for i in 0..20 {
        client_writer
            .send(RemoteMessage::Heartbeat {
                relay_id: format!("relay-{i}"),
            })
            .await
            .expect("客户端发送心跳");
        let reply = client_reader
            .next()
            .await
            .expect("收到服务端回复")
            .expect("解析");
        match reply {
            RemoteMessage::Heartbeat { relay_id } => {
                assert_eq!(relay_id, format!("relay-{i}"));
            }
            _ => panic!("收到非预期的消息"),
        }
    }
    let elapsed = start_time.elapsed();
    assert!(
        elapsed < Duration::from_millis(150),
        "20次小包往返耗时过长 ({elapsed:?})，疑似 Nagle 算法未关闭"
    );

    // 客户端并发读写
    let client_writer_handle = tokio::spawn(async move {
        for i in 0..MSG_COUNT {
            client_writer
                .send(RemoteMessage::AircraftData {
                    relay_id: "r1".into(),
                    link_id: "l1".into(),
                    payload: vec![(i % 256) as u8; 64],
                })
                .await
                .expect("client write error");
        }
    });

    let mut client_read_count = 0;
    while let Some(msg) = client_reader.next().await {
        let msg = msg.expect("client read error");
        if let RemoteMessage::ControlData { .. } = msg {
            client_read_count += 1;
            if client_read_count >= MSG_COUNT {
                break;
            }
        }
    }
    assert_eq!(client_read_count, MSG_COUNT);

    client_writer_handle.await.expect("client writer done");
    server_task.await.expect("server task done");
}

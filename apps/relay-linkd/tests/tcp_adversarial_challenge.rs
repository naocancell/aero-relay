use std::time::{Duration, Instant};

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

/// 挑战测试 1：验证 TCP 套接字底层真正的 `TCP_NODELAY` 状态及小报文高频往返低延迟特性
#[tokio::test]
async fn test_challenge_socket_nodelay_truth_verification() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定测试监听器");
    let addr = listener.local_addr().expect("获取本地端口");

    let server_task = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("接受客户端连接");
        // 服务端显式启用 nodelay
        server_stream.set_nodelay(true).expect("服务端设置 nodelay");
        // 关键断言：直接查询操作系统内核 socket 选项，验证 nodelay 为 true
        assert!(
            server_stream.nodelay().expect("获取服务端 nodelay 状态"),
            "服务端接受的套接字未成功启用 TCP_NODELAY"
        );

        let (read_half, write_half) = server_stream.into_split();
        let mut reader = FramedRead::new(read_half, RemoteCodec::new());
        let mut writer = FramedWrite::new(write_half, RemoteCodec::new());

        // Echo 回显 20 次小控制报文
        for _ in 0..20 {
            if let Some(Ok(msg)) = reader.next().await {
                writer.send(msg).await.expect("服务端回显消息");
            }
        }
    });

    let client_stream = TcpStream::connect(addr).await.expect("客户端连接服务端");
    client_stream.set_nodelay(true).expect("客户端设置 nodelay");
    assert!(
        client_stream.nodelay().expect("获取客户端 nodelay 状态"),
        "客户端套接字未成功启用 TCP_NODELAY"
    );

    let (read_half, write_half) = client_stream.into_split();
    let mut reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 测试 20 次小包（< 30 字节）往返时延：
    // 若开启 Nagle 算法且无对端快速 ACK，20 次往返将产生严重的 40ms~200ms 等待累计（总耗时会超过 800ms）。
    // 在 TCP_NODELAY 开启时，本地回环 20 次往返总耗时应在 50ms 以内（通常 < 5ms）。
    let start_time = Instant::now();
    for i in 0..20 {
        writer
            .send(RemoteMessage::Heartbeat {
                relay_id: format!("relay-{i}"),
            })
            .await
            .expect("客户端发送心跳");
        let reply = reader.next().await.expect("收到服务端回复").expect("解析");
        match reply {
            RemoteMessage::Heartbeat { relay_id } => {
                assert_eq!(relay_id, format!("relay-{i}"));
            }
            _ => panic!("收到非预期的消息"),
        }
    }
    let elapsed = start_time.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "20次小包往返耗时过长 ({elapsed:?})，疑似 Nagle 算法未关闭"
    );

    server_task.await.expect("服务端任务正常退出");
}

/// 挑战测试 2：Relay 客户端在 Outbound 发送缓冲区满（模拟对端完全不读）时，Inbound 命令接收仍不受阻断
#[tokio::test]
async fn test_challenge_relay_outbound_stall_does_not_block_inbound_active_path() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 Mock 服务端");
    let server_addr = listener.local_addr().expect("获取本地端口");

    let udp_socket = UdpSocket::bind("127.0.0.1:0").await.expect("绑定测试 UDP 发送端");
    let relay_udp_addr = UdpSocket::bind("127.0.0.1:0").await.expect("绑定测试 UDP 接收端").local_addr().unwrap();

    let relay_config = RelayConfig {
        id: "relay-adversarial-1".into(),
        server_addr: server_addr.to_string(),
        heartbeat_interval_ms: 100,
        data_timeout_ms: 2000,
        links: vec![LinkConfig {
            id: "udp-1".into(),
            kind: LinkKind::UdpUnicast {
                bind_addr: relay_udp_addr.to_string(),
                tx_target: "127.0.0.1:9".into(),
                allowed_source: None,
            },
            priority: 0,
        }],
        video: None,
        turntable: None,
    };

    let (snapshot_tx, mut snapshot_rx) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Relay });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (_reload_tx, reload_rx) = mpsc::channel(1);
    let (ready_tx, ready_rx) = oneshot::channel();

    // 启动真实的 Relay 运行时
    let relay_task = tokio::spawn(relay::run(
        relay_config,
        snapshot_tx,
        shutdown_rx,
        reload_rx,
        Some(ready_tx),
    ));

    ready_rx.await.expect("relay 准备就绪");

    // Mock 服务端接收 Relay 的连接
    let (server_stream, _) = listener.accept().await.expect("接受 relay 客户端连接");
    server_stream.set_nodelay(true).unwrap();
    let (read_half, write_half) = server_stream.into_split();
    let mut server_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut server_writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 完成握手：等待 RelayHello 并回复 HelloAck
    let hello = server_reader.next().await.unwrap().unwrap();
    assert_eq!(
        hello,
        RemoteMessage::RelayHello {
            relay_id: "relay-adversarial-1".into()
        }
    );
    server_writer
        .send(RemoteMessage::HelloAck {
            server_id: "mock-server".into(),
        })
        .await
        .unwrap();

    // 读出 LinkAnnounce 消息
    let announce = server_reader.next().await.unwrap().unwrap();
    match announce {
        RemoteMessage::LinkAnnounce { link_id, .. } => assert_eq!(link_id, "udp-1"),
        other => panic!("预期 LinkAnnounce，收到 {other:?}"),
    }

    // 此时确认 Relay 客户端已处于 connected 状态
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
    .expect("relay 应汇报 connected");

    // 对抗攻击：向 Relay 的 UDP 链路持续注入大量遥测数据（例如 300 个包，每个包 1024 字节）
    // 关键点：Mock 服务端**完全不调用 server_reader.next()**，让 TCP 接收窗口耗尽！
    let payload = vec![0xA5u8; 1024];
    for _ in 0..300 {
        let _ = udp_socket.send_to(&payload, relay_udp_addr).await;
        time::sleep(Duration::from_micros(100)).await;
    }

    // 给一点时间让 Relay 将数据刷入 TCP 发送缓冲区直至挂起
    time::sleep(Duration::from_millis(100)).await;

    // 核心对抗测试点：
    // 在 Relay 的底层 TCP 发送缓冲区被填满、writer.send() 必然挂起阻塞的前提下，
    // Mock 服务端向 Relay 发送一个 ActivePath 切换指令！
    server_writer
        .send(RemoteMessage::ActivePath {
            relay_id: "relay-adversarial-1".into(),
            link_id: Some("udp-1".into()),
        })
        .await
        .expect("服务端下发 ActivePath 指令");

    // 验证：Relay 端的 reader 协程是否能在 500ms 内不受 writer 阻塞的影响，及时处理该 ActivePath！
    let switched = time::timeout(Duration::from_millis(500), async {
        loop {
            if let SnapshotPayload::Relay(ref snap) = *snapshot_rx.borrow() {
                for link in &snap.links {
                    if link.path.link_id == "udp-1" && link.active {
                        return true;
                    }
                }
            }
            if snapshot_rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;

    // 清理
    let _ = shutdown_tx.send(true);
    let _ = relay_task.await;

    assert!(
        switched.is_ok(),
        "在发送缓冲区满/停滞时，Relay 无法及时读取并处理入站 ActivePath，存在读写耦合死锁！"
    );
}

/// 挑战测试 3：Relay 服务端在向 Relay 发送控制指令阻塞时，Relay 上报的遥测心跳依然能被服务端即刻读取
#[tokio::test]
async fn test_challenge_server_outbound_stall_does_not_block_inbound_telemetry() {
    let (relay_addr, gs_addr) = {
        let l1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let l2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        (l1.local_addr().unwrap().to_string(), l2.local_addr().unwrap().to_string())
    };

    let server_config = ServerConfig {
        id: "server-adversarial-1".into(),
        listen_addr: relay_addr.clone(),
        ground_station_listen_addr: gs_addr.clone(),
        ground_station_heartbeat_interval_ms: 50,
        heartbeat_timeout_ms: 1000,
        data_timeout_ms: 1000,
        video: None,
    };

    let (snapshot_tx, mut snapshot_rx) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
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

    ready_rx.await.expect("server 准备就绪");

    // 1. Mock Relay 连接 Server
    let relay_stream = TcpStream::connect(&relay_addr).await.expect("连接 server relay 端口");
    relay_stream.set_nodelay(true).unwrap();
    let (read_half, write_half) = relay_stream.into_split();
    let mut relay_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut relay_writer = FramedWrite::new(write_half, RemoteCodec::new());

    // Relay 握手并声明链路
    relay_writer
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-stall-test".into(),
        })
        .await
        .unwrap();
    let ack = relay_reader.next().await.unwrap().unwrap();
    match ack {
        RemoteMessage::HelloAck { .. } => {}
        other => panic!("预期 HelloAck，收到 {other:?}"),
    }

    relay_writer
        .send(RemoteMessage::LinkAnnounce {
            relay_id: "relay-stall-test".into(),
            link_id: "link-1".into(),
            kind: "udp_unicast".into(),
            priority: 0,
        })
        .await
        .unwrap();

    // 2. Mock Ground Station 连接 Server
    let mut gs_stream = TcpStream::connect(&gs_addr).await.expect("连接 ground station 端口");
    gs_stream.set_nodelay(true).unwrap();
    // 地面站握手
    tokio::io::AsyncWriteExt::write_all(&mut gs_stream, b"dqiang").await.unwrap();

    // 先发送一条有效机载数据让 server 将 link-1 设为活跃路径
    relay_writer
        .send(RemoteMessage::AircraftData {
            relay_id: "relay-stall-test".into(),
            link_id: "link-1".into(),
            payload: vec![1, 2, 3, 4],
        })
        .await
        .unwrap();

    // 等待 server 确认活跃路径
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
    .expect("server 应确立 active_path");

    // 3. 对抗攻击：地面站向服务端注入大量控制指令
    // 服务端会将这些控制指令推入该 Relay 的 outbound channel，并通过 writer 写入 Relay TCP 连接。
    // 关键点：Mock Relay 端的 `relay_reader` **故意停止读取**！
    let control_cmd = vec![0xCCu8; 512];
    for _ in 0..200 {
        let _ = tokio::io::AsyncWriteExt::write_all(&mut gs_stream, &control_cmd).await;
        time::sleep(Duration::from_micros(100)).await;
    }

    // 等待服务端将数据刷入 TCP 发送缓冲区，直到其发送端反压
    time::sleep(Duration::from_millis(100)).await;

    // 4. 关键对抗测试点：
    // 在服务端的 writer_task 因 Relay 端不读取而挂起阻塞时，
    // Mock Relay 持续向服务端上报新的遥测数据和心跳：
    let telemetry_before = {
        let snap = snapshot_rx.borrow().clone();
        if let SnapshotPayload::Server(s) = snap {
            s.relays.iter().find(|r| r.relay_id == "relay-stall-test").and_then(|r| r.links.first().map(|l| l.rx_bytes)).unwrap_or(0)
        } else {
            0
        }
    };

    let test_payload = vec![0xEFu8; 64];
    relay_writer
        .send(RemoteMessage::AircraftData {
            relay_id: "relay-stall-test".into(),
            link_id: "link-1".into(),
            payload: test_payload,
        })
        .await
        .expect("向服务端发送遥测数据");

    // 验证：服务端的 reader_task 是否能够不受其自身 writer_task 挂起的影响，在 500ms 内成功接收并更新 rx_bytes！
    let updated = time::timeout(Duration::from_millis(500), async {
        loop {
            if let SnapshotPayload::Server(ref snap) = *snapshot_rx.borrow() {
                if let Some(r) = snap.relays.iter().find(|r| r.relay_id == "relay-stall-test") {
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
        "在服务端向中继发送缓冲区满/反压时，服务端无法即刻读取中继上报的机载遥测数据！"
    );
}

/// 挑战测试 4：高频双向非对称并发压力测试（1000 帧全双工冲击）
#[tokio::test]
async fn test_challenge_bidirectional_duplex_stress_flood() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    const MSG_COUNT: usize = 1000;

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut reader = FramedRead::new(read_half, RemoteCodec::new());
        let mut writer = FramedWrite::new(write_half, RemoteCodec::new());

        let writer_handle = tokio::spawn(async move {
            for i in 0..MSG_COUNT {
                writer
                    .send(RemoteMessage::ControlData {
                        relay_id: "r1".into(),
                        link_id: "l1".into(),
                        payload: vec![(i % 256) as u8; 48],
                    })
                    .await
                    .expect("server write error");
            }
        });

        let mut read_count = 0;
        while let Some(msg) = reader.next().await {
            let msg = msg.expect("server read decode error");
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
    let (read_half, write_half) = client_stream.into_split();
    let mut client_reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut client_writer = FramedWrite::new(write_half, RemoteCodec::new());

    let client_writer_handle = tokio::spawn(async move {
        for i in 0..MSG_COUNT {
            client_writer
                .send(RemoteMessage::AircraftData {
                    relay_id: "r1".into(),
                    link_id: "l1".into(),
                    payload: vec![(i % 256) as u8; 96],
                })
                .await
                .expect("client write error");
        }
    });

    let mut client_read_count = 0;
    while let Some(msg) = client_reader.next().await {
        let msg = msg.expect("client read decode error");
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

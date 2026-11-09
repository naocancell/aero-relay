use std::net::TcpListener as StdTcpListener;
use std::time::Duration;
use futures_util::{SinkExt, StreamExt};
use relay_core::config::ServerConfig;
use relay_core::model::{Role, SnapshotPayload};
use relay_linkd::server;
use relay_linkd::transport::{RemoteCodec, RemoteMessage};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};

fn available_loopback_addrs() -> (String, String) {
    let relay_listener =
        StdTcpListener::bind("127.0.0.1:0").expect("reserve loopback test address");
    let ground_station_listener =
        StdTcpListener::bind("127.0.0.1:0").expect("reserve second loopback test address");
    let relay_addr = relay_listener
        .local_addr()
        .expect("reserved loopback address")
        .to_string();
    let ground_station_addr = ground_station_listener
        .local_addr()
        .expect("second reserved loopback address")
        .to_string();
    (relay_addr, ground_station_addr)
}

/// Helper to launch an in-process server runtime with clean shutdown controls.
async fn spawn_test_server() -> (
    String,
    String,
    watch::Sender<bool>,
    watch::Receiver<SnapshotPayload>,
    tokio::task::JoinHandle<Result<(), server::ServerError>>,
) {
    let (relay_addr, ground_station_addr) = available_loopback_addrs();
    let config = ServerConfig {
        id: "stress-server".into(),
        listen_addr: relay_addr.clone(),
        ground_station_listen_addr: ground_station_addr.clone(),
        ground_station_heartbeat_interval_ms: 20,
        heartbeat_timeout_ms: 300,
        data_timeout_ms: 300,
        video: None,
        auth: None,
    };
    let (snapshot_tx, snapshot_rx) =
        watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
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

    time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("server must become ready")
        .expect("ready signal received");

    (relay_addr, ground_station_addr, shutdown_tx, snapshot_rx, server_task)
}

#[tokio::test]
async fn test_clean_termination_when_relay_disconnects_or_aborts() {
    let (relay_addr, _gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server().await;

    // 1. 模拟正常连接并握手
    let stream = TcpStream::connect(&relay_addr).await.expect("connect relay");
    let mut relay = Framed::new(stream, RemoteCodec::new());
    relay
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-clean-term".into(),
        })
        .await
        .expect("send hello");

    assert!(matches!(
        relay.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    // 验证快照反映已连接
    time::timeout(Duration::from_secs(2), async {
        loop {
            if snapshot_rx.changed().await.is_ok() {
                if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                    if let Some(r) = s.relays.iter().find(|r| r.relay_id == "relay-clean-term") {
                        if r.connected {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("relay should appear connected in snapshot");

    // 2. 模拟客户端突发断开（drop socket）
    drop(relay);

    // 3. 验证服务端 handle_relay 任务正常退出，且快照在限定时间内标记为断开
    time::timeout(Duration::from_secs(2), async {
        loop {
            if snapshot_rx.changed().await.is_ok() {
                if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                    if let Some(r) = s.relays.iter().find(|r| r.relay_id == "relay-clean-term") {
                        if !r.connected {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("server should detect disconnection and mark relay disconnected without hang");

    // 4. 断开后再次建立连接，验证旧连接任务已完全让出资源
    let stream2 = TcpStream::connect(&relay_addr).await.expect("reconnect relay");
    let mut relay2 = Framed::new(stream2, RemoteCodec::new());
    relay2
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-clean-term".into(),
        })
        .await
        .expect("send hello again");
    assert!(matches!(
        relay2.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    // 优雅关闭服务
    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

fn make_valid_aircraft_frame() -> Vec<u8> {
    let mut frame = [0u8; 32];
    frame[0] = 0xEB;
    frame[1] = 0x90;
    for i in 2..30 {
        frame[i] = (i * 2) as u8;
    }
    let crc = relay_linkd::ground_station_protocol::crc_bytes(&frame[2..30]);
    frame[30..32].copy_from_slice(&crc);
    frame.to_vec()
}

#[tokio::test]
async fn test_rapid_reconnection_50_iterations_no_leak_or_deadlock() {
    let (relay_addr, _gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server().await;

    // 快速发起 50 次重连
    for i in 0..50 {
        let stream = TcpStream::connect(&relay_addr)
            .await
            .unwrap_or_else(|e| panic!("connect iteration {} failed: {}", i, e));
        let mut relay = Framed::new(stream, RemoteCodec::new());
        relay
            .send(RemoteMessage::RelayHello {
                relay_id: "rapid-reconnect-relay".into(),
            })
            .await
            .expect("send hello");

        let ack = time::timeout(Duration::from_millis(500), relay.next())
            .await
            .expect("ack timeout")
            .expect("ack some")
            .expect("ack ok");
        assert!(matches!(ack, RemoteMessage::HelloAck { .. }));

        relay
            .send(RemoteMessage::LinkAnnounce {
                relay_id: "rapid-reconnect-relay".into(),
                link_id: "link-rapid".into(),
                kind: "udp_unicast".into(),
                priority: 0,
            })
            .await
            .expect("send announce");

        relay
            .send(RemoteMessage::AircraftData {
                relay_id: "rapid-reconnect-relay".into(),
                link_id: "link-rapid".into(),
                payload: make_valid_aircraft_frame(),
            })
            .await
            .expect("send aircraft data");

        // 立即 drop 连接，开启下一轮迭代
        drop(relay);
    }

    // 第 51 次连接：保持连接并验证完全可用
    let stream_final = TcpStream::connect(&relay_addr).await.expect("connect final");
    let mut relay_final = Framed::new(stream_final, RemoteCodec::new());
    relay_final
        .send(RemoteMessage::RelayHello {
            relay_id: "rapid-reconnect-relay".into(),
        })
        .await
        .expect("send hello final");
    assert!(matches!(
        relay_final.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    relay_final
        .send(RemoteMessage::LinkAnnounce {
            relay_id: "rapid-reconnect-relay".into(),
            link_id: "link-rapid".into(),
            kind: "udp_unicast".into(),
            priority: 0,
        })
        .await
        .expect("announce final");

    relay_final
        .send(RemoteMessage::AircraftData {
            relay_id: "rapid-reconnect-relay".into(),
            link_id: "link-rapid".into(),
            payload: make_valid_aircraft_frame(),
        })
        .await
        .expect("send aircraft final");

    // 验证服务端快照状态健康
    time::timeout(Duration::from_secs(3), async {
        loop {
            if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow_and_update() {
                if let Some(r) = s.relays.iter().find(|r| r.relay_id == "rapid-reconnect-relay") {
                    if r.connected && s.active_path.is_some() {
                        break;
                    }
                }
            }
            if snapshot_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .expect("server state should remain completely consistent after 50 rapid reconnects");

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_high_throughput_full_duplex_bidirectional_traffic() {
    let (relay_addr, _gs_addr, shutdown_tx, _snapshot_rx, server_task) =
        spawn_test_server().await;

    let stream = TcpStream::connect(&relay_addr).await.expect("connect client");
    stream.set_nodelay(true).expect("set nodelay");
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 握手
    writer
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-throughput".into(),
        })
        .await
        .expect("send hello");
    assert!(matches!(
        reader.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    writer
        .send(RemoteMessage::LinkAnnounce {
            relay_id: "relay-throughput".into(),
            link_id: "link-fast".into(),
            kind: "udp_unicast".into(),
            priority: 0,
        })
        .await
        .expect("announce link");

    const MESSAGE_COUNT: usize = 1000;

    // 客户端发送端：高吞吐并发发送 1000 条 AircraftData
    let client_writer_task = tokio::spawn(async move {
        for i in 0..MESSAGE_COUNT {
            let mut payload = vec![0u8; 48];
            payload[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            writer
                .send(RemoteMessage::AircraftData {
                    relay_id: "relay-throughput".into(),
                    link_id: "link-fast".into(),
                    payload,
                })
                .await
                .expect("send high throughput aircraft data");
        }
        writer
    });

    // 客户端接收端：并发监听接收服务端的 ActivePath / ControlData 帧
    let client_reader_task = tokio::spawn(async move {
        let mut received = 0;
        while let Some(msg) = reader.next().await {
            let msg = msg.expect("decode message");
            if let RemoteMessage::ActivePath { .. } = msg {
                received += 1;
                // ActivePath 会随心跳和数据刷新持续发布
                if received >= 1 {
                    break;
                }
            }
        }
        assert!(received >= 1);
        reader
    });

    let (writer_res, reader_res) = tokio::join!(client_writer_task, client_reader_task);
    let mut writer = writer_res.expect("writer task joins");
    let _reader = reader_res.expect("reader task joins");

    // 验证完后关闭
    let _ = writer.send(RemoteMessage::Heartbeat {
        relay_id: "relay-throughput".into(),
    }).await;

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_backpressure_slow_reader_does_not_block_writer_duplex_split() {
    // 验证需求 R1 核心点：
    // 在一个全双工流上，当对端停止读取（形成发送反压）时，
    // 解耦后的另一侧能否继续实时接收和处理数据，绝不产生跨任务互锁死锁。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    // 服务端任务：模拟独立解耦的读写
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        stream.set_nodelay(true).expect("set nodelay");
        let (read_half, write_half) = stream.into_split();
        let mut server_reader = FramedRead::new(read_half, RemoteCodec::new());
        let mut server_writer = FramedWrite::new(write_half, RemoteCodec::new());

        // 服务端 Writer 协程：向客户端持续注入大量数据
        let writer_task = async {
            for i in 0..10_000 {
                server_writer
                    .send(RemoteMessage::ControlData {
                        relay_id: "relay-test".into(),
                        link_id: "link-1".into(),
                        payload: vec![i as u8; 256],
                    })
                    .await?;
            }
            Ok::<(), std::io::Error>(())
        };

        // 服务端 Reader 协程：持续接收来自客户端的数据
        let reader_task = async {
            let mut count = 0;
            while let Some(msg) = server_reader.next().await {
                let msg = msg?;
                if let RemoteMessage::AircraftData { .. } = msg {
                    count += 1;
                    if count >= 100 {
                        break;
                    }
                }
            }
            Ok::<usize, std::io::Error>(count)
        };

        // 使用 tokio::select! 运行 reader_task 和 writer_task
        // 关键验证：即使 writer_task 遭遇背压阻塞，reader_task 必须能够正常完成读取！
        let mut reader_completed = false;
        let mut reader_count = 0;

        tokio::select! {
            res = reader_task => {
                reader_count = res.expect("reader must succeed");
                reader_completed = true;
            }
            res = writer_task => {
                let _ = res;
            }
        }

        assert!(reader_completed, "Reader task must complete even if writer is backpressured");
        assert_eq!(reader_count, 100);
    });

    let client_stream = TcpStream::connect(addr).await.expect("connect client");
    client_stream.set_nodelay(true).expect("client nodelay");
    let (client_read, client_write) = client_stream.into_split();
    let mut client_writer = FramedWrite::new(client_write, RemoteCodec::new());
    // 客户端故意完全不轮询 client_read，制造发送背压
    let _client_reader = FramedRead::new(client_read, RemoteCodec::new());

    // 客户端持续向服务端发送 100 条 AircraftData
    for i in 0..100 {
        client_writer
            .send(RemoteMessage::AircraftData {
                relay_id: "relay-test".into(),
                link_id: "link-1".into(),
                payload: vec![i as u8; 32],
            })
            .await
            .expect("client send aircraft data");
    }

    // 验证服务端在 3 秒内顺利完成 reader 任务退出，绝不卡死
    time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server must not deadlock on backpressure")
        .expect("server task finishes");
}

#[tokio::test]
async fn test_relay_session_clean_termination_on_server_abort() {
    // 验证从 Relay 视角：当服务端关闭连接时，Relay 的 select! 块能否迅速退出并结束会话
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = Framed::new(stream, RemoteCodec::new());
        let _ = framed.next().await; // wait for hello
        framed.send(RemoteMessage::HelloAck { server_id: "test".into() }).await.expect("send ack");
        // 服务端稍作等待后立即主动关闭连接
        time::sleep(Duration::from_millis(50)).await;
        drop(framed);
    });

    // 模拟 Relay 端
    let stream = TcpStream::connect(addr).await.expect("connect");
    stream.set_nodelay(true).expect("nodelay");
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut writer = FramedWrite::new(write_half, RemoteCodec::new());

    // 握手
    writer.send(RemoteMessage::RelayHello { relay_id: "term-test".into() }).await.expect("hello");
    assert!(matches!(reader.next().await, Some(Ok(RemoteMessage::HelloAck { .. }))));

    // 运行仿制的 Relay session 读写协程模型
    let (aircraft_tx, mut aircraft_rx) = mpsc::channel::<Vec<u8>>(10);
    let writer_task = async {
        let mut interval = time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    writer.send(RemoteMessage::Heartbeat { relay_id: "term-test".into() }).await?;
                }
                data = aircraft_rx.recv() => {
                    let Some(data) = data else { return Ok::<(), std::io::Error>(()); };
                    writer.send(RemoteMessage::AircraftData {
                        relay_id: "term-test".into(),
                        link_id: "link-1".into(),
                        payload: data,
                    }).await?;
                }
            }
        }
    };

    let reader_task = async {
        loop {
            let Some(msg) = reader.next().await else {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "server closed session"));
            };
            let _ = msg?;
        }
    };

    let session_result = tokio::select! {
        res = writer_task => res,
        res = reader_task => res,
    };

    // 断言必须收到错误且迅速退出，绝不挂死
    assert!(session_result.is_err(), "Session should exit with error on server close");
    let err_kind = session_result.unwrap_err().kind();
    assert!(
        matches!(err_kind, std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset),
        "Expected UnexpectedEof or ConnectionReset, got {:?}",
        err_kind
    );

    drop(aircraft_tx);
    server_task.await.expect("server completes");
}

#[tokio::test]
async fn test_concurrent_competing_sessions_same_relay_displaces_gracefully() {
    let (relay_addr, _gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server().await;

    // 1. 建立客户端 A
    let stream_a = TcpStream::connect(&relay_addr).await.expect("connect client a");
    let mut relay_a = Framed::new(stream_a, RemoteCodec::new());
    relay_a
        .send(RemoteMessage::RelayHello {
            relay_id: "competing-relay".into(),
        })
        .await
        .expect("hello a");
    assert!(matches!(
        relay_a.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    // 等待服务端登记 relay-a
    time::timeout(Duration::from_secs(2), async {
        loop {
            if snapshot_rx.changed().await.is_ok() {
                if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                    if let Some(r) = s.relays.iter().find(|r| r.relay_id == "competing-relay") {
                        if r.connected {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("relay should be connected");

    // 2. 在客户端 A 仍保持在线（挂起等待）的情况下，客户端 B 接入同一 relay_id
    let stream_b = TcpStream::connect(&relay_addr).await.expect("connect client b");
    let mut relay_b = Framed::new(stream_b, RemoteCodec::new());
    relay_b
        .send(RemoteMessage::RelayHello {
            relay_id: "competing-relay".into(),
        })
        .await
        .expect("hello b");
    assert!(matches!(
        relay_b.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    // 3. 客户端 A 应该在短时间内收到来自服务端的断开信号（EOF 或 None）
    // 因为客户端 B 接入后，服务端旧连接对应的 outbound_tx 被淘汰 drop，导致旧任务在 select! 中退出并关闭 socket
    let a_closed = time::timeout(Duration::from_secs(2), async {
        // 读取直到收到 None 或 Err
        while let Some(msg) = relay_a.next().await {
            if msg.is_err() {
                return true;
            }
        }
        true
    })
    .await
    .expect("connection A must be displaced and terminated promptly by server");

    assert!(a_closed);

    // 4. 客户端 B 继续发送数据，必须完全畅通
    relay_b
        .send(RemoteMessage::AircraftData {
            relay_id: "competing-relay".into(),
            link_id: "link-b".into(),
            payload: vec![1, 2, 3],
        })
        .await
        .expect("relay b send aircraft data");

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}

#[tokio::test]
async fn test_server_handles_oversized_frame_attack_promptly() {
    use tokio::io::AsyncWriteExt;

    let (relay_addr, _gs_addr, shutdown_tx, mut snapshot_rx, server_task) =
        spawn_test_server().await;

    let mut stream = TcpStream::connect(&relay_addr).await.expect("connect attacker");
    stream.set_nodelay(true).expect("nodelay");

    // 先发送合法握手
    let mut relay = Framed::new(&mut stream, RemoteCodec::new());
    relay
        .send(RemoteMessage::RelayHello {
            relay_id: "attacker-relay".into(),
        })
        .await
        .expect("hello attacker");
    assert!(matches!(
        relay.next().await,
        Some(Ok(RemoteMessage::HelloAck { .. }))
    ));

    // 验证已连接
    time::timeout(Duration::from_secs(2), async {
        loop {
            if snapshot_rx.changed().await.is_ok() {
                if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                    if let Some(r) = s.relays.iter().find(|r| r.relay_id == "attacker-relay") {
                        if r.connected {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("attacker registered");

    drop(relay);

    // 伪造发送一个声称长达 16MB 的恶意畸变数据帧 (超出 4MB 限制)
    // 4字节大端长度前缀: 0x01, 0x00, 0x00, 0x00 = 16,777,216 bytes
    let malicious_header = [0x01, 0x00, 0x00, 0x00];
    stream
        .write_all(&malicious_header)
        .await
        .expect("write malicious header");

    // 验证服务端 handle_relay 任务立即在限定时间内识别异常、关闭连接并清理状态，绝不死锁或挂起
    time::timeout(Duration::from_secs(2), async {
        loop {
            if snapshot_rx.changed().await.is_ok() {
                if let SnapshotPayload::Server(ref s) = *snapshot_rx.borrow() {
                    if let Some(r) = s.relays.iter().find(|r| r.relay_id == "attacker-relay") {
                        if !r.connected {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("server must disconnect attacker session promptly on frame length violation");

    let _ = shutdown_tx.send(true);
    let _ = server_task.await;
}


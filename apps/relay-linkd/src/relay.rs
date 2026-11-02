use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use relay_core::config::{LinkConfig, LinkKind, RelayConfig};
use relay_core::model::{LinkHealth, LinkSnapshot, RelaySnapshot, Role, SnapshotPayload};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::transport::{RemoteCodec, RemoteMessage};
use crate::turntable_protocol::TurntableFrameAssembler;
use crate::video::{self, VideoHealth};
use crate::web_service::ReloadRequest;

const LINK_QUEUE_CAPACITY: usize = 128;
const AIRCRAFT_QUEUE_CAPACITY: usize = 256;
const SERVER_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

struct TurntableTransmitter {
    socket: Arc<UdpSocket>,
    target_addr: SocketAddr,
    assembler: TurntableFrameAssembler,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("relay I/O failed: {0}")]
    Io(#[from] io::Error),
}

pub async fn run(
    config: RelayConfig,
    snapshot_tx: watch::Sender<SnapshotPayload>,
    mut shutdown: watch::Receiver<bool>,
    mut reload_rx: mpsc::Receiver<ReloadRequest>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> Result<(), RelayError> {
    let started_at = Instant::now();
    let mut current_config = config;
    let mut runtime =
        start_runtime(current_config.clone(), started_at, snapshot_tx.clone()).await?;
    let mut reload_available = true;
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    loop {
        tokio::select! {
            result = &mut runtime.task => {
                return join_runtime_result(result);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    debug!("relay received shutdown signal");
                }
                let _ = runtime.shutdown.send(true);
                return join_runtime(runtime.task).await;
            }
            request = reload_rx.recv(), if reload_available => {
                let Some(request) = request else {
                    warn!("local IPC reload channel closed; keeping relay runtime alive");
                    reload_available = false;
                    continue;
                };
                let Some(next_config) = request.config.relay.clone() else {
                    let _ = request.response.send(Err("配置中缺少 [relay] 段".into()));
                    continue;
                };
                let previous_config = current_config.clone();
                let _ = runtime.shutdown.send(true);
                if let Err(error) = join_runtime(runtime.task).await {
                    let message = error.to_string();
                    let _ = request.response.send(Err(message));
                    return Err(error);
                }
                match start_runtime(next_config.clone(), started_at, snapshot_tx.clone()).await {
                    Ok(next_runtime) => {
                        current_config = next_config;
                        runtime = next_runtime;
                        let _ = request.response.send(Ok(()));
                    }
                    Err(error) => {
                        let message = error.to_string();
                        match start_runtime(previous_config, started_at, snapshot_tx.clone()).await {
                            Ok(previous_runtime) => {
                                runtime = previous_runtime;
                                let _ = request.response.send(Err(message));
                            }
                            Err(recovery_error) => {
                                let _ = request.response.send(Err(format!("{message}; 恢复旧配置失败: {recovery_error}")));
                                return Err(recovery_error);
                            }
                        }
                    }
                }
            }
        }
    }
}

struct RuntimeHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), RelayError>>,
}

async fn start_runtime(
    config: RelayConfig,
    started_at: Instant,
    snapshot_tx: watch::Sender<SnapshotPayload>,
) -> Result<RuntimeHandle, RelayError> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_once(
        config,
        started_at,
        snapshot_tx,
        shutdown_rx,
        Some(ready_tx),
    ));
    match ready_rx.await {
        Ok(()) => Ok(RuntimeHandle {
            shutdown: shutdown_tx,
            task,
        }),
        Err(_) => match task.await {
            Ok(Ok(())) => {
                Err(io::Error::other("relay runtime stopped before becoming ready").into())
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(io::Error::other(error.to_string()).into()),
        },
    }
}

async fn join_runtime(
    task: tokio::task::JoinHandle<Result<(), RelayError>>,
) -> Result<(), RelayError> {
    join_runtime_result(task.await)
}

fn join_runtime_result(
    result: Result<Result<(), RelayError>, tokio::task::JoinError>,
) -> Result<(), RelayError> {
    match result {
        Ok(result) => result,
        Err(error) => Err(io::Error::other(error.to_string()).into()),
    }
}

async fn run_once(
    config: RelayConfig,
    started_at: Instant,
    snapshot_tx: watch::Sender<SnapshotPayload>,
    mut shutdown: watch::Receiver<bool>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> Result<(), RelayError> {
    let health = Arc::new(Mutex::new(
        config
            .links
            .iter()
            .map(|link| (link.id.clone(), LinkHealth::default()))
            .collect::<BTreeMap<_, _>>(),
    ));
    let (video_active_tx, video_active_rx) = watch::channel(None);
    let connection = Arc::new(Mutex::new(ConnectionState {
        video_active_tx: Some(video_active_tx),
        ..ConnectionState::default()
    }));
    let connection_generation = Arc::new(AtomicU64::new(0));
    let (aircraft_tx, aircraft_rx) = mpsc::channel(AIRCRAFT_QUEUE_CAPACITY);
    let mut control_senders = BTreeMap::new();
    let mut link_tasks = Vec::new();
    let video_health = if config.video.is_some() {
        VideoHealth::configured()
    } else {
        VideoHealth::disabled()
    };
    let (video_shutdown_tx, video_shutdown_rx) = watch::channel(false);
    let mut video_task = config.video.clone().map(|video_config| {
        let video_health = video_health.clone();
        tokio::spawn(async move {
            video::supervise_relay_video(
                video_config,
                video_health,
                video_active_rx,
                video_shutdown_rx,
            )
            .await;
        })
    });

    for link in config.links.iter().cloned() {
        let (control_tx, control_rx) = mpsc::channel(LINK_QUEUE_CAPACITY);
        control_senders.insert(link.id.clone(), control_tx);
        link_tasks.push(tokio::spawn(link_supervisor(
            link,
            health.clone(),
            aircraft_tx.clone(),
            control_rx,
            connection_generation.clone(),
        )));
    }
    drop(aircraft_tx);

    let turntable_transmitter = if let Some(turntable_config) = &config.turntable {
        match UdpSocket::bind(&turntable_config.bind_addr).await {
            Ok(socket) => match turntable_config.target_addr.parse::<SocketAddr>() {
                Ok(target_addr) => {
                    info!(
                        bind = %turntable_config.bind_addr,
                        target = %target_addr,
                        "伺服转台固定UDP点播发送器初始化成功"
                    );
                    Some(TurntableTransmitter {
                        socket: Arc::new(socket),
                        target_addr,
                        assembler: TurntableFrameAssembler::new(),
                    })
                }
                Err(error) => {
                    warn!(error = %error, target = %turntable_config.target_addr, "伺服转台目标地址无效");
                    None
                }
            },
            Err(error) => {
                warn!(error = %error, bind = %turntable_config.bind_addr, "伺服转台本地UDP端口绑定失败");
                None
            }
        }
    } else {
        info!("中继站未配置 [relay.turntable] 伺服转台功能");
        None
    };

    let snapshot_task = tokio::spawn(publish_snapshots(
        config.clone(),
        started_at,
        health.clone(),
        connection.clone(),
        video_health,
        snapshot_tx,
    ));
    let mut server_task = tokio::spawn(run_server_loop(
        config,
        aircraft_rx,
        control_senders,
        connection,
        connection_generation,
        health,
        turntable_transmitter,
    ));
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    let result = tokio::select! {
        result = &mut server_task => {
            match result {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error.to_string()).into()),
            }
        }
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                debug!("relay received shutdown signal");
            }
            Ok(())
        }
    };

    snapshot_task.abort();
    server_task.abort();
    let _ = video_shutdown_tx.send(true);
    if let Some(mut task) = video_task.take()
        && time::timeout(Duration::from_secs(5), &mut task)
            .await
            .is_err()
    {
        task.abort();
    }
    for task in link_tasks {
        task.abort();
    }
    result
}

#[derive(Debug, Default)]
struct ConnectionState {
    connected: bool,
    active_link_id: Option<String>,
    last_error: Option<String>,
    video_active_tx: Option<watch::Sender<Option<Instant>>>,
}

impl ConnectionState {
    fn set_active_link(&mut self, link_id: Option<String>) {
        let video_ownership = link_id.as_ref().map(|_| Instant::now());
        self.active_link_id = link_id;
        if let Some(sender) = &self.video_active_tx {
            sender.send_replace(video_ownership);
        }
    }

    fn mark_connected(&mut self) {
        self.connected = true;
        self.set_active_link(None);
        self.last_error = None;
    }

    fn mark_disconnected(&mut self) {
        self.connected = false;
        self.set_active_link(None);
    }
}

async fn publish_snapshots(
    config: RelayConfig,
    started_at: Instant,
    health: Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    connection: Arc<Mutex<ConnectionState>>,
    video_health: VideoHealth,
    snapshot_tx: watch::Sender<SnapshotPayload>,
) {
    let mut ticker = time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let connection_state = connection.lock().expect("connection mutex poisoned");
        let health_state = health.lock().expect("health mutex poisoned");
        let snapshot = RelaySnapshot {
            role: Role::Relay,
            relay_id: config.id.clone(),
            server_connected: connection_state.connected,
            links: config
                .links
                .iter()
                .map(|link| LinkSnapshot {
                    path: relay_core::model::PathId::new(&config.id, &link.id),
                    kind: link.kind.name().into(),
                    adapter_ready: health_state
                        .get(&link.id)
                        .is_some_and(|health| health.adapter_ready),
                    last_rx_at: health_state
                        .get(&link.id)
                        .and_then(|health| health.last_rx_at_epoch_ms)
                        .map(|value| value.to_string()),
                    last_tx_at: health_state
                        .get(&link.id)
                        .and_then(|health| health.last_tx_at_epoch_ms)
                        .map(|value| value.to_string()),
                    rx_bytes: health_state
                        .get(&link.id)
                        .map_or(0, |health| health.rx_bytes),
                    tx_bytes: health_state
                        .get(&link.id)
                        .map_or(0, |health| health.tx_bytes),
                    rx_rate_bytes_per_second: health_state
                        .get(&link.id)
                        .map_or(0.0, |health| health.rx_rate_bytes_per_second),
                    tx_rate_bytes_per_second: health_state
                        .get(&link.id)
                        .map_or(0.0, |health| health.tx_rate_bytes_per_second),
                    last_error: health_state
                        .get(&link.id)
                        .and_then(|health| health.last_error.clone()),
                    active: connection_state.active_link_id.as_deref() == Some(link.id.as_str()),
                })
                .collect(),
            uptime_seconds: started_at.elapsed().as_secs(),
            recent_error: connection_state.last_error.clone(),
            video: video_health.snapshot(),
        };
        drop(health_state);
        drop(connection_state);
        let _ = snapshot_tx.send(SnapshotPayload::Relay(snapshot));
    }
}

async fn run_server_loop(
    config: RelayConfig,
    mut aircraft_rx: mpsc::Receiver<LocalAircraftData>,
    control_senders: BTreeMap<String, mpsc::Sender<ControlCommand>>,
    connection: Arc<Mutex<ConnectionState>>,
    connection_generation: Arc<AtomicU64>,
    health: Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    mut turntable: Option<TurntableTransmitter>,
) -> Result<(), RelayError> {
    let mut attempt = 0usize;
    'connection: loop {
        connection_generation.fetch_add(1, Ordering::AcqRel);
        match TcpStream::connect(&config.server_addr).await {
            Ok(stream) => {
                if let Err(error) = stream.set_nodelay(true) {
                    warn!(error = %error, "无法设置 relay client TcpStream nodelay");
                }
                let (read_half, write_half) = stream.into_split();
                let mut reader = FramedRead::new(read_half, RemoteCodec::new());
                let mut writer = FramedWrite::new(write_half, RemoteCodec::new());
                if let Err(error) = complete_handshake(&config, &mut reader, &mut writer).await {
                    record_connection_error(&connection, &error);
                    drain_aircraft(&mut aircraft_rx);
                    {
                        let mut state = connection.lock().expect("connection mutex poisoned");
                        state.mark_disconnected();
                    }
                    connection_generation.fetch_add(1, Ordering::AcqRel);
                    time::sleep(retry_delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                    continue 'connection;
                }
                attempt = 0;
                {
                    let mut state = connection.lock().expect("connection mutex poisoned");
                    state.mark_connected();
                }
                for link in &config.links {
                    if let Err(error) = writer
                        .send(RemoteMessage::LinkAnnounce {
                            relay_id: config.id.clone(),
                            link_id: link.id.clone(),
                            kind: link.kind.name().into(),
                            priority: link.priority,
                        })
                        .await
                    {
                        record_connection_error(&connection, &error);
                        drain_aircraft(&mut aircraft_rx);
                        {
                            let mut state = connection.lock().expect("connection mutex poisoned");
                            state.mark_disconnected();
                        }
                        connection_generation.fetch_add(1, Ordering::AcqRel);
                        time::sleep(retry_delay(attempt)).await;
                        attempt = attempt.saturating_add(1);
                        continue 'connection;
                    }
                }
                match run_connected_session(
                    &config,
                    reader,
                    writer,
                    &mut aircraft_rx,
                    &control_senders,
                    &connection,
                    &connection_generation,
                    &health,
                    &mut turntable,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        record_connection_error(&connection, &error);
                        warn!(error = %error, "relay server session ended");
                    }
                }
                drain_aircraft(&mut aircraft_rx);
            }
            Err(error) => {
                record_connection_error(&connection, &error);
                drain_aircraft(&mut aircraft_rx);
            }
        }
        {
            let mut state = connection.lock().expect("connection mutex poisoned");
            state.mark_disconnected();
        }
        connection_generation.fetch_add(1, Ordering::AcqRel);
        time::sleep(retry_delay(attempt)).await;
        attempt = attempt.saturating_add(1);
    }
}

async fn complete_handshake<R, W>(
    config: &RelayConfig,
    reader: &mut FramedRead<R, RemoteCodec>,
    writer: &mut FramedWrite<W, RemoteCodec>,
) -> Result<(), io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    writer
        .send(RemoteMessage::RelayHello {
            relay_id: config.id.clone(),
        })
        .await?;
    match time::timeout(SERVER_HANDSHAKE_TIMEOUT, reader.next())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for server hello acknowledgement",
            )
        })? {
        Some(Ok(RemoteMessage::HelloAck { .. })) => Ok(()),
        Some(Ok(RemoteMessage::Rejected { reason })) => {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, reason))
        }
        Some(Ok(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "server did not acknowledge relay hello",
        )),
        Some(Err(error)) => Err(error),
        None => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "server closed before acknowledging relay hello",
        )),
    }
}

async fn run_connected_session<R, W>(
    config: &RelayConfig,
    mut reader: FramedRead<R, RemoteCodec>,
    mut writer: FramedWrite<W, RemoteCodec>,
    aircraft_rx: &mut mpsc::Receiver<LocalAircraftData>,
    control_senders: &BTreeMap<String, mpsc::Sender<ControlCommand>>,
    connection: &Arc<Mutex<ConnectionState>>,
    connection_generation: &Arc<AtomicU64>,
    health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    turntable: &mut Option<TurntableTransmitter>,
) -> Result<(), io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let writer_task = async {
        let mut heartbeat = time::interval(Duration::from_millis(config.heartbeat_interval_ms));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    writer.send(RemoteMessage::Heartbeat { relay_id: config.id.clone() }).await?;
                    let statuses = health
                        .lock()
                        .expect("health mutex poisoned")
                        .iter()
                        .map(|(link_id, health)| RemoteMessage::LinkStatus {
                            relay_id: config.id.clone(),
                            link_id: link_id.clone(),
                            adapter_ready: health.adapter_ready,
                            last_tx_at: health.last_tx_at_epoch_ms,
                            tx_bytes: health.tx_bytes,
                            tx_rate_bytes_per_second: health.tx_rate_bytes_per_second,
                            last_error: health.last_error.clone(),
                        })
                        .collect::<Vec<_>>();
                    for status in statuses {
                        writer.send(status).await?;
                    }
                }
                data = aircraft_rx.recv() => {
                    let Some(data) = data else { return Ok(()); };
                    writer.send(RemoteMessage::AircraftData {
                        relay_id: config.id.clone(),
                        link_id: data.link_id,
                        payload: data.payload,
                    }).await?;
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };

    let reader_task = async {
        loop {
            let Some(message) = reader.next().await else {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "server closed relay session"));
            };
            let message = message?;
            match message {
                RemoteMessage::ActivePath { relay_id, link_id } if relay_id == config.id => {
                    connection
                        .lock()
                        .expect("connection mutex poisoned")
                        .set_active_link(link_id);
                }
                RemoteMessage::ControlData { relay_id, link_id, payload } if relay_id == config.id => {
                    let outbound_payload = if let Some(turntable) = turntable.as_mut() {
                        let (frames, passthrough) = turntable.assembler.process(&payload);
                        for frame in frames {
                            if let Err(error) = turntable.socket.send_to(&frame, turntable.target_addr).await {
                                warn!(error = %error, target = %turntable.target_addr, "向伺服转台 UDP 发送失败");
                            }
                        }
                        passthrough
                    } else {
                        payload
                    };
                    if !outbound_payload.is_empty() {
                        if let Some(sender) = control_senders.get(&link_id) {
                            let command = ControlCommand {
                                generation: connection_generation.load(Ordering::Acquire),
                                payload: outbound_payload,
                            };
                            if let Err(error) = sender.try_send(command) {
                                warn!(link_id, error = %error, "dropping control data because local link queue is full");
                            }
                        }
                    }
                }
                RemoteMessage::Rejected { reason } => {
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, reason));
                }
                _ => {}
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };

    tokio::select! {
        res = writer_task => res,
        res = reader_task => res,
    }
}

#[derive(Debug)]
struct LocalAircraftData {
    link_id: String,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct ControlCommand {
    generation: u64,
    payload: Vec<u8>,
}

async fn link_supervisor(
    config: LinkConfig,
    health: Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    aircraft_tx: mpsc::Sender<LocalAircraftData>,
    mut control_rx: mpsc::Receiver<ControlCommand>,
    connection_generation: Arc<AtomicU64>,
) {
    loop {
        set_adapter_state(&health, &config.id, false, None);
        let result = match &config.kind {
            LinkKind::Serial { path, baud_rate } => {
                run_serial(
                    &config.id,
                    path,
                    *baud_rate,
                    &health,
                    &aircraft_tx,
                    &mut control_rx,
                    &connection_generation,
                )
                .await
            }
            LinkKind::UdpUnicast {
                bind_addr,
                tx_target,
                allowed_source,
            } => {
                run_udp_unicast(
                    &config.id,
                    bind_addr,
                    tx_target,
                    allowed_source.as_deref(),
                    &health,
                    &aircraft_tx,
                    &mut control_rx,
                    &connection_generation,
                )
                .await
            }
            LinkKind::UdpMulticast {
                bind_addr,
                group_addr,
                interface_addr,
                tx_target,
                multicast_ttl,
                multicast_loop,
            } => {
                run_udp_multicast(
                    &config.id,
                    bind_addr,
                    group_addr,
                    interface_addr,
                    tx_target,
                    *multicast_ttl,
                    *multicast_loop,
                    &health,
                    &aircraft_tx,
                    &mut control_rx,
                    &connection_generation,
                )
                .await
            }
        };
        if let Err(error) = result {
            set_adapter_state(&health, &config.id, false, Some(error.to_string()));
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_serial(
    link_id: &str,
    path: &str,
    baud_rate: u32,
    health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    aircraft_tx: &mpsc::Sender<LocalAircraftData>,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    connection_generation: &Arc<AtomicU64>,
) -> Result<(), io::Error> {
    let mut serial = tokio_serial::new(path, baud_rate).open_native_async()?;
    set_adapter_state(health, link_id, true, None);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            result = serial.read(&mut buffer) => {
                let length = result?;
                if length == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "serial port closed"));
                }
                record_rx(health, link_id, length);
                publish_aircraft(aircraft_tx, LocalAircraftData { link_id: link_id.into(), payload: buffer[..length].to_vec() });
            }
            control = control_rx.recv() => {
                let Some(control) = control else { return Ok(()); };
                if control.generation != connection_generation.load(Ordering::Acquire) { continue; }
                serial.write_all(&control.payload).await?;
                record_tx(health, link_id, control.payload.len());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_udp_unicast(
    link_id: &str,
    bind_addr: &str,
    tx_target: &str,
    allowed_source: Option<&str>,
    health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    aircraft_tx: &mpsc::Sender<LocalAircraftData>,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    connection_generation: &Arc<AtomicU64>,
) -> Result<(), io::Error> {
    let socket = UdpSocket::bind(bind_addr).await?;
    let target: SocketAddr = tx_target.parse().map_err(invalid_socket)?;
    let allowed_source = allowed_source
        .map(str::parse::<SocketAddr>)
        .transpose()
        .map_err(invalid_socket)?;
    set_adapter_state(health, link_id, true, None);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buffer) => {
                let (length, source) = result?;
                if allowed_source.is_some_and(|allowed| allowed != source) { continue; }
                record_rx(health, link_id, length);
                publish_aircraft(aircraft_tx, LocalAircraftData { link_id: link_id.into(), payload: buffer[..length].to_vec() });
            }
            control = control_rx.recv() => {
                let Some(control) = control else { return Ok(()); };
                if control.generation != connection_generation.load(Ordering::Acquire) { continue; }
                socket.send_to(&control.payload, target).await?;
                record_tx(health, link_id, control.payload.len());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_udp_multicast(
    link_id: &str,
    bind_addr: &str,
    group_addr: &str,
    interface_addr: &str,
    tx_target: &str,
    multicast_ttl: u32,
    multicast_loop: bool,
    health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    aircraft_tx: &mpsc::Sender<LocalAircraftData>,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    connection_generation: &Arc<AtomicU64>,
) -> Result<(), io::Error> {
    let socket = UdpSocket::bind(bind_addr).await?;
    let group: Ipv4Addr = group_addr.parse().map_err(invalid_ip)?;
    let interface: Ipv4Addr = interface_addr.parse().map_err(invalid_ip)?;
    socket.join_multicast_v4(group, interface)?;
    socket.set_multicast_ttl_v4(multicast_ttl)?;
    socket.set_multicast_loop_v4(multicast_loop)?;
    let target: SocketAddr = tx_target.parse().map_err(invalid_socket)?;
    set_adapter_state(health, link_id, true, None);
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buffer) => {
                let (length, _) = result?;
                record_rx(health, link_id, length);
                publish_aircraft(aircraft_tx, LocalAircraftData { link_id: link_id.into(), payload: buffer[..length].to_vec() });
            }
            control = control_rx.recv() => {
                let Some(control) = control else { return Ok(()); };
                if control.generation != connection_generation.load(Ordering::Acquire) { continue; }
                socket.send_to(&control.payload, target).await?;
                record_tx(health, link_id, control.payload.len());
            }
        }
    }
}

fn publish_aircraft(aircraft_tx: &mpsc::Sender<LocalAircraftData>, data: LocalAircraftData) {
    if aircraft_tx.try_send(data).is_err() {
        debug!("dropping aircraft data because the server queue is full");
    }
}

fn set_adapter_state(
    health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>,
    link_id: &str,
    adapter_ready: bool,
    error: Option<String>,
) {
    if let Some(link) = health
        .lock()
        .expect("health mutex poisoned")
        .get_mut(link_id)
    {
        link.adapter_ready = adapter_ready;
        link.last_error = error;
    }
}

fn record_rx(health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>, link_id: &str, bytes: usize) {
    if let Some(link) = health
        .lock()
        .expect("health mutex poisoned")
        .get_mut(link_id)
    {
        link.mark_rx(Instant::now(), bytes);
    }
}

fn record_tx(health: &Arc<Mutex<BTreeMap<String, LinkHealth>>>, link_id: &str, bytes: usize) {
    if let Some(link) = health
        .lock()
        .expect("health mutex poisoned")
        .get_mut(link_id)
    {
        link.mark_tx(Instant::now(), bytes);
    }
}

fn record_connection_error(
    connection: &Arc<Mutex<ConnectionState>>,
    error: &dyn std::fmt::Display,
) {
    connection
        .lock()
        .expect("connection mutex poisoned")
        .last_error = Some(error.to_string());
}

fn drain_aircraft(receiver: &mut mpsc::Receiver<LocalAircraftData>) {
    while receiver.try_recv().is_ok() {}
}

fn retry_delay(attempt: usize) -> Duration {
    match attempt {
        0 => Duration::from_secs(1),
        1 => Duration::from_secs(2),
        _ => Duration::from_secs(5),
    }
}

pub fn list_serial_ports() -> Result<Vec<String>, io::Error> {
    #[allow(unused_mut)]
    let mut names = tokio_serial::available_ports()
        .map_err(|error| io::Error::other(error.to_string()))?
        .into_iter()
        .map(|port| port.port_name)
        .collect::<Vec<_>>();

    // 在 Linux 环境下补充扫描 SoC 板载平台硬件串口 (如 RK3588 的 /dev/ttyS4 等)
    #[cfg(target_os = "linux")]
    {
        if let Ok(entries) = std::fs::read_dir("/sys/class/tty") {
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if file_name.starts_with("ttyS") || file_name.starts_with("ttyUSB") || file_name.starts_with("ttyACM") {
                    let path = entry.path();
                    if let Ok(target) = std::fs::read_link(&path) {
                        let target_str = target.to_string_lossy();
                        // 真实物理平台串口或 USB 串口符号链接会指向具体的驱动与总线设备节点
                        if target_str.contains(".serial/") || target_str.contains("/serial8250") || target_str.contains("/usb") {
                            names.push(format!("/dev/{}", file_name));
                        }
                    }
                }
            }
        }
    }

    Ok(normalize_serial_port_names(names))
}

fn normalize_serial_port_names<I, S>(names: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut names = names.into_iter().map(Into::into).collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

fn invalid_socket(error: std::net::AddrParseError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

fn invalid_ip(error: std::net::AddrParseError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

#[cfg(test)]
mod tests {
    use super::normalize_serial_port_names;
    use super::*;
    use relay_core::config::{Config, LinkConfig, LinkKind};
    use tokio::net::TcpListener;
    use tokio_util::codec::Framed;

    #[test]
    fn serial_port_names_are_sorted_and_unique() {
        assert_eq!(
            normalize_serial_port_names(["COM10", "COM3", "COM3"]),
            vec!["COM10", "COM3"]
        );
    }

    #[test]
    fn active_path_controls_local_video_ownership() {
        let (video_active_tx, video_active_rx) = watch::channel(None);
        let mut connection = ConnectionState {
            video_active_tx: Some(video_active_tx),
            ..ConnectionState::default()
        };

        connection.set_active_link(Some("serial-1".into()));
        assert!(video_active_rx.borrow().is_some());

        connection.mark_disconnected();
        assert!(video_active_rx.borrow().is_none());
        assert!(connection.active_link_id.is_none());
    }

    #[tokio::test]
    async fn relay_runtime_acknowledges_hot_reload_without_stopping_ipc_owner() {
        let config = RelayConfig {
            id: "relay-a".into(),
            server_addr: "127.0.0.1:1".into(),
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: vec![LinkConfig {
                id: "udp-1".into(),
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
        let (snapshot_tx, _) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Relay });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (reload_tx, reload_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(run(
            config.clone(),
            snapshot_tx,
            shutdown_rx,
            reload_rx,
            Some(ready_tx),
        ));
        time::timeout(Duration::from_secs(1), ready_rx)
            .await
            .expect("relay runtime should become ready")
            .expect("relay ready signal");

        let (response_tx, response_rx) = oneshot::channel();
        reload_tx
            .send(ReloadRequest {
                config: Config {
                    relay: Some(config),
                    server: None,
                },
                response: response_tx,
            })
            .await
            .expect("reload request should reach relay runtime");
        assert_eq!(
            time::timeout(Duration::from_secs(1), response_rx)
                .await
                .expect("reload response")
                .expect("reload response channel"),
            Ok(())
        );
        assert!(!task.is_finished());

        shutdown_tx.send(true).expect("shutdown relay runtime");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("relay runtime should stop")
            .expect("relay runtime task")
            .expect("relay runtime should stop cleanly");
    }

    #[tokio::test]
    async fn relay_does_not_report_connected_before_server_ack() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay test server");
        let config = RelayConfig {
            id: "relay-awaiting-ack".into(),
            server_addr: listener
                .local_addr()
                .expect("relay test server address")
                .to_string(),
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: Vec::new(),
            video: None,
            turntable: None,
        };
        let (aircraft_tx, aircraft_rx) = mpsc::channel(1);
        let _aircraft_tx = aircraft_tx;
        let connection = Arc::new(Mutex::new(ConnectionState::default()));
        let connection_generation = Arc::new(AtomicU64::new(0));
        let health = Arc::new(Mutex::new(BTreeMap::new()));
        let task = tokio::spawn(run_server_loop(
            config,
            aircraft_rx,
            BTreeMap::new(),
            connection.clone(),
            connection_generation,
            health,
            None,
        ));

        let (stream, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("relay should connect to the test server")
            .expect("accept relay connection");
        let mut session = Framed::new(stream, RemoteCodec::new());
        assert_eq!(
            time::timeout(Duration::from_secs(1), session.next())
                .await
                .expect("relay hello timeout")
                .expect("relay hello frame")
                .expect("relay hello decode"),
            RemoteMessage::RelayHello {
                relay_id: "relay-awaiting-ack".into(),
            }
        );

        time::sleep(Duration::from_millis(50)).await;
        assert!(
            !connection
                .lock()
                .expect("connection mutex poisoned")
                .connected,
            "a TCP connection without HelloAck must remain disconnected"
        );

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn relay_retries_when_server_does_not_ack_hello() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay test server");
        let config = RelayConfig {
            id: "relay-handshake-timeout".into(),
            server_addr: listener
                .local_addr()
                .expect("relay test server address")
                .to_string(),
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: Vec::new(),
            video: None,
            turntable: None,
        };
        let (aircraft_tx, aircraft_rx) = mpsc::channel(1);
        let _aircraft_tx = aircraft_tx;
        let connection = Arc::new(Mutex::new(ConnectionState::default()));
        let connection_generation = Arc::new(AtomicU64::new(0));
        let health = Arc::new(Mutex::new(BTreeMap::new()));
        let task = tokio::spawn(run_server_loop(
            config,
            aircraft_rx,
            BTreeMap::new(),
            connection.clone(),
            connection_generation,
            health,
            None,
        ));

        let (first_stream, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("relay should make its first connection")
            .expect("accept first relay connection");
        let mut first_session = Framed::new(first_stream, RemoteCodec::new());
        assert_eq!(
            time::timeout(Duration::from_secs(1), first_session.next())
                .await
                .expect("first relay hello timeout")
                .expect("first relay hello frame")
                .expect("first relay hello decode"),
            RemoteMessage::RelayHello {
                relay_id: "relay-handshake-timeout".into(),
            }
        );

        let (second_stream, _) = time::timeout(Duration::from_secs(4), listener.accept())
            .await
            .expect("relay should retry after the missing server acknowledgement")
            .expect("accept retried relay connection");
        let mut second_session = Framed::new(second_stream, RemoteCodec::new());
        assert_eq!(
            time::timeout(Duration::from_secs(1), second_session.next())
                .await
                .expect("retried relay hello timeout")
                .expect("retried relay hello frame")
                .expect("retried relay hello decode"),
            RemoteMessage::RelayHello {
                relay_id: "relay-handshake-timeout".into(),
            }
        );
        assert!(
            !connection
                .lock()
                .expect("connection mutex poisoned")
                .connected,
            "a retry awaiting HelloAck must remain disconnected"
        );

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn relay_reconnects_after_server_closes_session() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay test server");
        let server_addr = listener
            .local_addr()
            .expect("relay test server address")
            .to_string();
        let config = RelayConfig {
            id: "relay-reconnect".into(),
            server_addr,
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: Vec::new(),
            video: None,
            turntable: None,
        };
        let (aircraft_tx, aircraft_rx) = mpsc::channel(1);
        let _aircraft_tx = aircraft_tx;
        let connection = Arc::new(Mutex::new(ConnectionState::default()));
        let connection_generation = Arc::new(AtomicU64::new(0));
        let health = Arc::new(Mutex::new(BTreeMap::new()));

        let task = tokio::spawn(run_server_loop(
            config,
            aircraft_rx,
            BTreeMap::new(),
            connection,
            connection_generation,
            health,
            None,
        ));

        let (first_stream, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("relay should make its first connection")
            .expect("accept first relay connection");
        let mut first_session = Framed::new(first_stream, RemoteCodec::new());
        assert_eq!(
            time::timeout(Duration::from_secs(1), first_session.next())
                .await
                .expect("first relay hello timeout")
                .expect("first relay hello frame")
                .expect("first relay hello decode"),
            RemoteMessage::RelayHello {
                relay_id: "relay-reconnect".into(),
            }
        );
        first_session
            .send(RemoteMessage::HelloAck {
                server_id: "server-a".into(),
            })
            .await
            .expect("acknowledge first relay connection");
        drop(first_session);

        let (second_stream, _) = time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("relay should reconnect after the first session closes")
            .expect("accept second relay connection");
        let mut second_session = Framed::new(second_stream, RemoteCodec::new());
        assert_eq!(
            time::timeout(Duration::from_secs(1), second_session.next())
                .await
                .expect("second relay hello timeout")
                .expect("second relay hello frame")
                .expect("second relay hello decode"),
            RemoteMessage::RelayHello {
                relay_id: "relay-reconnect".into(),
            }
        );

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn relay_forwards_turntable_frames_to_target_over_udp() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let server_addr = listener
            .local_addr()
            .expect("server address")
            .to_string();

        let turntable_receiver = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind mock turntable udp receiver");
        let turntable_target = turntable_receiver
            .local_addr()
            .expect("mock turntable address")
            .to_string();

        let config = RelayConfig {
            id: "relay-turntable-test".into(),
            server_addr,
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: Vec::new(),
            video: None,
            turntable: Some(relay_core::config::TurntableConfig {
                bind_addr: "127.0.0.1:0".into(),
                target_addr: turntable_target,
            }),
        };

        let (aircraft_tx, aircraft_rx) = mpsc::channel(1);
        let _aircraft_tx = aircraft_tx;
        let connection = Arc::new(Mutex::new(ConnectionState::default()));
        let connection_generation = Arc::new(AtomicU64::new(0));
        let health = Arc::new(Mutex::new(BTreeMap::new()));

        let turntable_transmitter = Some(TurntableTransmitter {
            socket: Arc::new(UdpSocket::bind("127.0.0.1:0").await.expect("bind tx socket")),
            target_addr: config.turntable.as_ref().unwrap().target_addr.parse().unwrap(),
            assembler: TurntableFrameAssembler::new(),
        });

        let (control_tx, mut control_rx) = mpsc::channel(10);
        let mut control_senders = BTreeMap::new();
        control_senders.insert("link-a".to_string(), control_tx);

        let task = tokio::spawn(run_server_loop(
            config,
            aircraft_rx,
            control_senders,
            connection,
            connection_generation,
            health,
            turntable_transmitter,
        ));

        let (stream, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("relay connects")
            .expect("accept relay");
        let mut session = Framed::new(stream, RemoteCodec::new());

        let _ = session.next().await; // RelayHello
        session
            .send(RemoteMessage::HelloAck {
                server_id: "server-a".into(),
            })
            .await
            .expect("send hello ack");

        // 构造转台 32 字节帧：EB 90 02 01 C2 ... CRC16
        let mut expected_frame = [0u8; 32];
        expected_frame[0] = 0xEB;
        expected_frame[1] = 0x90;
        expected_frame[2] = 0x02;
        expected_frame[3] = 0x01;
        expected_frame[4] = 0xC2;
        for i in 5..30 {
            expected_frame[i] = 0x88;
        }
        let [c0, c1] = crate::ground_station_protocol::crc_bytes(&expected_frame[2..30]);
        expected_frame[30] = c0;
        expected_frame[31] = c1;

        // 1. 服务端下发包含杂质和转台帧的 ControlData
        let mut payload = vec![0x11, 0x22, 0x33];
        payload.extend_from_slice(&expected_frame);
        payload.extend_from_slice(&[0x44, 0x55]);

        session
            .send(RemoteMessage::ControlData {
                relay_id: "relay-turntable-test".into(),
                link_id: "link-a".into(),
                payload,
            })
            .await
            .expect("send control data");

        // 验证转台 UDP 接收端收到刚好 32 字节的转台帧
        let mut recv_buf = vec![0u8; 1024];
        let (len, _) = time::timeout(
            Duration::from_secs(2),
            turntable_receiver.recv_from(&mut recv_buf),
        )
        .await
        .expect("turntable receive timeout")
        .expect("turntable receive");

        assert_eq!(len, 32);
        assert_eq!(&recv_buf[..32], &expected_frame);

        // 验证机载链路只收到了透传的杂质数据，转台控制帧被完全摘除
        let command = time::timeout(Duration::from_secs(1), control_rx.recv())
            .await
            .expect("receive timeout")
            .expect("command received");
        assert_eq!(command.payload, vec![0x11, 0x22, 0x33, 0x44, 0x55]);

        // 2. 服务端下发纯转台控制帧
        session
            .send(RemoteMessage::ControlData {
                relay_id: "relay-turntable-test".into(),
                link_id: "link-a".into(),
                payload: expected_frame.to_vec(),
            })
            .await
            .expect("send pure turntable frame");

        // 转台再次收到 32 字节
        let (len, _) = time::timeout(
            Duration::from_secs(2),
            turntable_receiver.recv_from(&mut recv_buf),
        )
        .await
        .expect("turntable receive timeout")
        .expect("turntable receive");
        assert_eq!(len, 32);
        assert_eq!(&recv_buf[..32], &expected_frame);

        // 机载链路没有任何数据收到（超时无数据）
        assert!(time::timeout(Duration::from_millis(200), control_rx.recv())
            .await
            .is_err());

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn relay_session_socket_nodelay_and_concurrent_duplex() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay test server");
        let server_addr = listener
            .local_addr()
            .expect("relay test server address")
            .to_string();
        let config = RelayConfig {
            id: "relay-duplex".into(),
            server_addr,
            heartbeat_interval_ms: 20,
            data_timeout_ms: 100,
            links: Vec::new(),
            video: None,
            turntable: None,
        };
        let (aircraft_tx, aircraft_rx) = mpsc::channel(100);
        let connection = Arc::new(Mutex::new(ConnectionState::default()));
        let connection_generation = Arc::new(AtomicU64::new(0));
        let health = Arc::new(Mutex::new(BTreeMap::new()));

        let task = tokio::spawn(run_server_loop(
            config,
            aircraft_rx,
            BTreeMap::new(),
            connection.clone(),
            connection_generation,
            health,
            None,
        ));

        let (server_stream, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("relay should connect")
            .expect("accept relay connection");
        let _ = server_stream.set_nodelay(true);

        let (read_half, write_half) = server_stream.into_split();
        let mut server_reader = FramedRead::new(read_half, RemoteCodec::new());
        let mut server_writer = FramedWrite::new(write_half, RemoteCodec::new());

        let hello = time::timeout(Duration::from_secs(1), server_reader.next())
            .await
            .expect("hello timeout")
            .expect("hello received")
            .expect("hello decode");
        assert_eq!(
            hello,
            RemoteMessage::RelayHello {
                relay_id: "relay-duplex".into(),
            }
        );

        server_writer
            .send(RemoteMessage::HelloAck {
                server_id: "test-server".into(),
            })
            .await
            .expect("send ack");

        server_writer
            .send(RemoteMessage::ActivePath {
                relay_id: "relay-duplex".into(),
                link_id: Some("link-1".into()),
            })
            .await
            .expect("send active path");

        aircraft_tx
            .send(LocalAircraftData {
                link_id: "link-1".into(),
                payload: vec![1, 2, 3],
            })
            .await
            .expect("send aircraft data");

        time::timeout(Duration::from_secs(1), async {
            loop {
                let active = connection.lock().unwrap().active_link_id.clone();
                if active == Some("link-1".into()) {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("relay should process ActivePath concurrently without deadlock");

        let msg = time::timeout(Duration::from_secs(1), server_reader.next())
            .await
            .expect("telemetry timeout")
            .expect("telemetry received")
            .expect("telemetry decode");
        match msg {
            RemoteMessage::AircraftData { payload, .. } => assert_eq!(payload, vec![1, 2, 3]),
            RemoteMessage::Heartbeat { .. } => {}
            _ => {}
        }

        task.abort();
        let _ = task.await;
    }
}

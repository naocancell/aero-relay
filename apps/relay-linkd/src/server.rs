use std::collections::BTreeMap;
use std::io;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use relay_core::config::ServerConfig;
use relay_core::model::{
    LinkHealth, LinkSnapshot, PathId, Role, ServerRelaySnapshot, ServerSnapshot, SnapshotPayload,
    SwitchRecord, VIDEO_OWNERSHIP_LEASE_TIMEOUT, VideoSnapshot,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, warn};

use relay_core::path_selector::{PathCandidate, PathSelector};

use crate::ground_station_protocol::{AircraftFrameAssembler, FRAME_LENGTH, build_heartbeat_frame};
use crate::transport::{RemoteCodec, RemoteMessage};
use crate::turntable_protocol::TurntableFrameAssembler;
use crate::video::{self, VideoHealth};
use crate::web_service::ReloadRequest;

const GROUND_STATION_HANDSHAKE: &[u8; 6] = b"dqiang";
const GROUND_STATION_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum RelayOutbound {
    Control { target: PathId, payload: Vec<u8> },
}

#[derive(Clone, Debug)]
struct RelaySender {
    connection_id: u64,
    sender: mpsc::Sender<RelayOutbound>,
    active_path_sender: watch::Sender<Option<String>>,
}

#[derive(Clone, Debug)]
pub struct GroundStationSession {
    pub generation: u64,
    pub sender: mpsc::Sender<Vec<u8>>,
    pub shutdown_tx: watch::Sender<bool>,
}

#[derive(Clone)]
struct RelayRuntime {
    state: Arc<Mutex<ServerState>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    ground_station_sender: Arc<Mutex<Option<GroundStationSession>>>,
    aircraft_framer: Arc<Mutex<AircraftFrameAssembler>>,
    next_connection_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct ServerLinkState {
    pub kind: String,
    pub priority: u32,
    pub health: LinkHealth,
}

#[derive(Debug)]
struct LinkStatusUpdate {
    adapter_ready: bool,
    last_tx_at: Option<u64>,
    tx_bytes: u64,
    tx_rate_bytes_per_second: f64,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RelayState {
    pub relay_id: String,
    pub connected: bool,
    pub heartbeat_at: Option<Instant>,
    pub heartbeat_epoch_ms: Option<u64>,
    pub links: BTreeMap<String, ServerLinkState>,
}

pub struct ServerState {
    heartbeat_timeout: Duration,
    data_timeout: Duration,
    relays: BTreeMap<String, RelayState>,
    active_path: Option<PathId>,
    video_owner_relay_id: Option<String>,
    switches: Vec<SwitchRecord>,
    ground_station_connected: bool,
    ground_station_heartbeat_sequence: u8,
    started_at: Instant,
    recent_error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("server I/O failed: {0}")]
    Io(#[from] io::Error),
}

pub async fn run(
    config: ServerConfig,
    snapshot_tx: watch::Sender<SnapshotPayload>,
    mut shutdown: watch::Receiver<bool>,
    mut reload_rx: mpsc::Receiver<ReloadRequest>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> Result<(), ServerError> {
    let mut current_config = config;
    let mut runtime = start_runtime(current_config.clone(), snapshot_tx.clone()).await?;
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
                    debug!("server received shutdown signal");
                }
                let _ = runtime.shutdown.send(true);
                return join_runtime(runtime.task).await;
            }
            request = reload_rx.recv(), if reload_available => {
                let Some(request) = request else {
                    warn!("local IPC reload channel closed; keeping server runtime alive");
                    reload_available = false;
                    continue;
                };
                let Some(next_config) = request.config.server.clone() else {
                    let _ = request.response.send(Err("配置中缺少 [server] 段".into()));
                    continue;
                };
                let previous_config = current_config.clone();
                let _ = runtime.shutdown.send(true);
                if let Err(error) = join_runtime(runtime.task).await {
                    let message = error.to_string();
                    let _ = request.response.send(Err(message));
                    return Err(error);
                }
                match start_runtime(next_config.clone(), snapshot_tx.clone()).await {
                    Ok(next_runtime) => {
                        current_config = next_config;
                        runtime = next_runtime;
                        let _ = request.response.send(Ok(()));
                    }
                    Err(error) => {
                        let message = error.to_string();
                        match start_runtime(previous_config, snapshot_tx.clone()).await {
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
    task: tokio::task::JoinHandle<Result<(), ServerError>>,
}

async fn start_runtime(
    config: ServerConfig,
    snapshot_tx: watch::Sender<SnapshotPayload>,
) -> Result<RuntimeHandle, ServerError> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_once(config, snapshot_tx, shutdown_rx, Some(ready_tx)));
    match ready_rx.await {
        Ok(()) => Ok(RuntimeHandle {
            shutdown: shutdown_tx,
            task,
        }),
        Err(_) => match task.await {
            Ok(Ok(())) => {
                Err(io::Error::other("server runtime stopped before becoming ready").into())
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(io::Error::other(error.to_string()).into()),
        },
    }
}

async fn join_runtime(
    task: tokio::task::JoinHandle<Result<(), ServerError>>,
) -> Result<(), ServerError> {
    join_runtime_result(task.await)
}

fn join_runtime_result(
    result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    match result {
        Ok(result) => result,
        Err(error) => Err(io::Error::other(error.to_string()).into()),
    }
}

async fn run_once(
    config: ServerConfig,
    snapshot_tx: watch::Sender<SnapshotPayload>,
    mut shutdown: watch::Receiver<bool>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> Result<(), ServerError> {
    let state = Arc::new(Mutex::new(ServerState::new(
        Duration::from_millis(config.heartbeat_timeout_ms),
        Duration::from_millis(config.data_timeout_ms),
    )));
    let relay_senders = Arc::new(Mutex::new(BTreeMap::<String, RelaySender>::new()));
    let next_connection_id = Arc::new(AtomicU64::new(1));
    let ground_station_sender = Arc::new(Mutex::new(None::<GroundStationSession>));
    let next_ground_station_generation = Arc::new(AtomicU64::new(1));
    let aircraft_framer = Arc::new(Mutex::new(AircraftFrameAssembler::default()));
    let listener = TcpListener::bind(&config.listen_addr).await?;
    let ground_station_listener = TcpListener::bind(&config.ground_station_listen_addr).await?;
    let video_health = if config.video.is_some() {
        VideoHealth::configured()
    } else {
        VideoHealth::disabled()
    };
    let (video_shutdown_tx, video_shutdown_rx) = watch::channel(false);
    let mut video_task = config.video.clone().map(|video_config| {
        let video_health = video_health.clone();
        tokio::spawn(async move {
            video::supervise_server_video(video_config, video_health, video_shutdown_rx).await;
        })
    });
    let snapshot_task = tokio::spawn(publish_snapshots(
        state.clone(),
        relay_senders.clone(),
        video_health,
        snapshot_tx,
    ));
    let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
    let relay_runtime = RelayRuntime {
        state: state.clone(),
        relay_senders: relay_senders.clone(),
        ground_station_sender: ground_station_sender.clone(),
        aircraft_framer,
        next_connection_id,
    };
    let mut relay_task = tokio::spawn(accept_relays(
        listener,
        relay_runtime,
        session_shutdown_rx.clone(),
    ));
    let mut ground_station_task = tokio::spawn(accept_ground_stations(
        ground_station_listener,
        state.clone(),
        ground_station_sender,
        relay_senders,
        Duration::from_millis(config.ground_station_heartbeat_interval_ms),
        next_ground_station_generation,
        session_shutdown_rx,
    ));
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    let (result, completed_task) = tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                debug!("server received shutdown signal");
            }
            (Ok(()), None)
        }
        result = &mut relay_task => {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error.to_string()).into()),
            };
            (result, Some("relay"))
        }
        result = &mut ground_station_task => {
            let result = match result {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error.to_string()).into()),
            };
            (result, Some("ground_station"))
        }
    };
    snapshot_task.abort();
    let _ = session_shutdown_tx.send(true);
    let _ = snapshot_task.await;
    if completed_task != Some("relay") {
        let _ = relay_task.await;
    }
    if completed_task != Some("ground_station") {
        let _ = ground_station_task.await;
    }
    let _ = video_shutdown_tx.send(true);
    if let Some(mut task) = video_task.take()
        && time::timeout(Duration::from_secs(5), &mut task)
            .await
            .is_err()
    {
        task.abort();
    }
    result
}

async fn publish_snapshots(
    state: Arc<Mutex<ServerState>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    video_health: VideoHealth,
    snapshot_tx: watch::Sender<SnapshotPayload>,
) {
    let mut ticker = time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut video_ownership = VideoOwnershipCoordinator::default();
    loop {
        ticker.tick().await;
        let now = Instant::now();
        let (mut snapshot, active_path, video_owner_relay_id) = {
            let mut server = state.lock().expect("server state mutex poisoned");
            server.refresh_active_path(now, "periodic freshness check");
            let active_path = server.active_path.clone();
            let video_owner_relay_id = video_ownership
                .select(now, active_path.as_ref().map(|path| path.relay_id.as_str()));
            server.video_owner_relay_id = video_owner_relay_id.clone();
            (server.snapshot(), active_path, video_owner_relay_id)
        };
        snapshot.video = video_health.snapshot();
        let _ = snapshot_tx.send(SnapshotPayload::Server(snapshot));

        let current_senders = relay_senders
            .lock()
            .expect("relay sender mutex poisoned")
            .clone();
        for (relay_id, sender) in &current_senders {
            let link_id = active_path
                .as_ref()
                .filter(|path| {
                    path.relay_id == *relay_id && video_owner_relay_id.as_ref() == Some(relay_id)
                })
                .map(|path| path.link_id.clone());
            let _ = sender.active_path_sender.send(link_id);
        }
    }
}

#[derive(Default)]
struct VideoOwnershipCoordinator {
    granted_relay_id: Option<String>,
    grant_not_before: Option<Instant>,
}

impl VideoOwnershipCoordinator {
    fn select(&mut self, now: Instant, selected_relay_id: Option<&str>) -> Option<String> {
        if self.granted_relay_id.as_deref() == selected_relay_id {
            return self.granted_relay_id.clone();
        }
        if self.granted_relay_id.take().is_some() {
            self.grant_not_before = now.checked_add(VIDEO_OWNERSHIP_LEASE_TIMEOUT);
        }
        let selected_relay_id = selected_relay_id?;
        if self.grant_not_before.is_some_and(|deadline| now < deadline) {
            return None;
        }
        self.grant_not_before = None;
        self.granted_relay_id = Some(selected_relay_id.to_owned());
        self.granted_relay_id.clone()
    }
}

fn configure_accepted_stream(stream: &TcpStream, role: &str) -> io::Result<()> {
    if let Err(error) = stream.set_nodelay(true) {
        warn!(error = %error, "failed to set TCP_NODELAY on {role} socket");
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SocketRole {
    Relay,
    GroundStation,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct SocketInspection {
    pub peer_addr: std::net::SocketAddr,
    pub role: SocketRole,
    pub nodelay: bool,
}

#[cfg(test)]
static TEST_SOCKET_PROBE: std::sync::RwLock<Option<tokio::sync::mpsc::UnboundedSender<SocketInspection>>> =
    std::sync::RwLock::new(None);

#[cfg(test)]
pub(crate) fn register_socket_probe(sender: tokio::sync::mpsc::UnboundedSender<SocketInspection>) {
    *TEST_SOCKET_PROBE.write().unwrap() = Some(sender);
}

#[cfg(test)]
pub(crate) fn unregister_socket_probe() {
    *TEST_SOCKET_PROBE.write().unwrap() = None;
}

#[cfg(test)]
fn notify_socket_probe(peer_addr: std::net::SocketAddr, role: SocketRole, stream: &TcpStream) {
    if let Some(probe) = TEST_SOCKET_PROBE.read().unwrap().as_ref() {
        let nodelay = stream.nodelay().unwrap_or(false);
        let _ = probe.send(SocketInspection {
            peer_addr,
            role,
            nodelay,
        });
    }
}

async fn accept_relays(
    listener: TcpListener,
    runtime: RelayRuntime,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let mut sessions = JoinSet::new();
    let result = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let _ = configure_accepted_stream(&stream, "relay");
                    #[cfg(test)]
                    notify_socket_probe(peer, SocketRole::Relay, &stream);
                    let runtime = runtime.clone();
                    sessions.spawn(async move {
                        if let Err(error) = handle_relay(stream, runtime).await {
                            warn!(%peer, error = %error, "relay session ended");
                        }
                    });
                }
                Err(error) => warn!(error = %error, "failed to accept relay session"),
            },
            result = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(Err(error)) = result {
                    warn!(error = %error, "relay session task stopped unexpectedly");
                }
            }
        }
    };
    sessions.shutdown().await;
    result
}

async fn handle_relay(stream: TcpStream, runtime: RelayRuntime) -> Result<(), io::Error> {
    let RelayRuntime {
        state,
        relay_senders,
        ground_station_sender,
        aircraft_framer,
        next_connection_id,
    } = runtime;
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, RemoteCodec::new());
    let mut writer = FramedWrite::new(write_half, RemoteCodec::new());
    let Some(first) = reader.next().await else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "relay closed before hello",
        ));
    };
    let relay_id = match first? {
        RemoteMessage::RelayHello { relay_id } if !relay_id.trim().is_empty() => relay_id,
        _ => {
            writer
                .send(RemoteMessage::Rejected {
                    reason: "first message must be RelayHello".into(),
                })
                .await?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid relay hello",
            ));
        }
    };
    let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
    writer
        .send(RemoteMessage::HelloAck {
            server_id: "relay-link-server".into(),
        })
        .await?;
    {
        let now = Instant::now();
        let mut server = state.lock().expect("server state mutex poisoned");
        server.register_relay(&relay_id, now);
    }
    let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
    let (active_path_tx, mut active_path_rx) = watch::channel(None);
    let (proxy_req_tx, mut proxy_req_rx) = mpsc::channel(64);
    crate::http_proxy::global_proxy_hub().register_relay(
        relay_id.clone(),
        connection_id,
        proxy_req_tx,
    );
    relay_senders
        .lock()
        .expect("relay sender mutex poisoned")
        .insert(
            relay_id.clone(),
            RelaySender {
                connection_id,
                sender: outbound_tx,
                active_path_sender: active_path_tx,
            },
        );

    let writer_task = async {
        loop {
            tokio::select! {
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else { break; };
                    if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                        return Ok(());
                    }
                    match outbound {
                        RelayOutbound::Control { target, payload } => {
                            let current_target = {
                                let mut server = state.lock().expect("server state mutex poisoned");
                                server.refresh_active_path(Instant::now(), "control send freshness check")
                            };
                            if !target.link_id.is_empty() && current_target.as_ref() != Some(&target) {
                                continue;
                            }
                            writer.send(RemoteMessage::ControlData {
                                relay_id: target.relay_id,
                                link_id: target.link_id,
                                payload,
                            }).await?;
                        }
                    }
                }
                Some(proxy_req) = proxy_req_rx.recv() => {
                    if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                        return Ok(());
                    }
                    writer.send(RemoteMessage::HttpProxyRequest {
                        request_id: proxy_req.request_id,
                        target: proxy_req.target,
                        method: proxy_req.method,
                        uri: proxy_req.uri,
                        headers: proxy_req.headers,
                        body: proxy_req.body,
                    }).await?;
                }
                changed = active_path_rx.changed() => {
                    if changed.is_err() { break; }
                    if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                        return Ok(());
                    }
                    let link_id = active_path_rx.borrow_and_update().clone();
                    let expected_link_id = state
                        .lock()
                        .expect("server state mutex poisoned")
                        .video_active_link_for(&relay_id);
                    if expected_link_id != link_id {
                        continue;
                    }
                    writer.send(RemoteMessage::ActivePath {
                        relay_id: relay_id.clone(),
                        link_id,
                    }).await?;
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };

    let reader_task = async {
        loop {
            let Some(inbound) = reader.next().await else {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "relay closed session"));
            };
            if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                return Ok(());
            }
            match inbound? {
                RemoteMessage::LinkAnnounce { relay_id: announced, link_id, kind, priority } if announced == relay_id => {
                    state.lock().expect("server state mutex poisoned").register_link_with_priority(&relay_id, &link_id, kind, priority);
                }
                RemoteMessage::Heartbeat { relay_id: announced } if announced == relay_id => {
                    let now = Instant::now();
                    let mut server = state.lock().expect("server state mutex poisoned");
                    server.heartbeat(&relay_id, now);
                    server.refresh_active_path(now, "heartbeat");
                }
                RemoteMessage::LinkStatus {
                    relay_id: announced,
                    link_id,
                    adapter_ready,
                    last_tx_at,
                    tx_bytes,
                    tx_rate_bytes_per_second,
                    last_error,
                } if announced == relay_id => {
                    let now = Instant::now();
                    let mut server = state.lock().expect("server state mutex poisoned");
                    server.update_link_status(
                        &relay_id,
                        &link_id,
                        LinkStatusUpdate {
                            adapter_ready,
                            last_tx_at,
                            tx_bytes,
                            tx_rate_bytes_per_second,
                            last_error,
                        },
                    );
                    server.refresh_active_path(now, "link status");
                }
                RemoteMessage::AircraftData { relay_id: announced, link_id, payload } if announced == relay_id => {
                    let path = PathId::new(&relay_id, &link_id);
                    let (frames, is_corrupted) = aircraft_framer
                        .lock()
                        .expect("aircraft framer mutex poisoned")
                        .push_inspect(&path, &payload);
                    let now = Instant::now();
                    let mut server = state.lock().expect("server state mutex poisoned");
                    let active = if !is_corrupted {
                        server.record_aircraft_data(&relay_id, &link_id, now, payload.len())
                    } else {
                        debug!(path = %path, "dropping corrupted/noise aircraft data without updating link health");
                        server.active_path.as_ref().map(|p| p == &path)
                    };
                    drop(server);
                    if active == Some(true) {
                        let session = ground_station_sender.lock().expect("ground station sender mutex poisoned").clone();
                        if let Some(session) = session {
                            for outbound in frames {
                                if let Err(error) = session.sender.try_send(outbound.to_vec()) {
                                    warn!(error = %error, "dropping framed aircraft data because ground station queue is full");
                                    break;
                                }
                            }
                        }
                    }
                }
                RemoteMessage::HttpProxyResponse {
                    request_id,
                    status,
                    headers,
                    body,
                } => {
                    crate::http_proxy::global_proxy_hub().complete_request(
                        request_id,
                        crate::http_proxy::ProxyResponsePayload {
                            status,
                            headers,
                            body,
                        },
                    );
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

    let result = tokio::select! {
        res = writer_task => res,
        res = reader_task => res,
    };
    crate::http_proxy::global_proxy_hub().unregister_relay(&relay_id, connection_id);
    let removed_current = {
        let mut senders = relay_senders.lock().expect("relay sender mutex poisoned");
        if senders
            .get(&relay_id)
            .is_some_and(|sender| sender.connection_id == connection_id)
        {
            senders.remove(&relay_id);
            true
        } else {
            false
        }
    };
    if removed_current {
        let now = Instant::now();
        let mut server = state.lock().expect("server state mutex poisoned");
        server.disconnect_relay(&relay_id);
        server.refresh_active_path(now, "relay disconnected");
        if let Err(error) = &result {
            server.record_error(error);
        }
    }
    result
}

fn is_current_connection(
    relay_senders: &Arc<Mutex<BTreeMap<String, RelaySender>>>,
    relay_id: &str,
    connection_id: u64,
) -> bool {
    relay_senders
        .lock()
        .expect("relay sender mutex poisoned")
        .get(relay_id)
        .is_some_and(|sender| sender.connection_id == connection_id)
}

async fn accept_ground_stations(
    listener: TcpListener,
    state: Arc<Mutex<ServerState>>,
    sender_slot: Arc<Mutex<Option<GroundStationSession>>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    heartbeat_interval: Duration,
    next_generation: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let mut sessions = JoinSet::new();
    let result = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error.into()),
                };
                let _ = configure_accepted_stream(&stream, "ground station");
                #[cfg(test)]
                notify_socket_probe(peer, SocketRole::GroundStation, &stream);
                let state = state.clone();
                let sender_slot = sender_slot.clone();
                let relay_senders = relay_senders.clone();
                let next_generation = next_generation.clone();
                sessions.spawn(async move {
                    if let Err(error) = handle_ground_station(
                        stream,
                        state.clone(),
                        sender_slot,
                        relay_senders,
                        heartbeat_interval,
                        next_generation,
                    )
                    .await
                    {
                        state
                            .lock()
                            .expect("server state mutex poisoned")
                            .record_error(&error);
                        warn!(%peer, error = %error, "ground station session ended");
                    }
                });
            }
            result = sessions.join_next(), if !sessions.is_empty() => {
                if let Some(Err(error)) = result {
                    warn!(error = %error, "ground station session task stopped unexpectedly");
                }
            }
        }
    };
    sessions.shutdown().await;
    result
}

async fn handle_ground_station(
    stream: TcpStream,
    state: Arc<Mutex<ServerState>>,
    sender_slot: Arc<Mutex<Option<GroundStationSession>>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    heartbeat_interval: Duration,
    next_generation: Arc<AtomicU64>,
) -> Result<(), io::Error> {
    let (mut reader, mut writer) = stream.into_split();
    let handshake_res = time::timeout(GROUND_STATION_HANDSHAKE_TIMEOUT, async {
        let mut buf = [0u8; 1024];
        let mut accumulated = Vec::new();
        let mut packet_count = 0;
        while packet_count < 5 {
            let n = match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) => return Err(error),
            };
            packet_count += 1;
            accumulated.extend_from_slice(&buf[..n]);
            if let Some(pos) = accumulated
                .windows(GROUND_STATION_HANDSHAKE.len())
                .position(|w| w == GROUND_STATION_HANDSHAKE)
            {
                let remaining = accumulated[pos + GROUND_STATION_HANDSHAKE.len()..].to_vec();
                return Ok(remaining);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid ground station handshake",
        ))
    })
    .await;

    let initial_payload = match handshake_res {
        Ok(Ok(remaining)) => remaining,
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ground station handshake timed out",
            ));
        }
    };

    let my_generation = next_generation.fetch_add(1, Ordering::Relaxed);
    let (sender, mut outbound_rx) = mpsc::channel(128);
    let (session_shutdown_tx, mut session_shutdown_rx) = watch::channel(false);

    {
        let mut sender_slot = sender_slot
            .lock()
            .expect("ground station sender mutex poisoned");
        if let Some(old_session) = sender_slot.take() {
            tracing::info!(
                stale_generation = old_session.generation,
                new_generation = my_generation,
                "新地面站接入，平滑顶替并通知废弃旧半开会话"
            );
            let _ = old_session.shutdown_tx.send(true);
        }
        *sender_slot = Some(GroundStationSession {
            generation: my_generation,
            sender,
            shutdown_tx: session_shutdown_tx,
        });
        state
            .lock()
            .expect("server state mutex poisoned")
            .set_ground_station_connected(true);
    }

    // 读写解耦：Reader 协程独立负责上行控制指令接收与分发
    let state_for_reader = state.clone();
    let relay_senders_for_reader = relay_senders.clone();
    let reader_task = async move {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut turntable_assembler = TurntableFrameAssembler::new();
        if !initial_payload.is_empty() {
            route_control(
                &state_for_reader,
                &relay_senders_for_reader,
                &mut turntable_assembler,
                &initial_payload,
            );
        }
        loop {
            let length = match reader.read(&mut buffer).await {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "ground station closed",
                    ));
                }
                Ok(length) => length,
                Err(error) => return Err(error),
            };
            route_control(
                &state_for_reader,
                &relay_senders_for_reader,
                &mut turntable_assembler,
                &buffer[..length],
            );
        }
    };

    // Writer 协程独立负责下行遥测与周期心跳发送，下行背压绝不影响 Reader 协程
    let state_for_writer = state.clone();
    let writer_task = async {
        let mut heartbeat = time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else {
                        return Ok(());
                    };
                    writer.write_all(&outbound).await?;
                }
                _ = heartbeat.tick() => {
                    writer.write_all(&next_heartbeat_frame(&state_for_writer)).await?;
                }
            }
        }
    };

    let session = tokio::select! {
        res = reader_task => res,
        res = writer_task => res,
        changed = session_shutdown_rx.changed() => {
            if changed.is_ok() && *session_shutdown_rx.borrow() {
                tracing::debug!(my_generation, "地面站会话已被更新代际顶替，平稳退出本任务");
            }
            Ok(())
        }
    };

    finish_ground_station_session(&state, &sender_slot, my_generation);
    session
}

fn finish_ground_station_session(
    state: &Arc<Mutex<ServerState>>,
    sender_slot: &Arc<Mutex<Option<GroundStationSession>>>,
    my_generation: u64,
) {
    let mut sender_slot = sender_slot
        .lock()
        .expect("ground station sender mutex poisoned");
    if sender_slot
        .as_ref()
        .is_some_and(|session| session.generation == my_generation)
    {
        *sender_slot = None;
        state
            .lock()
            .expect("server state mutex poisoned")
            .set_ground_station_connected(false);
    } else {
        tracing::debug!(
            my_generation,
            "旧地面站会话退出，当前存在更新代际，保留活跃会话"
        );
    }
}

fn next_heartbeat_frame(state: &Arc<Mutex<ServerState>>) -> [u8; FRAME_LENGTH] {
    let (sequence, active_path) = {
        let mut server = state.lock().expect("server state mutex poisoned");
        server.refresh_active_path(Instant::now(), "ground station heartbeat");
        (
            server.next_ground_station_heartbeat_sequence(),
            server.control_target(),
        )
    };
    match build_heartbeat_frame(sequence, active_path.as_ref()) {
        Ok(frame) => frame,
        Err(error) => {
            warn!(error = %error, "active path does not fit in the heartbeat frame");
            build_heartbeat_frame(sequence, None).expect("empty heartbeat path always fits")
        }
    }
}

#[cfg(test)]
fn frame_aircraft_payload(
    aircraft_framer: &Arc<Mutex<AircraftFrameAssembler>>,
    path: &PathId,
    payload: &[u8],
) -> Vec<Vec<u8>> {
    let frames = aircraft_framer
        .lock()
        .expect("aircraft framer mutex poisoned")
        .push(path, payload);
    frames.into_iter().map(|frame| frame.to_vec()).collect()
}

fn route_control(
    state: &Arc<Mutex<ServerState>>,
    relay_senders: &Arc<Mutex<BTreeMap<String, RelaySender>>>,
    turntable_assembler: &mut TurntableFrameAssembler,
    payload: &[u8],
) {
    let (turntable_frames, passthrough) = turntable_assembler.process(payload);
    let senders = relay_senders
        .lock()
        .expect("relay sender mutex poisoned")
        .clone();

    // 1. 转台控制帧广播给所有已连接的中继，确保主备链路切换时定向天线转台不丢目标
    for frame in turntable_frames {
        for (relay_id, entry) in &senders {
            let _ = entry.sender.try_send(RelayOutbound::Control {
                target: PathId::new(relay_id, ""),
                payload: frame.to_vec(),
            });
        }
    }

    // 2. 纯机载飞控透传数据仅发往当前活跃链路的目标中继
    if !passthrough.is_empty() {
        let target = {
            let mut server = state.lock().expect("server state mutex poisoned");
            server.refresh_active_path(Instant::now(), "control freshness check")
        };
        let Some(target) = target else {
            warn!("dropping ground-station control data because no active path is available");
            return;
        };
        if let Some(entry) = senders.get(&target.relay_id) {
            if let Err(error) = entry.sender.try_send(RelayOutbound::Control {
                target,
                payload: passthrough,
            }) {
                warn!(error = %error, "dropping control data because relay queue is full");
            }
        } else {
            debug!(path = %target, bytes = passthrough.len(), "dropping control data because active relay is disconnected");
        }
    }
}

impl ServerState {
    pub fn new(heartbeat_timeout: Duration, data_timeout: Duration) -> Self {
        Self {
            heartbeat_timeout,
            data_timeout,
            relays: BTreeMap::new(),
            active_path: None,
            video_owner_relay_id: None,
            switches: Vec::new(),
            ground_station_connected: false,
            ground_station_heartbeat_sequence: 0,
            started_at: Instant::now(),
            recent_error: None,
        }
    }

    pub fn register_relay(&mut self, relay_id: impl Into<String>, now: Instant) {
        let relay_id = relay_id.into();
        self.relays
            .entry(relay_id.clone())
            .and_modify(|relay| {
                relay.connected = true;
                relay.heartbeat_at = Some(now);
                relay.heartbeat_epoch_ms = Some(unix_timestamp_ms_u64());
                for link in relay.links.values_mut() {
                    link.health = LinkHealth::default();
                }
            })
            .or_insert_with(|| RelayState {
                relay_id,
                connected: true,
                heartbeat_at: Some(now),
                heartbeat_epoch_ms: Some(unix_timestamp_ms_u64()),
                links: BTreeMap::new(),
            });
    }

    pub fn register_link(&mut self, relay_id: &str, link_id: &str, kind: impl Into<String>) {
        self.register_link_with_priority(relay_id, link_id, kind, 0);
    }

    pub fn register_link_with_priority(
        &mut self,
        relay_id: &str,
        link_id: &str,
        kind: impl Into<String>,
        priority: u32,
    ) {
        let relay = self
            .relays
            .entry(relay_id.to_owned())
            .or_insert_with(|| RelayState {
                relay_id: relay_id.to_owned(),
                connected: false,
                heartbeat_at: None,
                heartbeat_epoch_ms: None,
                links: BTreeMap::new(),
            });
        relay
            .links
            .entry(link_id.to_owned())
            .and_modify(|link| {
                link.priority = priority;
            })
            .or_insert(ServerLinkState {
                kind: kind.into(),
                priority,
                health: LinkHealth::default(),
            });
    }

    pub fn set_relay_status(&mut self, relay_id: &str, connected: bool) {
        if let Some(relay) = self.relays.get_mut(relay_id) {
            relay.connected = connected;
            if !connected {
                relay.heartbeat_at = None;
                for link in relay.links.values_mut() {
                    link.health.adapter_ready = false;
                }
            }
        }
    }

    pub fn disconnect_relay(&mut self, relay_id: &str) {
        self.set_relay_status(relay_id, false);
    }

    pub fn heartbeat(&mut self, relay_id: &str, now: Instant) {
        if let Some(relay) = self.relays.get_mut(relay_id) {
            relay.heartbeat_at = Some(now);
            relay.heartbeat_epoch_ms = Some(unix_timestamp_ms_u64());
            relay.connected = true;
        }
    }

    pub fn set_link_health(&mut self, relay_id: &str, link_id: &str, health: LinkHealth) {
        self.register_link(relay_id, link_id, "unknown");
        if let Some(relay) = self.relays.get_mut(relay_id)
            && let Some(link) = relay.links.get_mut(link_id)
        {
            link.health = health;
        }
    }

    fn update_link_status(&mut self, relay_id: &str, link_id: &str, status: LinkStatusUpdate) {
        if let Some(relay) = self.relays.get_mut(relay_id)
            && let Some(link) = relay.links.get_mut(link_id)
        {
            link.health.adapter_ready = status.adapter_ready;
            link.health.last_tx_at_epoch_ms = status.last_tx_at;
            link.health.tx_bytes = status.tx_bytes;
            link.health.tx_rate_bytes_per_second = status.tx_rate_bytes_per_second;
            link.health.last_error = status.last_error;
        }
    }

    pub fn record_aircraft_data(
        &mut self,
        relay_id: &str,
        link_id: &str,
        now: Instant,
        payload_len: usize,
    ) -> Option<bool> {
        let relay = self.relays.get_mut(relay_id)?;
        let link = relay.links.get_mut(link_id)?;
        link.health.adapter_ready = true;
        link.health.mark_rx(now, payload_len);
        Some(
            self.refresh_active_path(now, "aircraft data")
                .is_some_and(|path| path == PathId::new(relay_id, link_id)),
        )
    }

    pub fn refresh_active_path(&mut self, now: Instant, reason: &str) -> Option<PathId> {
        let candidates = self
            .relays
            .values()
            .flat_map(|relay| {
                relay.links.iter().map(|(link_id, link)| PathCandidate {
                    path: PathId::new(&relay.relay_id, link_id),
                    connected: relay.connected,
                    heartbeat_at: relay.heartbeat_at,
                    link: link.health.clone(),
                    priority: link.priority,
                })
            })
            .collect::<Vec<_>>();
        let selected = PathSelector::choose_with_timeouts(
            now,
            self.active_path.as_ref(),
            &candidates,
            self.heartbeat_timeout,
            self.data_timeout,
        );
        if selected != self.active_path {
            self.switches.push(SwitchRecord {
                at: unix_timestamp_ms(),
                from: self.active_path.clone(),
                to: selected.clone(),
                reason: reason.into(),
            });
            if self.switches.len() > 32 {
                self.switches.remove(0);
            }
            self.active_path = selected.clone();
        }
        selected
    }

    pub fn set_ground_station_connected(&mut self, connected: bool) {
        self.ground_station_connected = connected;
    }

    pub fn control_target(&self) -> Option<PathId> {
        self.active_path.clone()
    }

    fn video_active_link_for(&self, relay_id: &str) -> Option<String> {
        self.active_path
            .as_ref()
            .filter(|path| {
                path.relay_id == relay_id && self.video_owner_relay_id.as_deref() == Some(relay_id)
            })
            .map(|path| path.link_id.clone())
    }

    fn next_ground_station_heartbeat_sequence(&mut self) -> u8 {
        let sequence = self.ground_station_heartbeat_sequence;
        self.ground_station_heartbeat_sequence = sequence.wrapping_add(1);
        sequence
    }

    pub fn record_error(&mut self, error: &dyn std::fmt::Display) {
        self.recent_error = Some(error.to_string());
    }

    pub fn snapshot(&self) -> ServerSnapshot {
        ServerSnapshot {
            role: Role::Server,
            ground_station_connected: self.ground_station_connected,
            active_path: self.active_path.clone(),
            relays: self
                .relays
                .values()
                .map(|relay| ServerRelaySnapshot {
                    relay_id: relay.relay_id.clone(),
                    connected: relay.connected,
                    last_heartbeat_at: relay.heartbeat_epoch_ms.map(|value| value.to_string()),
                    links: relay
                        .links
                        .iter()
                        .map(|(link_id, link)| LinkSnapshot {
                            path: PathId::new(&relay.relay_id, link_id),
                            kind: link.kind.clone(),
                            adapter_ready: link.health.adapter_ready,
                            last_rx_at: link
                                .health
                                .last_rx_at_epoch_ms
                                .map(|value| value.to_string()),
                            last_tx_at: link
                                .health
                                .last_tx_at_epoch_ms
                                .map(|value| value.to_string()),
                            rx_bytes: link.health.rx_bytes,
                            tx_bytes: link.health.tx_bytes,
                            rx_rate_bytes_per_second: link.health.rx_rate_bytes_per_second,
                            tx_rate_bytes_per_second: link.health.tx_rate_bytes_per_second,
                            last_error: link.health.last_error.clone(),
                            active: self.active_path.as_ref()
                                == Some(&PathId::new(&relay.relay_id, link_id)),
                        })
                        .collect(),
                })
                .collect(),
            switches: self.switches.clone(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            recent_error: self.recent_error.clone(),
            video: VideoSnapshot::default(),
        }
    }
}

fn unix_timestamp_ms() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|_| "0".into())
}

fn unix_timestamp_ms_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::config::Config;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::Framed;

    fn available_loopback_addrs() -> (String, String) {
        let relay_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback test address");
        let ground_station_listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("reserve second loopback test address");
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

    #[cfg(windows)]
    fn abortive_close(stream: std::net::TcpStream) {
        use std::mem::size_of;
        use std::os::windows::io::AsRawSocket;

        #[repr(C)]
        struct Linger {
            onoff: u16,
            linger: u16,
        }

        unsafe extern "system" {
            fn setsockopt(
                socket: u64,
                level: i32,
                option: i32,
                value: *const i8,
                option_length: i32,
            ) -> i32;
        }

        let linger = Linger {
            onoff: 1,
            linger: 0,
        };
        let result = unsafe {
            setsockopt(
                stream.as_raw_socket(),
                0xffff,
                0x0080,
                (&linger as *const Linger).cast(),
                size_of::<Linger>() as i32,
            )
        };
        assert_eq!(result, 0, "setsockopt(SO_LINGER) failed");
        drop(stream);
    }

    #[tokio::test]
    async fn ground_station_outbound_data_is_raw_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let sender_slot_for_task = sender_slot.clone();
        let next_generation = Arc::new(AtomicU64::new(1));
        let next_generation_for_task = next_generation.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state,
                sender_slot_for_task,
                relay_senders,
                Duration::from_secs(60),
                next_generation_for_task,
            )
            .await
        });
        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");
        client
            .write_all(b"dqiang")
            .await
            .expect("ground station handshake");
        let sender = time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(session) = sender_slot.lock().expect("sender mutex").clone() {
                    break session.sender;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ground station sender should be installed");

        let mut heartbeat = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        time::timeout(Duration::from_secs(1), client.read_exact(&mut heartbeat))
            .await
            .expect("initial heartbeat should arrive")
            .expect("read initial heartbeat");
        assert!(crate::ground_station_protocol::is_valid_frame(&heartbeat));

        let expected = vec![0x00, 0x01, 0xA5, 0xFF];
        sender
            .send(expected.clone())
            .await
            .expect("queue ground station data");

        let mut received = vec![0u8; expected.len()];
        time::timeout(Duration::from_secs(1), client.read_exact(&mut received))
            .await
            .expect("ground station should receive bytes")
            .expect("read raw ground station bytes");
        assert_eq!(received, expected);

        task.abort();
    }

    #[tokio::test]
    async fn ground_station_without_handshake_is_closed() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let next_generation = Arc::new(AtomicU64::new(1));
        let task = tokio::spawn({
            let state = state.clone();
            let sender_slot = sender_slot.clone();
            let relay_senders = relay_senders.clone();
            let next_generation = next_generation.clone();
            async move {
                let (stream, _) = listener.accept().await.expect("accept ground station");
                handle_ground_station(
                    stream,
                    state,
                    sender_slot,
                    relay_senders,
                    Duration::from_secs(60),
                    next_generation,
                )
                .await
            }
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");
        let mut byte = [0u8; 1];
        let bytes_read = time::timeout(Duration::from_secs(2), client.read(&mut byte))
            .await
            .expect("missing handshake should close client")
            .expect("read client close");
        task.abort();

        assert_eq!(bytes_read, 0);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn ground_station_read_error_clears_connection_state() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let state_for_task = state.clone();
        let sender_slot_for_task = sender_slot.clone();
        let next_generation = Arc::new(AtomicU64::new(1));
        let next_generation_for_task = next_generation.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders,
                Duration::from_secs(60),
                next_generation_for_task,
            )
            .await
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");
        client
            .write_all(b"dqiang")
            .await
            .expect("ground station handshake");
        time::timeout(Duration::from_secs(1), async {
            loop {
                if sender_slot.lock().expect("sender mutex").is_some() {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ground station sender should be installed");

        abortive_close(client.into_std().expect("convert client socket"));

        let result = time::timeout(Duration::from_secs(1), task)
            .await
            .expect("session should end after reset")
            .expect("session task should not panic");
        assert!(result.is_err(), "reset should be reported as an I/O error");
        assert!(sender_slot.lock().expect("sender mutex").is_none());
        assert!(
            !state
                .lock()
                .expect("server state mutex")
                .ground_station_connected
        );
    }

    #[tokio::test]
    async fn ground_station_handshake_is_not_blocked_by_another_client() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let next_generation = Arc::new(AtomicU64::new(1));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_task = tokio::spawn(accept_ground_stations(
            listener,
            state,
            sender_slot,
            relay_senders,
            Duration::from_secs(60),
            next_generation,
            shutdown_rx,
        ));

        let mut silent_client = TcpStream::connect(&address)
            .await
            .expect("silent client should connect");
        time::sleep(Duration::from_millis(50)).await;

        let mut valid_client = TcpStream::connect(&address)
            .await
            .expect("valid client should connect");
        valid_client
            .write_all(b"dqiang")
            .await
            .expect("ground station handshake");
        let mut heartbeat = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        time::timeout(
            Duration::from_millis(800),
            valid_client.read_exact(&mut heartbeat),
        )
        .await
        .expect("valid client should not wait for silent client")
        .expect("read valid client heartbeat");
        assert!(crate::ground_station_protocol::is_valid_frame(&heartbeat));

        silent_client.shutdown().await.expect("close silent client");
        valid_client.shutdown().await.expect("close valid client");
        server_task.abort();
    }

    #[tokio::test]
    async fn ground_station_handshake_in_later_packet_passes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let next_generation = Arc::new(AtomicU64::new(1));
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state,
                sender_slot,
                relay_senders,
                Duration::from_secs(60),
                next_generation,
            )
            .await
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");

        // 包 1: 无关数据
        client.write_all(b"junk1").await.expect("write packet 1");
        time::sleep(Duration::from_millis(10)).await;
        // 包 2: 无关数据
        client.write_all(b"junk2").await.expect("write packet 2");
        time::sleep(Duration::from_millis(10)).await;
        // 包 3: 包含 dqiang 握手
        client
            .write_all(b"hello_dqiang_world")
            .await
            .expect("write packet 3");

        let mut heartbeat = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        time::timeout(Duration::from_secs(1), client.read_exact(&mut heartbeat))
            .await
            .expect("initial heartbeat should arrive")
            .expect("read initial heartbeat");
        assert!(crate::ground_station_protocol::is_valid_frame(&heartbeat));

        task.abort();
    }

    #[tokio::test]
    async fn ground_station_handshake_exceeds_5_packets_fails() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let next_generation = Arc::new(AtomicU64::new(1));
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state,
                sender_slot,
                relay_senders,
                Duration::from_secs(60),
                next_generation,
            )
            .await
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");

        // 发送 5 包均不含 dqiang
        for i in 1..=5 {
            client
                .write_all(format!("junk{}", i).as_bytes())
                .await
                .expect("write junk packet");
            time::sleep(Duration::from_millis(10)).await;
        }

        let mut byte = [0u8; 1];
        let bytes_read = time::timeout(Duration::from_secs(1), client.read(&mut byte))
            .await
            .expect("handshake failure should close connection")
            .expect("read client close");

        assert_eq!(bytes_read, 0);
        task.abort();
    }

    #[test]
    fn finishing_ground_station_session_clears_slot_and_state() {
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let (tx, _rx) = mpsc::channel(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let session = GroundStationSession {
            generation: 1,
            sender: tx,
            shutdown_tx,
        };
        let sender_slot = Arc::new(Mutex::new(Some(session)));
        state
            .lock()
            .expect("server state mutex")
            .set_ground_station_connected(true);

        // 如果代际不匹配，不应清空槽位与连接状态
        finish_ground_station_session(&state, &sender_slot, 999);
        assert!(
            sender_slot
                .lock()
                .expect("ground station sender mutex")
                .is_some()
        );
        assert!(
            state
                .lock()
                .expect("server state mutex")
                .ground_station_connected
        );

        // 代际匹配时，正确清空槽位与连接状态
        finish_ground_station_session(&state, &sender_slot, 1);

        assert!(
            sender_slot
                .lock()
                .expect("ground station sender mutex")
                .is_none()
        );
        assert!(
            !state
                .lock()
                .expect("server state mutex")
                .ground_station_connected
        );
    }

    #[tokio::test]
    async fn server_periodically_renews_active_path_ownership() {
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("server state mutex");
            server.register_relay("relay-a", now);
            server.register_link("relay-a", "serial-1", "serial");
            server.set_relay_status("relay-a", true);
            server.set_link_health(
                "relay-a",
                "serial-1",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "video ownership test"),
                Some(PathId::new("relay-a", "serial-1"))
            );
        }

        let (relay_tx, _relay_rx) = mpsc::channel(4);
        let (active_path_tx, mut active_path_rx) = watch::channel(None);
        let relay_senders = Arc::new(Mutex::new(BTreeMap::from([(
            "relay-a".to_string(),
            RelaySender {
                connection_id: 1,
                sender: relay_tx,
                active_path_sender: active_path_tx,
            },
        )])));
        let (snapshot_tx, _snapshot_rx) =
            watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
        let task = tokio::spawn(publish_snapshots(
            state,
            relay_senders,
            VideoHealth::disabled(),
            snapshot_tx,
        ));

        time::timeout(Duration::from_secs(1), async {
            loop {
                if active_path_rx.borrow().as_deref() == Some("serial-1") {
                    break;
                }
                active_path_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("active path should be updated to serial-1");

        task.abort();
    }

    #[test]
    fn cross_relay_video_handoff_waits_for_the_previous_lease() {
        let mut coordinator = VideoOwnershipCoordinator::default();
        let now = Instant::now();

        assert_eq!(
            coordinator.select(now, Some("relay-a")),
            Some("relay-a".to_string())
        );
        assert_eq!(coordinator.select(now, Some("relay-b")), None);
        assert_eq!(
            coordinator.select(
                now + VIDEO_OWNERSHIP_LEASE_TIMEOUT - Duration::from_millis(1),
                Some("relay-b")
            ),
            None
        );
        assert_eq!(
            coordinator.select(now + VIDEO_OWNERSHIP_LEASE_TIMEOUT, Some("relay-b")),
            Some("relay-b".to_string())
        );
    }

    #[tokio::test]
    async fn server_hot_reload_closes_sessions_owned_by_previous_runtime() {
        let (relay_addr, ground_station_addr) = available_loopback_addrs();
        let config = ServerConfig {
            id: "server-a".into(),
            listen_addr: relay_addr.clone(),
            ground_station_listen_addr: ground_station_addr.clone(),
            ground_station_heartbeat_interval_ms: 20,
            heartbeat_timeout_ms: 100,
            data_timeout_ms: 100,
            video: None,
            auth: None,
        };
        let (snapshot_tx, mut snapshot_rx) =
            watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
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
            .expect("server runtime should become ready")
            .expect("server ready signal");

        let mut relay = Framed::new(
            TcpStream::connect(&relay_addr)
                .await
                .expect("connect relay session"),
            RemoteCodec::new(),
        );
        relay
            .send(RemoteMessage::RelayHello {
                relay_id: "relay-a".into(),
            })
            .await
            .expect("send relay hello");
        assert!(matches!(
            relay.next().await,
            Some(Ok(RemoteMessage::HelloAck { .. }))
        ));

        let mut ground_station = TcpStream::connect(&ground_station_addr)
            .await
            .expect("connect ground station session");
        ground_station
            .write_all(GROUND_STATION_HANDSHAKE)
            .await
            .expect("send ground station handshake");
        let mut heartbeat = [0u8; FRAME_LENGTH];
        ground_station
            .read_exact(&mut heartbeat)
            .await
            .expect("read initial ground station heartbeat");

        time::timeout(Duration::from_secs(1), async {
            loop {
                let connected = {
                    let snapshot = snapshot_rx.borrow_and_update();
                    matches!(
                        &*snapshot,
                        SnapshotPayload::Server(snapshot)
                            if snapshot.ground_station_connected
                                && snapshot.relays.iter().any(|relay| relay.connected)
                    )
                };
                if connected {
                    break;
                }
                snapshot_rx
                    .changed()
                    .await
                    .expect("server snapshot channel");
            }
        })
        .await
        .expect("initial sessions should appear in the server snapshot");

        let mut relay_closed = Box::pin(async move {
            while let Some(message) = relay.next().await {
                message.expect("read old relay session while waiting for close");
            }
        });
        let mut ground_station_closed = Box::pin(async move {
            let mut buffer = [0u8; FRAME_LENGTH];
            loop {
                match ground_station.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });

        let (response_tx, response_rx) = oneshot::channel();
        reload_tx
            .send(ReloadRequest {
                config: Config {
                    relay: None,
                    server: Some(config),
                },
                response: response_tx,
            })
            .await
            .expect("reload request should reach server runtime");
        let mut response_rx = Box::pin(response_rx);
        let mut relay_closed_before_response = false;
        let mut ground_station_closed_before_response = false;
        let response = loop {
            tokio::select! {
                biased;
                _ = &mut relay_closed, if !relay_closed_before_response => {
                    relay_closed_before_response = true;
                }
                _ = &mut ground_station_closed, if !ground_station_closed_before_response => {
                    ground_station_closed_before_response = true;
                }
                response = &mut response_rx => {
                    assert!(
                        relay_closed_before_response && ground_station_closed_before_response,
                        "reload completed before every old session closed"
                    );
                    break response;
                }
            }
        };
        assert_eq!(response.expect("reload response channel"), Ok(()));
        assert!(!task.is_finished());

        time::timeout(Duration::from_secs(1), async {
            loop {
                let disconnected = {
                    let snapshot = snapshot_rx.borrow_and_update();
                    matches!(
                        &*snapshot,
                        SnapshotPayload::Server(snapshot)
                            if !snapshot.ground_station_connected && snapshot.relays.is_empty()
                    )
                };
                if disconnected {
                    break;
                }
                snapshot_rx
                    .changed()
                    .await
                    .expect("server snapshot channel");
            }
        })
        .await
        .expect("replacement runtime should publish a disconnected snapshot");

        let mut reconnected_relay = Framed::new(
            TcpStream::connect(&relay_addr)
                .await
                .expect("reconnect relay session"),
            RemoteCodec::new(),
        );
        reconnected_relay
            .send(RemoteMessage::RelayHello {
                relay_id: "relay-a".into(),
            })
            .await
            .expect("send reconnected relay hello");
        assert!(matches!(
            reconnected_relay.next().await,
            Some(Ok(RemoteMessage::HelloAck { .. }))
        ));

        let mut reconnected_ground_station = TcpStream::connect(&ground_station_addr)
            .await
            .expect("reconnect ground station session");
        reconnected_ground_station
            .write_all(GROUND_STATION_HANDSHAKE)
            .await
            .expect("send reconnected ground station handshake");
        reconnected_ground_station
            .read_exact(&mut heartbeat)
            .await
            .expect("read reconnected ground station heartbeat");

        time::timeout(Duration::from_secs(1), async {
            loop {
                let connected = {
                    let snapshot = snapshot_rx.borrow_and_update();
                    matches!(
                        &*snapshot,
                        SnapshotPayload::Server(snapshot)
                            if snapshot.ground_station_connected
                                && snapshot.relays.iter().any(|relay| relay.connected)
                    )
                };
                if connected {
                    break;
                }
                snapshot_rx
                    .changed()
                    .await
                    .expect("server snapshot channel");
            }
        })
        .await
        .expect("reconnected sessions should appear in the server snapshot");

        shutdown_tx.send(true).expect("shutdown server runtime");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("server runtime should stop")
            .expect("server runtime task")
            .expect("server runtime should stop cleanly");
    }

    #[tokio::test]
    async fn ground_station_inbound_data_is_forwarded_to_active_relay() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener
            .local_addr()
            .expect("test listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("state mutex");
            server.register_relay("relay-a", now);
            server.register_link("relay-a", "serial-1", "serial");
            server.set_relay_status("relay-a", true);
            server.set_link_health(
                "relay-a",
                "serial-1",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "test"),
                Some(PathId::new("relay-a", "serial-1"))
            );
        }
        let (relay_tx, mut relay_rx) = mpsc::channel(1);
        let (active_path_tx, _active_path_rx) = watch::channel(None);
        let relay_senders = Arc::new(Mutex::new(BTreeMap::from([(
            "relay-a".to_owned(),
            RelaySender {
                connection_id: 1,
                sender: relay_tx,
                active_path_sender: active_path_tx,
            },
        )])));
        let sender_slot = Arc::new(Mutex::new(None));
        let sender_slot_for_task = sender_slot.clone();
        let state_for_task = state.clone();
        let relay_senders_for_task = relay_senders.clone();
        let next_generation = Arc::new(AtomicU64::new(1));
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders_for_task,
                Duration::from_secs(60),
                next_generation,
            )
            .await
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");
        client
            .write_all(b"dqiang")
            .await
            .expect("ground station handshake");
        let expected = vec![0x10, 0x20, 0xA5, 0xFF];
        client
            .write_all(&expected)
            .await
            .expect("send ground station control bytes");

        let outbound = time::timeout(Duration::from_secs(1), relay_rx.recv())
            .await
            .expect("relay should receive control bytes")
            .expect("relay queue should remain open");
        match outbound {
            RelayOutbound::Control { target, payload } => {
                assert_eq!(target, PathId::new("relay-a", "serial-1"));
                assert_eq!(payload, expected);
            }
        }

        task.abort();
    }

    #[tokio::test]
    async fn ground_station_heartbeat_is_sent_on_data_connection() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ground station listener");
        let address = listener
            .local_addr()
            .expect("ground station listener address")
            .to_string();
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("state mutex");
            server.register_relay("relay-a", now);
            server.register_link("relay-a", "serial-radio-1", "serial");
            server.set_relay_status("relay-a", true);
            server.set_link_health(
                "relay-a",
                "serial-radio-1",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "heartbeat test"),
                Some(PathId::new("relay-a", "serial-radio-1"))
            );
        }
        let sender_slot = Arc::new(Mutex::new(None));
        let relay_senders = Arc::new(Mutex::new(BTreeMap::new()));
        let state_for_task = state.clone();
        let sender_slot_for_task = sender_slot.clone();
        let relay_senders_for_task = relay_senders.clone();
        let next_generation = Arc::new(AtomicU64::new(1));
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders_for_task,
                Duration::from_millis(20),
                next_generation,
            )
            .await
        });

        let mut client = TcpStream::connect(address)
            .await
            .expect("ground station should connect");
        client
            .write_all(b"dqiang")
            .await
            .expect("ground station handshake");
        let mut received = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        time::timeout(Duration::from_secs(1), client.read_exact(&mut received))
            .await
            .expect("heartbeat should be sent")
            .expect("read heartbeat frame");
        assert!(crate::ground_station_protocol::is_valid_frame(&received));
        assert_eq!(received[2], crate::ground_station_protocol::HEARTBEAT_TYPE);
        assert_eq!(
            received[5],
            crate::ground_station_protocol::HEARTBEAT_STATUS_ACTIVE_PATH
        );

        task.abort();
    }

    #[test]
    fn valid_aircraft_frame_is_forwarded_without_appended_heartbeat() {
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("state mutex");
            server.register_relay("relay-a", now);
            server.register_link("relay-a", "serial-radio-1", "serial");
            server.set_relay_status("relay-a", true);
            server.set_link_health(
                "relay-a",
                "serial-radio-1",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "framing test"),
                Some(PathId::new("relay-a", "serial-radio-1"))
            );
        }

        let mut aircraft = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        aircraft[..2].copy_from_slice(&crate::ground_station_protocol::FRAME_HEADER);
        for (index, byte) in aircraft[2..30].iter_mut().enumerate() {
            *byte = 0x20u8.wrapping_add(index as u8);
        }
        let [c0, c1] = crate::ground_station_protocol::crc_bytes(&aircraft[2..30]);
        aircraft[30] = c0;
        aircraft[31] = c1;

        let aircraft_framer = Arc::new(Mutex::new(AircraftFrameAssembler::default()));
        let outputs = frame_aircraft_payload(
            &aircraft_framer,
            &PathId::new("relay-a", "serial-radio-1"),
            &aircraft,
        );

        assert_eq!(outputs.len(), 1);
        assert_eq!(
            outputs[0].len(),
            crate::ground_station_protocol::FRAME_LENGTH
        );
        assert_eq!(&outputs[0], &aircraft);
    }

    #[tokio::test]
    async fn server_relay_and_ground_station_sockets_have_nodelay_enabled() {
        let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();
        register_socket_probe(probe_tx);

        let (relay_addr, ground_station_addr) = available_loopback_addrs();
        let config = ServerConfig {
            id: "server-nodelay-test".into(),
            listen_addr: relay_addr.clone(),
            ground_station_listen_addr: ground_station_addr.clone(),
            ground_station_heartbeat_interval_ms: 20,
            heartbeat_timeout_ms: 200,
            data_timeout_ms: 200,
            video: None,
            auth: None,
        };
        let (snapshot_tx, _) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_reload_tx, reload_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = oneshot::channel();

        let task = tokio::spawn(run(config, snapshot_tx, shutdown_rx, reload_rx, Some(ready_tx)));
        time::timeout(Duration::from_secs(1), ready_rx)
            .await
            .expect("server ready timeout")
            .expect("server ready");

        let relay_client = TcpStream::connect(&relay_addr).await.expect("connect relay");
        let relay_client_addr = relay_client.local_addr().expect("relay client local addr");
        assert!(relay_client.set_nodelay(true).is_ok());
        assert!(relay_client.nodelay().unwrap());

        let relay_inspection = time::timeout(Duration::from_secs(1), async {
            while let Some(item) = probe_rx.recv().await {
                if item.peer_addr == relay_client_addr {
                    return item;
                }
            }
            panic!("failed to receive probe inspection for relay connection");
        })
        .await
        .expect("probe timeout for relay");

        assert_eq!(relay_inspection.role, SocketRole::Relay);
        assert!(
            relay_inspection.nodelay,
            "服务端接受的 Relay 套接字未开启 TCP_NODELAY"
        );

        let gs_client = TcpStream::connect(&ground_station_addr).await.expect("connect gs");
        let gs_client_addr = gs_client.local_addr().expect("gs client local addr");
        assert!(gs_client.set_nodelay(true).is_ok());
        assert!(gs_client.nodelay().unwrap());

        let gs_inspection = time::timeout(Duration::from_secs(1), async {
            while let Some(item) = probe_rx.recv().await {
                if item.peer_addr == gs_client_addr {
                    return item;
                }
            }
            panic!("failed to receive probe inspection for ground station connection");
        })
        .await
        .expect("probe timeout for ground station");

        assert_eq!(gs_inspection.role, SocketRole::GroundStation);
        assert!(
            gs_inspection.nodelay,
            "服务端接受的 Ground Station 套接字未开启 TCP_NODELAY"
        );

        unregister_socket_probe();
        shutdown_tx.send(true).expect("shutdown server");
        let _ = task.await;
    }

    #[tokio::test]
    async fn ground_station_reconnection_smoothly_displaces_stale_session_and_remains_active() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ground station listener");
        let address = listener
            .local_addr()
            .expect("listener address")
            .to_string();

        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("state mutex");
            server.register_relay("relay-active", now);
            server.register_link("relay-active", "link-main", "udp");
            server.set_relay_status("relay-active", true);
            server.set_link_health(
                "relay-active",
                "link-main",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "setup active path"),
                Some(PathId::new("relay-active", "link-main"))
            );
        }

        let (relay_tx, mut relay_rx) = mpsc::channel(16);
        let relay_senders = Arc::new(Mutex::new(BTreeMap::from([(
            "relay-active".to_owned(),
            RelaySender {
                connection_id: 1,
                sender: relay_tx,
                active_path_sender: watch::channel(None).0,
            },
        )])));

        let sender_slot = Arc::new(Mutex::new(None));
        let next_generation = Arc::new(AtomicU64::new(1));
        let (_server_shutdown_tx, server_shutdown_rx) = watch::channel(false);

        let server_task = tokio::spawn(accept_ground_stations(
            listener,
            state.clone(),
            sender_slot.clone(),
            relay_senders.clone(),
            Duration::from_secs(60),
            next_generation.clone(),
            server_shutdown_rx,
        ));

        // 1. 客户端 1 连接并握手
        let mut client1 = TcpStream::connect(&address)
            .await
            .expect("client 1 connect");
        client1
            .write_all(b"dqiang")
            .await
            .expect("client 1 handshake");

        // 确认客户端 1 进入活跃状态，generation 为 1
        time::timeout(Duration::from_secs(1), async {
            loop {
                let slot = sender_slot.lock().expect("slot mutex");
                if let Some(session) = slot.as_ref() {
                    if session.generation == 1 {
                        break;
                    }
                }
                drop(slot);
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("client 1 session generation should be 1");
        assert!(state.lock().expect("state mutex").ground_station_connected);

        // 客户端 1 读取初始心跳
        let mut heartbeat1 = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        client1
            .read_exact(&mut heartbeat1)
            .await
            .expect("client 1 read heartbeat");
        assert!(crate::ground_station_protocol::is_valid_frame(&heartbeat1));

        // 2. 模拟网络半开（客户端 1 静默停止收发），客户端 2 发起重连并握手
        let mut client2 = TcpStream::connect(&address)
            .await
            .expect("client 2 connect");
        client2
            .write_all(b"dqiang")
            .await
            .expect("client 2 handshake");

        // 确认客户端 2 顺利顶替客户端 1，slot 中 generation 自增为 2
        let client2_sender = time::timeout(Duration::from_secs(1), async {
            loop {
                let slot = sender_slot.lock().expect("slot mutex");
                if let Some(session) = slot.as_ref() {
                    if session.generation == 2 {
                        break session.sender.clone();
                    }
                }
                drop(slot);
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("client 2 should displace client 1 with generation 2");
        assert!(state.lock().expect("state mutex").ground_station_connected);

        // 客户端 2 可以正常读取心跳
        let mut heartbeat2 = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        client2
            .read_exact(&mut heartbeat2)
            .await
            .expect("client 2 read heartbeat");
        assert!(crate::ground_station_protocol::is_valid_frame(&heartbeat2));

        // 3. 人为关闭客户端 1，旧任务触发退出并调用 finish_ground_station_session(state, slot, generation=1)
        client1.shutdown().await.expect("client 1 shutdown");
        drop(client1);

        // 等待一段时间让客户端 1 的旧任务充分退出执行清理
        time::sleep(Duration::from_millis(100)).await;

        // 关键断言：旧任务退出绝不应清空新任务的 slot，连接状态必须依然为 true
        assert!(
            state.lock().expect("state mutex").ground_station_connected,
            "旧会话任务退出后，新会话连接状态必须保持 true"
        );
        let current_generation = sender_slot
            .lock()
            .expect("slot mutex")
            .as_ref()
            .map(|s| s.generation);
        assert_eq!(
            current_generation,
            Some(2),
            "当前活跃槽位必须依然是客户端 2 的 generation 2"
        );

        // 4. 验证客户端 2 依然具有完全正常的飞控上行与数据下行能力
        let control_payload = vec![0x11, 0x22, 0x33, 0x44];
        client2
            .write_all(&control_payload)
            .await
            .expect("client 2 write control payload");

        let relayed_cmd = time::timeout(Duration::from_secs(1), relay_rx.recv())
            .await
            .expect("relay should receive control command from client 2")
            .expect("relay channel open");
        match relayed_cmd {
            RelayOutbound::Control { target, payload } => {
                assert_eq!(target, PathId::new("relay-active", "link-main"));
                assert_eq!(payload, control_payload);
            }
        }

        // 下行推送验证
        let telemetry_data = vec![0xAA, 0xBB, 0xCC, 0xDD];
        client2_sender
            .send(telemetry_data.clone())
            .await
            .expect("push telemetry down");
        let mut received_telemetry = vec![0u8; telemetry_data.len()];
        client2
            .read_exact(&mut received_telemetry)
            .await
            .expect("client 2 read telemetry");
        assert_eq!(received_telemetry, telemetry_data);

        // 5. 客户端 2 退出后，会话正常注销
        client2.shutdown().await.expect("client 2 shutdown");
        drop(client2);

        time::timeout(Duration::from_secs(1), async {
            loop {
                if !state.lock().expect("state mutex").ground_station_connected {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ground station should be disconnected after client 2 exits");
        assert!(sender_slot.lock().expect("slot mutex").is_none());

        server_task.abort();
    }

    #[tokio::test]
    async fn ground_station_telemetry_backpressure_does_not_block_flight_control() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("listener addr").to_string();

        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let now = Instant::now();
        {
            let mut server = state.lock().expect("state mutex");
            server.register_relay("relay-1", now);
            server.register_link("relay-1", "link-1", "udp");
            server.set_relay_status("relay-1", true);
            server.set_link_health(
                "relay-1",
                "link-1",
                LinkHealth {
                    adapter_ready: true,
                    last_rx_at: Some(now),
                    ..LinkHealth::default()
                },
            );
            assert_eq!(
                server.refresh_active_path(now, "setup active path"),
                Some(PathId::new("relay-1", "link-1"))
            );
        }

        let (relay_tx, mut relay_rx) = mpsc::channel(16);
        let relay_senders = Arc::new(Mutex::new(BTreeMap::from([(
            "relay-1".to_owned(),
            RelaySender {
                connection_id: 1,
                sender: relay_tx,
                active_path_sender: watch::channel(None).0,
            },
        )])));

        let sender_slot = Arc::new(Mutex::new(None));
        let next_generation = Arc::new(AtomicU64::new(1));
        let (_server_shutdown_tx, server_shutdown_rx) = watch::channel(false);

        let server_task = tokio::spawn(accept_ground_stations(
            listener,
            state.clone(),
            sender_slot.clone(),
            relay_senders.clone(),
            Duration::from_millis(10), // 高频心跳加剧下行写入
            next_generation.clone(),
            server_shutdown_rx,
        ));

        // 客户端连接并完成握手
        let mut client = TcpStream::connect(&address)
            .await
            .expect("connect client");
        client
            .set_nodelay(true)
            .expect("set nodelay on test client");
        client.write_all(b"dqiang").await.expect("handshake");

        // 等待会话就绪
        let session_sender = time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(session) = sender_slot.lock().expect("slot mutex").clone() {
                    break session.sender;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("session should be established");

        // 客户端只读一次握手后的首个心跳，随后故意完全停止读取，制造 TCP 接收窗口填满与背压
        let mut initial_heartbeat = [0u8; crate::ground_station_protocol::FRAME_LENGTH];
        client
            .read_exact(&mut initial_heartbeat)
            .await
            .expect("read initial heartbeat");

        // 启动后台持续下发大量下行遥测数据，迅速打满服务端的 TCP 发送缓冲区直至 writer_task 完全挂起
        let (flood_stop_tx, flood_stop_rx) = watch::channel(false);
        let flood_sender = session_sender.clone();
        let flood_task = tokio::spawn(async move {
            let big_chunk = vec![0x5A; 32 * 1024]; // 32KB 大包
            loop {
                if *flood_stop_rx.borrow() {
                    break;
                }
                // 尽可能推入下行队列，触发 TCP 发送缓冲区填满与挂起
                let _ = flood_sender.try_send(big_chunk.clone());
                tokio::task::yield_now().await;
            }
        });

        // 给予短暂时间让 TCP 缓冲区彻底塞满并进入背压阻塞
        time::sleep(Duration::from_millis(150)).await;

        // 在下行写任务完全阻塞挂起的情况下，地面站发送紧急飞控命令
        let emergency_command = vec![0xEB, 0x90, 0x02, 0xFE, 0x12, 0x34, 0x56, 0x78];
        client
            .write_all(&emergency_command)
            .await
            .expect("write emergency command while downlink is backpressured");

        // 验证：服务端的 reader_task 必须毫秒级响应并成功路由该飞控命令，绝不受下行背压影响
        let relayed = time::timeout(Duration::from_millis(500), relay_rx.recv())
            .await
            .expect("紧急飞控指令读取与路由超时：下行遥测背压阻塞了上行飞控命令！")
            .expect("relay channel 保持正常");

        match relayed {
            RelayOutbound::Control { target, payload } => {
                assert_eq!(target, PathId::new("relay-1", "link-1"));
                assert_eq!(payload, emergency_command);
            }
        }

        // 清理测试资源
        let _ = flood_stop_tx.send(true);
        let _ = flood_task.await;
        client.shutdown().await.expect("client shutdown");
        server_task.abort();
    }
}

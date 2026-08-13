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
    LinkHealth, LinkSnapshot, PathId, Role, ServerRelaySnapshot, ServerSnapshot, SwitchRecord,
    VideoSnapshot,
};
use relay_ipc::SnapshotPayload;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use relay_core::path_selector::{PathCandidate, PathSelector};

use crate::ground_station_protocol::{AircraftFrameAssembler, FRAME_LENGTH, build_heartbeat_frame};
use crate::ipc_service::ReloadRequest;
use crate::transport::{RemoteCodec, RemoteMessage};
use crate::video::{self, VideoHealth};

const GROUND_STATION_HANDSHAKE: &[u8; 6] = b"dqiang";
const GROUND_STATION_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum RelayOutbound {
    Control { target: PathId, payload: Vec<u8> },
    ActivePath { link_id: Option<String> },
}

#[derive(Clone, Debug)]
struct RelaySender {
    connection_id: u64,
    sender: mpsc::Sender<RelayOutbound>,
}

#[derive(Clone)]
struct RelayRuntime {
    state: Arc<Mutex<ServerState>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    ground_station_sender: Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
    aircraft_framer: Arc<Mutex<AircraftFrameAssembler>>,
    next_connection_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct ServerLinkState {
    pub kind: String,
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
            request = reload_rx.recv() => {
                let Some(request) = request else {
                    let _ = runtime.shutdown.send(true);
                    return join_runtime(runtime.task).await;
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
    let ground_station_sender = Arc::new(Mutex::new(None::<mpsc::Sender<Vec<u8>>>));
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
    let relay_runtime = RelayRuntime {
        state: state.clone(),
        relay_senders: relay_senders.clone(),
        ground_station_sender: ground_station_sender.clone(),
        aircraft_framer,
        next_connection_id,
    };
    let mut relay_task = tokio::spawn(accept_relays(listener, relay_runtime));
    let mut ground_station_task = tokio::spawn(accept_ground_stations(
        ground_station_listener,
        state.clone(),
        ground_station_sender,
        relay_senders,
        Duration::from_millis(config.ground_station_heartbeat_interval_ms),
    ));
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    let result = tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                debug!("server received shutdown signal");
            }
            Ok(())
        }
        result = &mut relay_task => {
            match result {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error.to_string()).into()),
            }
        }
        result = &mut ground_station_task => {
            match result {
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error.to_string()).into()),
            }
        }
    };
    snapshot_task.abort();
    relay_task.abort();
    ground_station_task.abort();
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
    let mut announced_paths = BTreeMap::<String, (u64, Option<String>)>::new();
    loop {
        ticker.tick().await;
        let (mut snapshot, active_path) = {
            let mut server = state.lock().expect("server state mutex poisoned");
            server.refresh_active_path(Instant::now(), "periodic freshness check");
            (server.snapshot(), server.active_path.clone())
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
                .filter(|path| path.relay_id == *relay_id)
                .map(|path| path.link_id.clone());
            let announcement_key = (sender.connection_id, link_id.clone());
            if announced_paths.get(relay_id) == Some(&announcement_key) {
                continue;
            }
            if sender
                .sender
                .try_send(RelayOutbound::ActivePath {
                    link_id: link_id.clone(),
                })
                .is_ok()
            {
                announced_paths.insert(relay_id.clone(), announcement_key);
            }
        }
        announced_paths.retain(|relay_id, _| current_senders.contains_key(relay_id));
    }
}

async fn accept_relays(listener: TcpListener, runtime: RelayRuntime) -> Result<(), ServerError> {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_relay(stream, runtime).await {
                        warn!(%peer, error = %error, "relay session ended");
                    }
                });
            }
            Err(error) => warn!(error = %error, "failed to accept relay session"),
        }
    }
}

async fn handle_relay(stream: TcpStream, runtime: RelayRuntime) -> Result<(), io::Error> {
    let RelayRuntime {
        state,
        relay_senders,
        ground_station_sender,
        aircraft_framer,
        next_connection_id,
    } = runtime;
    let mut framed = Framed::new(stream, RemoteCodec::new());
    let Some(first) = framed.next().await else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "relay closed before hello",
        ));
    };
    let relay_id = match first? {
        RemoteMessage::RelayHello { relay_id } if !relay_id.trim().is_empty() => relay_id,
        _ => {
            framed
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
    framed
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
    relay_senders
        .lock()
        .expect("relay sender mutex poisoned")
        .insert(
            relay_id.clone(),
            RelaySender {
                connection_id,
                sender: outbound_tx,
            },
        );

    let result = loop {
        tokio::select! {
            outbound = outbound_rx.recv() => {
                let Some(outbound) = outbound else { break Ok(()); };
                if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                    break Ok(());
                }
                match outbound {
                    RelayOutbound::Control { target, payload } => {
                        let current_target = {
                            let mut server = state.lock().expect("server state mutex poisoned");
                            server.refresh_active_path(Instant::now(), "control send freshness check")
                        };
                        if current_target.as_ref() != Some(&target) {
                            continue;
                        }
                        framed.send(RemoteMessage::ControlData {
                            relay_id: target.relay_id,
                            link_id: target.link_id,
                            payload,
                        }).await?;
                    }
                    RelayOutbound::ActivePath { link_id } => {
                        framed.send(RemoteMessage::ActivePath {
                            relay_id: relay_id.clone(),
                            link_id,
                        }).await?;
                    }
                }
            }
            inbound = framed.next() => {
                if !is_current_connection(&relay_senders, &relay_id, connection_id) {
                    break Ok(());
                }
                let Some(inbound) = inbound else { break Err(io::Error::new(io::ErrorKind::UnexpectedEof, "relay closed session")); };
                match inbound? {
                    RemoteMessage::LinkAnnounce { relay_id: announced, link_id, kind } if announced == relay_id => {
                        state.lock().expect("server state mutex poisoned").register_link(&relay_id, &link_id, kind);
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
                        let now = Instant::now();
                        let mut server = state.lock().expect("server state mutex poisoned");
                        let active = server.record_aircraft_data(&relay_id, &link_id, now, payload.len());
                        drop(server);
                        if active == Some(true) {
                            let path = PathId::new(&relay_id, &link_id);
                            let frames = frame_aircraft_payload(
                                &aircraft_framer,
                                &path,
                                &payload,
                            );
                            let sender = ground_station_sender.lock().expect("ground station sender mutex poisoned").clone();
                            if let Some(sender) = sender {
                                for outbound in frames {
                                    if let Err(error) = sender.try_send(outbound) {
                                        warn!(error = %error, "dropping framed aircraft data because ground station queue is full");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    RemoteMessage::Rejected { reason } => break Err(io::Error::new(io::ErrorKind::PermissionDenied, reason)),
                    _ => {}
                }
            }
        }
    };
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
    sender_slot: Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    heartbeat_interval: Duration,
) -> Result<(), ServerError> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        let sender_slot = sender_slot.clone();
        let relay_senders = relay_senders.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_ground_station(
                stream,
                state.clone(),
                sender_slot,
                relay_senders,
                heartbeat_interval,
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
}

async fn handle_ground_station(
    stream: TcpStream,
    state: Arc<Mutex<ServerState>>,
    sender_slot: Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
    relay_senders: Arc<Mutex<BTreeMap<String, RelaySender>>>,
    heartbeat_interval: Duration,
) -> Result<(), io::Error> {
    let (mut reader, mut writer) = stream.into_split();
    let mut handshake = [0u8; GROUND_STATION_HANDSHAKE.len()];
    match time::timeout(
        GROUND_STATION_HANDSHAKE_TIMEOUT,
        reader.read_exact(&mut handshake),
    )
    .await
    {
        Ok(Ok(_)) if handshake == *GROUND_STATION_HANDSHAKE => {}
        Ok(Ok(_)) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid ground station handshake",
            ));
        }
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ground station handshake timed out",
            ));
        }
    }

    let (sender, mut outbound_rx) = mpsc::channel(128);
    {
        let mut sender_slot = sender_slot
            .lock()
            .expect("ground station sender mutex poisoned");
        if sender_slot.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a ground station is already connected",
            ));
        }
        *sender_slot = Some(sender);
        state
            .lock()
            .expect("server state mutex poisoned")
            .set_ground_station_connected(true);
    }

    let mut buffer = vec![0u8; 64 * 1024];
    let mut heartbeat = time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let session = loop {
        tokio::select! {
            outbound = outbound_rx.recv() => {
                let Some(outbound) = outbound else { break Ok(()); };
                if let Err(error) = writer.write_all(&outbound).await {
                    break Err(error);
                }
            }
            inbound = reader.read(&mut buffer) => {
                let length = match inbound {
                    Ok(length) => length,
                    Err(error) => break Err(error),
                };
                if length == 0 {
                    break Err(io::Error::new(io::ErrorKind::UnexpectedEof, "ground station closed"));
                }
                route_control(&state, &relay_senders, &buffer[..length]);
            }
            _ = heartbeat.tick() => {
                if let Err(error) = writer.write_all(&next_heartbeat_frame(&state)).await {
                    break Err(error);
                }
            }
        }
    };

    finish_ground_station_session(&state, &sender_slot);
    session
}

fn finish_ground_station_session(
    state: &Arc<Mutex<ServerState>>,
    sender_slot: &Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
) {
    let mut sender_slot = sender_slot
        .lock()
        .expect("ground station sender mutex poisoned");
    *sender_slot = None;
    state
        .lock()
        .expect("server state mutex poisoned")
        .set_ground_station_connected(false);
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
    payload: &[u8],
) {
    let target = {
        let mut server = state.lock().expect("server state mutex poisoned");
        server.refresh_active_path(Instant::now(), "control freshness check")
    };
    let Some(target) = target else {
        warn!("dropping ground-station control data because no active path is available");
        return;
    };
    let sender = relay_senders
        .lock()
        .expect("relay sender mutex poisoned")
        .get(&target.relay_id)
        .map(|entry| entry.sender.clone());
    if let Some(sender) = sender {
        if let Err(error) = sender.try_send(RelayOutbound::Control {
            target,
            payload: payload.to_vec(),
        }) {
            warn!(error = %error, "dropping control data because relay queue is full");
        }
    } else {
        debug!(path = %target, bytes = payload.len(), "dropping control data because active relay is disconnected");
    }
}

impl ServerState {
    pub fn new(heartbeat_timeout: Duration, data_timeout: Duration) -> Self {
        Self {
            heartbeat_timeout,
            data_timeout,
            relays: BTreeMap::new(),
            active_path: None,
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
            .or_insert(ServerLinkState {
                kind: kind.into(),
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
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state,
                sender_slot_for_task,
                relay_senders,
                Duration::from_secs(60),
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
                if let Some(sender) = sender_slot.lock().expect("sender mutex").clone() {
                    break sender;
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
        let task = tokio::spawn({
            let state = state.clone();
            let sender_slot = sender_slot.clone();
            let relay_senders = relay_senders.clone();
            async move {
                let (stream, _) = listener.accept().await.expect("accept ground station");
                handle_ground_station(
                    stream,
                    state,
                    sender_slot,
                    relay_senders,
                    Duration::from_secs(60),
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
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders,
                Duration::from_secs(60),
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
        let server_task = tokio::spawn(accept_ground_stations(
            listener,
            state,
            sender_slot,
            relay_senders,
            Duration::from_secs(60),
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

    #[test]
    fn finishing_ground_station_session_clears_slot_and_state() {
        let state = Arc::new(Mutex::new(ServerState::new(
            Duration::from_secs(3),
            Duration::from_secs(3),
        )));
        let sender_slot = Arc::new(Mutex::new(Some(mpsc::channel(1).0)));
        state
            .lock()
            .expect("server state mutex")
            .set_ground_station_connected(true);

        finish_ground_station_session(&state, &sender_slot);

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
    async fn server_runtime_acknowledges_hot_reload_without_stopping_ipc_owner() {
        let config = ServerConfig {
            id: "server-a".into(),
            listen_addr: "127.0.0.1:0".into(),
            ground_station_listen_addr: "127.0.0.1:0".into(),
            ground_station_heartbeat_interval_ms: 20,
            heartbeat_timeout_ms: 100,
            data_timeout_ms: 100,
            video: None,
        };
        let (snapshot_tx, _) = watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
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
        assert_eq!(
            time::timeout(Duration::from_secs(1), response_rx)
                .await
                .expect("reload response")
                .expect("reload response channel"),
            Ok(())
        );
        assert!(!task.is_finished());

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
        let relay_senders = Arc::new(Mutex::new(BTreeMap::from([(
            "relay-a".to_owned(),
            RelaySender {
                connection_id: 1,
                sender: relay_tx,
            },
        )])));
        let sender_slot = Arc::new(Mutex::new(None));
        let sender_slot_for_task = sender_slot.clone();
        let state_for_task = state.clone();
        let relay_senders_for_task = relay_senders.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders_for_task,
                Duration::from_secs(60),
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
            RelayOutbound::ActivePath { .. } => panic!("unexpected active-path message"),
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
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ground station");
            handle_ground_station(
                stream,
                state_for_task,
                sender_slot_for_task,
                relay_senders_for_task,
                Duration::from_millis(20),
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
}

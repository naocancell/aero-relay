use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use relay_core::config::{Config, load_config, save_config};
use relay_core::model::Role;
use relay_ipc::{IpcMessage, JsonFrameCodec, LocalIpcListener, LocalIpcStream, SnapshotPayload};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::Framed;
use tracing::debug;

use crate::relay;

pub struct ReloadRequest {
    pub config: Config,
    pub response: oneshot::Sender<Result<(), String>>,
}

pub async fn run(
    endpoint: String,
    config: Config,
    config_path: PathBuf,
    role: Role,
    snapshot_rx: watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ReloadRequest>,
) -> io::Result<()> {
    let mut listener = LocalIpcListener::bind(endpoint)?;
    let tcp_bind_addr = std::env::var("RELAY_LINK_TCP_LISTEN")
        .unwrap_or_else(|_| format!("0.0.0.0:{}", relay_ipc::DEFAULT_TCP_CONTROL_PORT));
    let tcp_listener = match tokio::net::TcpListener::bind(&tcp_bind_addr).await {
        Ok(l) => {
            tracing::info!(bind = %tcp_bind_addr, "TCP control interface listening");
            Some(l)
        }
        Err(err) => {
            tracing::warn!(bind = %tcp_bind_addr, %err, "failed to bind TCP control interface, fallback to local only");
            None
        }
    };
    let config = Arc::new(Mutex::new(config));
    loop {
        let stream = match &tcp_listener {
            Some(tcp) => {
                tokio::select! {
                    res = listener.accept() => res?,
                    res = tcp.accept() => {
                        let (s, peer) = res?;
                        let _ = s.set_nodelay(true);
                        tracing::info!(%peer, "TCP control client connected");
                        LocalIpcStream::Tcp(s)
                    }
                }
            }
            None => listener.accept().await?,
        };
        let config = config.clone();
        let config_path = config_path.clone();
        let snapshot_rx = snapshot_rx.clone();
        let reload_tx = reload_tx.clone();
        tokio::spawn(async move {
            if let Err(error) =
                handle_client(stream, config, config_path, role, snapshot_rx, reload_tx).await
            {
                debug!(error = %error, "control client disconnected");
            }
        });
    }
}

async fn handle_client(
    stream: LocalIpcStream,
    config: Arc<Mutex<Config>>,
    config_path: PathBuf,
    role: Role,
    mut snapshot_rx: watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ReloadRequest>,
) -> io::Result<()> {
    let mut framed = Framed::new(stream, JsonFrameCodec::new());
    let mut subscribed = false;
    loop {
        if subscribed {
            tokio::select! {
                changed = snapshot_rx.changed() => {
                    changed.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "snapshot provider stopped"))?;
                    let snapshot = { snapshot_rx.borrow().clone() };
                    framed.send(IpcMessage::StateChanged { snapshot }).await?;
                }
                message = framed.next() => {
                    if !handle_message(message, &mut framed, &config, &config_path, role, &mut subscribed, &snapshot_rx, &reload_tx).await? {
                        return Ok(());
                    }
                }
            }
        } else {
            let message = framed.next().await;
            if !handle_message(
                message,
                &mut framed,
                &config,
                &config_path,
                role,
                &mut subscribed,
                &snapshot_rx,
                &reload_tx,
            )
            .await?
            {
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_message(
    message: Option<Result<IpcMessage, io::Error>>,
    framed: &mut Framed<LocalIpcStream, JsonFrameCodec>,
    config: &Arc<Mutex<Config>>,
    config_path: &Path,
    role: Role,
    subscribed: &mut bool,
    snapshot_rx: &watch::Receiver<SnapshotPayload>,
    reload_tx: &mpsc::Sender<ReloadRequest>,
) -> io::Result<bool> {
    let Some(message) = message else {
        return Ok(false);
    };
    let message = message?;
    match message {
        IpcMessage::GetSnapshot => {
            let snapshot = { snapshot_rx.borrow().clone() };
            framed.send(IpcMessage::Snapshot { snapshot }).await?;
        }
        IpcMessage::Subscribe => {
            *subscribed = true;
            let snapshot = { snapshot_rx.borrow().clone() };
            framed.send(IpcMessage::Snapshot { snapshot }).await?;
        }
        IpcMessage::GetConfig => {
            let config = { config.lock().expect("config mutex poisoned").clone() };
            framed
                .send(IpcMessage::Config {
                    config: Box::new(config),
                })
                .await?;
        }
        IpcMessage::GetSerialPorts => match relay::list_serial_ports() {
            Ok(ports) => {
                framed.send(IpcMessage::SerialPorts { ports }).await?;
            }
            Err(error) => {
                framed
                    .send(IpcMessage::Error {
                        message: format!("读取串口列表失败: {error}"),
                    })
                    .await?;
            }
        },
        IpcMessage::SaveConfig { config: candidate } => {
            let candidate = *candidate;
            let selected_role_is_present = match role {
                Role::Relay => candidate.relay.is_some(),
                Role::Server => candidate.server.is_some(),
            };
            if !selected_role_is_present {
                framed
                    .send(IpcMessage::Error {
                        message: "当前启动模式的配置段不能被删除".into(),
                    })
                    .await?;
                return Ok(true);
            }
            match save_config(config_path, &candidate) {
                Ok(()) => {
                    {
                        *config.lock().expect("config mutex poisoned") = candidate;
                    }
                    framed.send(IpcMessage::ConfigSaved).await?;
                }
                Err(error) => {
                    framed
                        .send(IpcMessage::Error {
                            message: error.to_string(),
                        })
                        .await?;
                }
            }
        }
        IpcMessage::RestartBackend => {
            let candidate = match load_config(config_path) {
                Ok(candidate) => candidate,
                Err(error) => {
                    framed
                        .send(IpcMessage::Error {
                            message: error.to_string(),
                        })
                        .await?;
                    return Ok(true);
                }
            };
            let active_role_is_present = match role {
                Role::Relay => candidate.relay.is_some(),
                Role::Server => candidate.server.is_some(),
            };
            if !active_role_is_present {
                framed
                    .send(IpcMessage::Error {
                        message: "当前启动模式的配置段不能被删除".into(),
                    })
                    .await?;
                return Ok(true);
            }

            let (response_tx, response_rx) = oneshot::channel();
            reload_tx
                .send(ReloadRequest {
                    config: candidate.clone(),
                    response: response_tx,
                })
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "后台运行时已停止"))?;
            match response_rx.await {
                Ok(Ok(())) => {
                    *config.lock().expect("config mutex poisoned") = candidate;
                    framed.send(IpcMessage::BackendRestarted).await?;
                }
                Ok(Err(message)) => {
                    framed.send(IpcMessage::Error { message }).await?;
                }
                Err(_) => {
                    framed
                        .send(IpcMessage::Error {
                            message: "后台运行时未响应重启请求".into(),
                        })
                        .await?;
                }
            }
        }
        _ => {
            framed
                .send(IpcMessage::Error {
                    message: "message is not a request".into(),
                })
                .await?;
        }
    }
    Ok(true)
}

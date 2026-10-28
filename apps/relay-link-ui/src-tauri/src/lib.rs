use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay_core::config::Config;
use relay_ipc::{fixed_endpoint, IpcMessage, JsonFrameCodec, LocalIpcClient, SnapshotPayload};
use tauri::{Emitter, State};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::codec::Framed;

#[derive(Clone)]
struct AppState {
    endpoint: Arc<RwLock<String>>,
    subscription_started: Arc<AtomicBool>,
}

#[tauri::command]
async fn set_endpoint(state: State<'_, AppState>, endpoint: String) -> Result<String, String> {
    let clean = endpoint.trim().to_string();
    let mut w = state.endpoint.write().await;
    *w = clean.clone();
    Ok(clean)
}

#[tauri::command]
async fn get_endpoint(state: State<'_, AppState>) -> Result<String, String> {
    let r = state.endpoint.read().await;
    Ok(r.clone())
}

#[tauri::command]
async fn get_snapshot(state: State<'_, AppState>) -> Result<SnapshotPayload, String> {
    let endpoint = state.endpoint.read().await.clone();
    let mut framed = connect(&endpoint)
        .await
        .map_err(|error| error.to_string())?;
    framed
        .send(IpcMessage::GetSnapshot)
        .await
        .map_err(|error| error.to_string())?;
    match framed
        .next()
        .await
        .transpose()
        .map_err(|error| error.to_string())?
    {
        Some(IpcMessage::Snapshot { snapshot }) => Ok(snapshot),
        Some(IpcMessage::Error { message }) => Err(message),
        Some(other) => Err(format!("unexpected IPC response: {other:?}")),
        None => Err("后台服务已断开".into()),
    }
}

#[tauri::command]
async fn get_config(state: State<'_, AppState>) -> Result<Config, String> {
    let endpoint = state.endpoint.read().await.clone();
    let mut framed = connect(&endpoint)
        .await
        .map_err(|error| error.to_string())?;
    framed
        .send(IpcMessage::GetConfig)
        .await
        .map_err(|error| error.to_string())?;
    match framed
        .next()
        .await
        .transpose()
        .map_err(|error| error.to_string())?
    {
        Some(IpcMessage::Config { config }) => Ok(*config),
        Some(IpcMessage::Error { message }) => Err(message),
        Some(other) => Err(format!("unexpected IPC response: {other:?}")),
        None => Err("后台服务已断开".into()),
    }
}

#[tauri::command]
async fn save_config(state: State<'_, AppState>, config: Config) -> Result<(), String> {
    let endpoint = state.endpoint.read().await.clone();
    let mut framed = connect(&endpoint)
        .await
        .map_err(|error| error.to_string())?;
    framed
        .send(IpcMessage::SaveConfig {
            config: Box::new(config),
        })
        .await
        .map_err(|error| error.to_string())?;
    match framed
        .next()
        .await
        .transpose()
        .map_err(|error| error.to_string())?
    {
        Some(IpcMessage::ConfigSaved) => Ok(()),
        Some(IpcMessage::Error { message }) => Err(message),
        Some(other) => Err(format!("unexpected IPC response: {other:?}")),
        None => Err("后台服务已断开".into()),
    }
}

#[tauri::command]
async fn get_serial_ports(state: State<'_, AppState>) -> Result<Vec<String>, String> {
    let endpoint = state.endpoint.read().await.clone();
    let mut framed = connect(&endpoint)
        .await
        .map_err(|error| error.to_string())?;
    framed
        .send(IpcMessage::GetSerialPorts)
        .await
        .map_err(|error| error.to_string())?;
    match framed
        .next()
        .await
        .transpose()
        .map_err(|error| error.to_string())?
    {
        Some(IpcMessage::SerialPorts { ports }) => Ok(ports),
        Some(IpcMessage::Error { message }) => Err(message),
        Some(other) => Err(format!("unexpected IPC response: {other:?}")),
        None => Err("后台服务已断开".into()),
    }
}

#[tauri::command]
async fn restart_backend(state: State<'_, AppState>) -> Result<(), String> {
    let endpoint = state.endpoint.read().await.clone();
    let mut framed = connect(&endpoint)
        .await
        .map_err(|error| error.to_string())?;
    framed
        .send(IpcMessage::RestartBackend)
        .await
        .map_err(|error| error.to_string())?;
    match framed
        .next()
        .await
        .transpose()
        .map_err(|error| error.to_string())?
    {
        Some(IpcMessage::BackendRestarted) => Ok(()),
        Some(IpcMessage::Error { message }) => Err(message),
        Some(other) => Err(format!("unexpected IPC response: {other:?}")),
        None => Err("后台服务已断开".into()),
    }
}

#[tauri::command]
async fn subscribe_snapshot(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    if state
        .subscription_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(());
    }
    let endpoint_ref = state.endpoint.clone();
    tauri::async_runtime::spawn(async move {
        let mut attempt = 0usize;
        loop {
            let endpoint = endpoint_ref.read().await.clone();
            match connect(&endpoint).await {
                Ok(mut framed) => {
                    if framed.send(IpcMessage::Subscribe).await.is_err() {
                        let _ = app.emit("ipc-status", false);
                    } else {
                        match framed.next().await {
                            Some(Ok(IpcMessage::Snapshot { snapshot }))
                            | Some(Ok(IpcMessage::StateChanged { snapshot })) => {
                                attempt = 0;
                                let _ = app.emit("state-changed", snapshot);
                                let _ = app.emit("ipc-status", true);
                                loop {
                                    let current_endpoint = endpoint_ref.read().await.clone();
                                    if current_endpoint != endpoint {
                                        break;
                                    }
                                    tokio::select! {
                                        msg = framed.next() => {
                                            match msg {
                                                Some(Ok(IpcMessage::Snapshot { snapshot }))
                                                | Some(Ok(IpcMessage::StateChanged { snapshot })) => {
                                                    let _ = app.emit("state-changed", snapshot);
                                                }
                                                Some(Ok(_)) => {}
                                                Some(Err(_)) | None => break,
                                            }
                                        }
                                        _ = sleep(Duration::from_millis(500)) => {}
                                    }
                                }
                                let _ = app.emit("ipc-status", false);
                            }
                            Some(Ok(_)) | Some(Err(_)) | None => {
                                let _ = app.emit("ipc-status", false);
                            }
                        }
                    }
                }
                Err(_) => {
                    let _ = app.emit("ipc-status", false);
                }
            }
            sleep(retry_delay(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    });
    Ok(())
}

async fn connect(
    endpoint: &str,
) -> Result<Framed<relay_ipc::LocalIpcStream, JsonFrameCodec>, std::io::Error> {
    let stream = LocalIpcClient::connect(endpoint).await?;
    Ok(Framed::new(stream, JsonFrameCodec::new()))
}

fn retry_delay(attempt: usize) -> Duration {
    match attempt {
        0 => Duration::from_secs(1),
        1 => Duration::from_secs(2),
        _ => Duration::from_secs(5),
    }
}

pub fn run() {
    let endpoint = fixed_endpoint();
    tauri::Builder::default()
        .manage(AppState {
            endpoint: Arc::new(RwLock::new(endpoint)),
            subscription_started: Arc::new(AtomicBool::new(false)),
        })
        .invoke_handler(tauri::generate_handler![
            get_snapshot,
            get_config,
            save_config,
            get_serial_ports,
            restart_backend,
            subscribe_snapshot,
            set_endpoint,
            get_endpoint
        ])
        .run(tauri::generate_context!())
        .expect("error while running relay-link-ui");
}

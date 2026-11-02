use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::{header, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use relay_core::config::{save_config, Config};
use relay_core::model::Role;
use relay_ipc::SnapshotPayload;
use rust_embed::RustEmbed;
use tokio::sync::{mpsc, oneshot, watch};

use crate::ipc_service::ReloadRequest;
use crate::relay;

#[derive(RustEmbed)]
#[folder = "../relay-link-ui/dist/"]
struct EmbeddedAssets;

#[derive(Clone)]
struct AppState {
    config: Arc<Mutex<Config>>,
    config_path: PathBuf,
    role: Role,
    snapshot_rx: watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ReloadRequest>,
}

pub async fn run(
    config: Config,
    config_path: PathBuf,
    role: Role,
    snapshot_rx: watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ReloadRequest>,
) -> io::Result<()> {
    let bind_addr = std::env::var("RELAY_LINK_WEB_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let state = AppState {
        config: Arc::new(Mutex::new(config)),
        config_path,
        role,
        snapshot_rx,
        reload_tx,
    };

    let cors = tower_http::cors::CorsLayer::permissive();

    let app = Router::new()
        .route("/api/snapshot", get(get_snapshot))
        .route("/api/config", get(get_config).post(save_config_handler))
        .route("/api/restart", post(restart_handler))
        .route("/api/serial-ports", get(get_serial_ports))
        .route("/api/events", get(events_handler))
        .fallback(static_handler)
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!(bind = %bind_addr, "Web UI & REST API listening (open in browser)");
    axum::serve(listener, app)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

async fn get_snapshot(State(state): State<AppState>) -> Json<SnapshotPayload> {
    Json(state.snapshot_rx.borrow().clone())
}

async fn get_config(State(state): State<AppState>) -> Json<Config> {
    let cfg = state.config.lock().expect("config mutex poisoned").clone();
    Json(cfg)
}

async fn save_config_handler(
    State(state): State<AppState>,
    Json(candidate): Json<Config>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let selected_role_is_present = match state.role {
        Role::Relay => candidate.relay.is_some(),
        Role::Server => candidate.server.is_some(),
    };
    if !selected_role_is_present {
        return Err((
            StatusCode::BAD_REQUEST,
            "当前启动模式的配置段不能被删除".to_string(),
        ));
    }
    match save_config(&state.config_path, &candidate) {
        Ok(()) => {
            *state.config.lock().expect("config mutex poisoned") = candidate;
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("保存配置失败: {err}"),
        )),
    }
}

async fn restart_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let current_config = state.config.lock().expect("config mutex poisoned").clone();
    let (response_tx, response_rx) = oneshot::channel();
    let request = ReloadRequest {
        config: current_config,
        response: response_tx,
    };
    if state.reload_tx.send(request).await.is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "后台服务已停止".to_string(),
        ));
    }
    match response_rx.await {
        Ok(Ok(())) => Ok(Json(serde_json::json!({ "ok": true }))),
        Ok(Err(err)) => Err((StatusCode::INTERNAL_SERVER_ERROR, err)),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "等待后台重启超时".to_string(),
        )),
    }
}

async fn get_serial_ports() -> Result<Json<Vec<String>>, (StatusCode, String)> {
    match relay::list_serial_ports() {
        Ok(ports) => Ok(Json(ports)),
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("读取串口列表失败: {err}"),
        )),
    }
}

async fn events_handler(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.snapshot_rx.clone();
    let stream = futures_util::stream::unfold((rx, true), |(mut rx, first)| async move {
        if !first {
            if rx.changed().await.is_err() {
                return None;
            }
        }
        let snap = rx.borrow().clone();
        let json = serde_json::to_string(&snap).unwrap_or_default();
        let event = Ok::<_, std::convert::Infallible>(Event::default().data(json));
        Some((event, (rx, false)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn guess_mime(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".js") || path.ends_with(".mjs") {
        "application/javascript; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json; charset=utf-8"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else {
        "application/octet-stream"
    }
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();
    if path.is_empty() {
        path = "index.html".to_string();
    }
    match EmbeddedAssets::get(&path) {
        Some(content) => {
            let mime = guess_mime(&path);
            ([(header::CONTENT_TYPE, mime)], content.data).into_response()
        }
        None => {
            if let Some(index) = EmbeddedAssets::get("index.html") {
                ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], index.data).into_response()
            } else {
                (StatusCode::NOT_FOUND, "Web UI not bundled").into_response()
            }
        }
    }
}

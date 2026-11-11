use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    middleware::{self, Next},
    extract::{Path as AxumPath, Request, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{any, get, post},
    Json, Router,
};
use relay_core::config::{save_config, AuthRole, Config, SUPER_ADMIN_USERNAME};
use serde::Serialize;
use relay_core::model::{Role, SnapshotPayload};
use rust_embed::RustEmbed;
use tokio::sync::{mpsc, oneshot, watch};

use crate::http_proxy::{execute_local_http_request, global_proxy_hub, ProxyResponsePayload};
use crate::io_controller::IoControllerHandle;
use crate::relay;
use crate::transport::ProxyTarget;

pub struct ReloadRequest {
    pub config: Config,
    pub response: oneshot::Sender<Result<(), String>>,
}

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
    io_controller: Option<IoControllerHandle>,
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let mut table = [255u8; 256];
    for (i, &b) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        .iter()
        .enumerate()
    {
        table[b as usize] = i as u8;
    }
    let bytes = input.trim().as_bytes();
    let mut buf = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for &b in bytes {
        if b == b'=' {
            break;
        }
        let val = table[b as usize];
        if val == 255 {
            return None;
        }
        acc = (acc << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            buf.push((acc >> bits) as u8);
        }
    }
    Some(buf)
}

fn percent_decode_str(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next().unwrap_or(0);
            let h2 = chars.next().unwrap_or(0);
            let val = match (char::from(h1).to_digit(16), char::from(h2).to_digit(16)) {
                (Some(v1), Some(v2)) => (v1 * 16 + v2) as u8,
                _ => b'%',
            };
            bytes.push(val);
        } else if b == b'+' {
            bytes.push(b' ');
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_string())
}

fn decode_basic_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let token_str = trimmed
        .strip_prefix("Basic ")
        .or_else(|| trimmed.strip_prefix("basic "))
        .unwrap_or(trimmed);
    if token_str.is_empty() {
        return None;
    }
    base64_decode(token_str).and_then(|bytes| String::from_utf8(bytes).ok())
}

async fn basic_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path();
    // 远程代理路由（/proxy/relay/、/proxy/llink/、/proxy/llink-ws/）直接透传，
    // 由目标中继站自身的 basic_auth_middleware 校验目标中继站的账号凭据
    if req.method() == Method::OPTIONS
        || path.starts_with("/proxy/llink-ws/")
        || path == "/proxy/relay"
        || path.starts_with("/proxy/relay/")
        || path == "/proxy/llink"
        || path.starts_with("/proxy/llink/")
    {
        req.extensions_mut().insert(AuthRole::Anonymous);
        return next.run(req).await;
    }

    let auth_cfg = {
        let cfg = state.config.lock().expect("config mutex poisoned");
        cfg.effective_auth_for_role(state.role)
    };

    let mut explicitly_logged_out = false;
    let mut auth_decoded: Option<String> = None;

    // 1. 优先读取前端显式控制的 X-Relay-Auth 头（支持通过 "none" 强制忽略浏览器残留的 Basic Auth 缓存）
    if let Some(relay_auth) = req
        .headers()
        .get("x-relay-auth")
        .and_then(|val| val.to_str().ok())
    {
        let trimmed = relay_auth.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
            explicitly_logged_out = true;
        } else {
            auth_decoded = decode_basic_token(trimmed);
        }
    }

    // 2. 其次读取 URL Query 中的 auth / token 参数（用于 EventSource SSE 连接，优先级高于浏览器残留的 Authorization 头）
    if !explicitly_logged_out && auth_decoded.is_none() {
        if let Some(query) = req.uri().query() {
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    if k == "auth" || k == "token" {
                        let decoded_val = percent_decode_str(v);
                        let trimmed = decoded_val.trim();
                        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
                            explicitly_logged_out = true;
                        } else {
                            auth_decoded = decode_basic_token(trimmed);
                        }
                        break;
                    }
                }
            }
        }
    }

    // 3. 最后回退读取标准 Authorization 请求头（兼容 API 客户端与测试）
    if !explicitly_logged_out && auth_decoded.is_none() {
        auth_decoded = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|val| val.to_str().ok())
            .and_then(decode_basic_token);
    }

    if let Some(decoded) = auth_decoded {
        let (user, pass) = decoded.split_once(':').unwrap_or((&decoded, ""));
        if let Some(role) = auth_cfg.authenticate(user, pass) {
            req.extensions_mut().insert(role);
            return next.run(req).await;
        } else {
            // 不返回 WWW-Authenticate 响应头，防止触发浏览器原生 Basic Auth 弹窗
            return (
                StatusCode::UNAUTHORIZED,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "未授权访问：用户名或密码错误\n",
            )
                .into_response();
        }
    }

    let path = req.uri().path();
    let is_api_or_proxy = path.starts_with("/api/") || path.starts_with("/proxy/");

    // 静态资源（前端单页应用 HTML/JS/CSS）放行，由前端展示内置登录面板
    if !is_api_or_proxy {
        req.extensions_mut().insert(AuthRole::Anonymous);
        return next.run(req).await;
    }

    // /api/auth/me（含远程代理路径）允许匿名读取认证策略与登录角色
    if path == "/api/auth/me"
        || (path.starts_with("/proxy/relay/") && path.ends_with("/api/auth/me"))
    {
        req.extensions_mut().insert(AuthRole::Anonymous);
        return next.run(req).await;
    }

    if auth_cfg.enabled {
        // 不返回 WWW-Authenticate 响应头，由前端内置登录弹窗接管
        (
            StatusCode::UNAUTHORIZED,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "未授权访问：请输入管理用户名和密码\n",
        )
            .into_response()
    } else {
        req.extensions_mut().insert(AuthRole::Anonymous);
        next.run(req).await
    }
}

#[derive(Debug, Serialize)]
struct AuthInfoPayload {
    role: AuthRole,
    is_super_admin: bool,
    auth_enabled: bool,
    custom_username: String,
}

async fn get_auth_info(
    State(state): State<AppState>,
    axum::extract::Extension(current_role): axum::extract::Extension<AuthRole>,
) -> Json<AuthInfoPayload> {
    let cfg = state.config.lock().expect("config mutex poisoned");
    let auth = cfg.effective_auth_for_role(state.role);
    Json(AuthInfoPayload {
        role: current_role,
        is_super_admin: current_role == AuthRole::SuperAdmin,
        auth_enabled: auth.enabled,
        custom_username: auth.username.clone(),
    })
}

pub async fn run(
    config: Config,
    config_path: PathBuf,
    role: Role,
    snapshot_rx: watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ReloadRequest>,
    io_controller: Option<IoControllerHandle>,
) -> io::Result<()> {
    let bind_addr = std::env::var("RELAY_LINK_WEB_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:7480".to_string());

    let state = AppState {
        config: Arc::new(Mutex::new(config)),
        config_path,
        role,
        snapshot_rx,
        reload_tx,
        io_controller,
    };

    let cors = tower_http::cors::CorsLayer::permissive();

    let app = Router::new()
        .route("/api/snapshot", get(get_snapshot))
        .route("/api/auth/me", get(get_auth_info))
        .route("/api/config", get(get_config).post(save_config_handler))
        .route("/api/restart", post(restart_handler))
        .route("/api/serial-ports", get(get_serial_ports))
        .route("/api/events", get(events_handler))
        .route("/api/io/channel", post(set_io_channel_handler))
        .route("/api/io/all", post(set_io_all_handler))
        .route("/api/io/status", get(get_io_status_handler))
        .route("/proxy/relay/{relay_id}", any(proxy_relay_root_handler))
        .route("/proxy/relay/{relay_id}/", any(proxy_relay_root_handler))
        .route(
            "/proxy/relay/{relay_id}/{*path}",
            any(proxy_relay_path_handler),
        )
        .route("/proxy/llink-ws/{relay_id}", get(proxy_llink_ws_handler))
        .route("/proxy/llink/{relay_id}", any(proxy_llink_root_handler))
        .route("/proxy/llink/{relay_id}/", any(proxy_llink_root_handler))
        .route(
            "/proxy/llink/{relay_id}/{*path}",
            any(proxy_llink_path_handler),
        )
        .fallback(static_handler)
        .layer(middleware::from_fn_with_state(state.clone(), basic_auth_middleware))
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

fn redact_config_passwords(mut cfg: Config) -> Config {
    if let Some(relay) = cfg.relay.as_mut() {
        if let Some(auth) = relay.auth.as_mut() {
            auth.password.clear();
        }
    }
    if let Some(server) = cfg.server.as_mut() {
        if let Some(auth) = server.auth.as_mut() {
            auth.password.clear();
        }
    }
    cfg
}

async fn get_config(State(state): State<AppState>) -> Json<Config> {
    let cfg = state.config.lock().expect("config mutex poisoned").clone();
    Json(redact_config_passwords(cfg))
}

async fn save_config_handler(
    State(state): State<AppState>,
    axum::extract::Extension(current_role): axum::extract::Extension<AuthRole>,
    Json(mut candidate): Json<Config>,
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
    };

    let current_config = state.config.lock().expect("config mutex poisoned").clone();

    // 当提交的密码字段为空字符串时，表示未修改密码，自动沿用原配置中已保存的密码
    if let (Some(cand_relay), Some(curr_relay)) =
        (candidate.relay.as_mut(), current_config.relay.as_ref())
    {
        if let (Some(cand_auth), Some(curr_auth)) =
            (cand_relay.auth.as_mut(), curr_relay.auth.as_ref())
        {
            if cand_auth.password.is_empty() {
                cand_auth.password = curr_auth.password.clone();
            }
        }
    }
    if let (Some(cand_server), Some(curr_server)) =
        (candidate.server.as_mut(), current_config.server.as_ref())
    {
        if let (Some(cand_auth), Some(curr_auth)) =
            (cand_server.auth.as_mut(), curr_server.auth.as_ref())
        {
            if cand_auth.password.is_empty() {
                cand_auth.password = curr_auth.password.clone();
            }
        }
    }

    let current_auth = current_config.auth_for_role(state.role);
    let candidate_auth = candidate.auth_for_role(state.role);

    if current_auth != candidate_auth && current_role != AuthRole::SuperAdmin {
        return Err((
            StatusCode::FORBIDDEN,
            format!("无权修改账号安全配置：只有超级管理员（{SUPER_ADMIN_USERNAME}）才可以修改账号与登录开关"),
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
    if state.reload_tx.is_closed() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "后台服务已停止".to_string(),
        ));
    }

    if state.role == Role::Relay {
        let reload_tx = state.reload_tx.clone();
        tokio::spawn(async move {
            // 留出短暂窗口让本次 HTTP 200 响应先经由当前隧道发回服务端，避免重启销毁旧隧道导致服务端 504 超时
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let (response_tx, _response_rx) = oneshot::channel();
            let request = ReloadRequest {
                config: current_config,
                response: response_tx,
            };
            let _ = reload_tx.send(request).await;
        });
        return Ok(Json(serde_json::json!({ "ok": true })));
    }

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

async fn proxy_relay_root_handler(
    State(state): State<AppState>,
    AxumPath(relay_id): AxumPath<String>,
    req: Request,
) -> Response {
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let target_uri = format!("/{query}");
    dispatch_proxy(state.role, relay_id, ProxyTarget::RelayWeb, target_uri, req, false).await
}

async fn proxy_relay_path_handler(
    State(state): State<AppState>,
    AxumPath((relay_id, path)): AxumPath<(String, String)>,
    req: Request,
) -> Response {
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let clean_path = path.trim_start_matches('/');
    let target_uri = format!("/{clean_path}{query}");
    dispatch_proxy(state.role, relay_id, ProxyTarget::RelayWeb, target_uri, req, false).await
}

async fn proxy_llink_ws_handler(
    AxumPath(relay_id): AxumPath<String>,
    req: Request,
) -> Response {
    let Some(sec_key) = req
        .headers()
        .get("sec-websocket-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
    else {
        return (StatusCode::BAD_REQUEST, "Missing Sec-WebSocket-Key").into_response();
    };
    let accept_key = crate::http_proxy::compute_websocket_accept(&sec_key);
    let on_upgrade = hyper::upgrade::on(req);
    tokio::spawn(async move {
        if let Ok(upgraded) = on_upgrade.await {
            let io = hyper_util::rt::TokioIo::new(upgraded);
            crate::http_proxy::bridge_llink_websocket(relay_id, io).await;
        }
    });
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "Upgrade")
        .header("Sec-WebSocket-Accept", accept_key)
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn proxy_llink_root_handler(
    State(state): State<AppState>,
    AxumPath(relay_id): AxumPath<String>,
    req: Request,
) -> Response {
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let target_uri = format!("/{query}");
    dispatch_proxy(state.role, relay_id, ProxyTarget::LLink, target_uri, req, true).await
}

async fn proxy_llink_path_handler(
    State(state): State<AppState>,
    AxumPath((relay_id, path)): AxumPath<(String, String)>,
    req: Request,
) -> Response {
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let clean_path = path.trim_start_matches('/');
    let target_uri = format!("/{clean_path}{query}");
    dispatch_proxy(state.role, relay_id, ProxyTarget::LLink, target_uri, req, true).await
}

async fn dispatch_proxy(
    _role: Role,
    relay_id: String,
    target: ProxyTarget,
    target_uri: String,
    req: Request,
    set_llink_cookie: bool,
) -> Response {
    let method = req.method().as_str().to_string();
    let mut headers = Vec::new();
    for (k, v) in req.headers() {
        if let Ok(val) = v.to_str() {
            headers.push((k.as_str().to_string(), val.to_string()));
        }
    }

    let body_bytes = match axum::body::to_bytes(req.into_body(), 4 * 1024 * 1024).await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("读取请求体失败: {e}"),
            )
                .into_response();
        }
    };

    let result: Result<ProxyResponsePayload, (u16, String)> =
        if relay_id == "local" {
            Ok(execute_local_http_request(target, &method, &target_uri, &headers, &body_bytes).await)
        } else {
            global_proxy_hub()
                .forward_request(&relay_id, target, method, target_uri, headers, body_bytes)
                .await
        };

    match result {
        Ok(payload) => {
            let (resp_headers, resp_body) = if target == ProxyTarget::LLink {
                rewrite_llink_content(&relay_id, &payload.headers, payload.body)
            } else {
                (payload.headers, payload.body)
            };

            let status = StatusCode::from_u16(payload.status).unwrap_or(StatusCode::OK);
            let mut builder = Response::builder().status(status);
            for (k, v) in resp_headers {
                if k.eq_ignore_ascii_case("www-authenticate") {
                    continue;
                }
                // A relay on an older build can still forward invalid headers.
                match (
                    axum::http::HeaderName::from_bytes(k.as_bytes()),
                    axum::http::HeaderValue::from_str(&v),
                ) {
                    (Ok(name), Ok(value)) => builder = builder.header(name, value),
                    _ => tracing::debug!(header = %k, "dropping invalid proxied response header"),
                }
            }
            if set_llink_cookie || target == ProxyTarget::LLink {
                builder = builder.header(
                    header::SET_COOKIE,
                    format!("aero_llink_relay={relay_id}; Path=/; SameSite=Lax"),
                );
            }
            builder
                .body(Body::from(resp_body))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err((code, msg)) => (
            StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            msg,
        )
            .into_response(),
    }
}

fn rewrite_llink_content(
    relay_id: &str,
    headers: &[(String, String)],
    body: Vec<u8>,
) -> (Vec<(String, String)>, Vec<u8>) {
    let is_text = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("content-type")
            && (v.contains("text/html")
                || v.contains("javascript")
                || v.contains("text/javascript")
                || v.contains("application/json"))
    });
    if !is_text || body.is_empty() {
        return (headers.to_vec(), body);
    }

    if let Ok(text) = String::from_utf8(body.clone()) {
        let mut modified = text;
        let mut changed = false;

        // 0. 注入全局 WebSocket 代理拦截脚本：
        // 将所有访问 ":8000" 的 WebSocket 请求重定向到同端口的 /proxy/llink-ws/{relay_id} 隧道代理路由，
        // 彻底解决跨公网/服务端访问时 8000 端口不通或未转发导致页面参数/状态无法加载的问题。
        let ws_hook = format!(
            r#"<script>(function(){{var W=window.WebSocket;if(!W||W.__aero)return;var P=function(u,p){{if(typeof u==="string"&&u.indexOf(":8000")!==-1){{var s=window.location.protocol==="https:"?"wss:":"ws:";u=s+"//"+window.location.host+"/proxy/llink-ws/{relay_id}";}}return p!==undefined?new W(u,p):new W(u);}};P.prototype=W.prototype;P.CONNECTING=W.CONNECTING;P.OPEN=W.OPEN;P.CLOSING=W.CLOSING;P.CLOSED=W.CLOSED;P.__aero=true;window.WebSocket=P;}})();</script>"#
        );
        if modified.contains("<head>") && !modified.contains("/proxy/llink-ws/") {
            modified = modified.replacen("<head>", &format!("<head>{ws_hook}"), 1);
            changed = true;
        } else if modified.contains("<HEAD>") && !modified.contains("/proxy/llink-ws/") {
            modified = modified.replacen("<HEAD>", &format!("<HEAD>{ws_hook}"), 1);
            changed = true;
        }

        // 1. 修复 WebSocket 拼接时的带端口非法地址：
        // 将 window.location.host + ":8000" 重写为仅包含 hostname（不含端口号）
        if modified.contains(r#"window.location.host + ":8000""#) {
            modified = modified.replace(
                r#"window.location.host + ":8000""#,
                r#"window.location.hostname + ":8000""#,
            );
            changed = true;
        }
        if modified.contains(r#"window.location.host + ":" + 8000"#) {
            modified = modified.replace(
                r#"window.location.host + ":" + 8000"#,
                r#"window.location.hostname + ":8000""#,
            );
            changed = true;
        }
        if modified.contains("var _url = window.location.host;") {
            modified = modified.replace(
                "var _url = window.location.host;",
                "var _url = window.location.hostname;",
            );
            changed = true;
        }
        if modified.contains("const local_url = window.location.host;") {
            modified = modified.replace(
                "const local_url = window.location.host;",
                "const local_url = window.location.hostname;",
            );
            changed = true;
        }
        if modified.contains("var local_url = window.location.host;") {
            modified = modified.replace(
                "var local_url = window.location.host;",
                "var local_url = window.location.hostname;",
            );
            changed = true;
        }

        // 2. 修复子页面绝对路径跳出代理上下文的问题：
        // 将 "/plus/" 类的根路径重写为包含代理前缀的路径，保证所有 iframe 和请求精准命中代理路由
        let pfx = format!("/proxy/llink/{relay_id}");
        if modified.contains(r#""/plus/"#) {
            modified = modified.replace(r#""/plus/"#, &format!(r#""{pfx}/plus/"#));
            changed = true;
        }
        if modified.contains(r#""plus/"#) {
            modified = modified.replace(r#""plus/"#, &format!(r#""{pfx}/plus/"#));
            changed = true;
        }
        if modified.contains(r#"'/plus/"#) {
            modified = modified.replace(r#"'/plus/"#, &format!(r#"'{pfx}/plus/"#));
            changed = true;
        }
        if modified.contains(r#"'plus/"#) {
            modified = modified.replace(r#"'plus/"#, &format!(r#"'{pfx}/plus/"#));
            changed = true;
        }
        if modified.contains(r#""/login.html""#) {
            modified = modified.replace(r#""/login.html""#, &format!(r#""{pfx}/login.html""#));
            changed = true;
        }
        if modified.contains(r#""/plus.html""#) {
            modified = modified.replace(r#""/plus.html""#, &format!(r#""{pfx}/plus.html""#));
            changed = true;
        }
        if modified.contains(r#""/cgi-bin/"#) {
            modified = modified.replace(r#""/cgi-bin/"#, &format!(r#""{pfx}/cgi-bin/"#));
            changed = true;
        }
        if modified.contains(r#""../cgi-bin/"#) {
            modified = modified.replace(r#""../cgi-bin/"#, &format!(r#""{pfx}/cgi-bin/"#));
            changed = true;
        }
        if modified.contains(r#""./cgi-bin/"#) {
            modified = modified.replace(r#""./cgi-bin/"#, &format!(r#""{pfx}/cgi-bin/"#));
            changed = true;
        }

        if changed {
            let new_bytes = modified.into_bytes();
            let mut new_headers = Vec::new();
            for (k, v) in headers {
                if k.eq_ignore_ascii_case("content-length") {
                    new_headers.push((k.clone(), new_bytes.len().to_string()));
                } else {
                    new_headers.push((k.clone(), v.clone()));
                }
            }
            return (new_headers, new_bytes);
        }
    }

    (headers.to_vec(), body)
}

fn is_llink_feature_path(path: &str) -> bool {
    let p = path.trim_start_matches('/');
    p.starts_with("cgi-bin/")
        || p.starts_with("plus/")
        || p == "plus.html"
        || p.starts_with("plus.html?")
        || p.starts_with("html/")
        || p.starts_with("lib/")
        || p.starts_with("fonts/")
        || p.starts_with("images/")
        || p.starts_with("css/")
        || p.starts_with("js/")
        || p == "login.html"
        || p == "topology.html"
}

fn extract_llink_relay_from_headers(headers: &HeaderMap) -> Option<String> {
    if let Some(referer) = headers.get(header::REFERER).and_then(|v| v.to_str().ok()) {
        if let Some(idx) = referer.find("/proxy/llink/") {
            let rest = &referer[idx + "/proxy/llink/".len()..];
            let relay_id = rest.split(['/', '?', '#']).next().unwrap_or("");
            if !relay_id.is_empty() {
                return Some(relay_id.to_string());
            }
        }
    }
    if let Some(cookie_str) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
        for part in cookie_str.split(';') {
            let trimmed = part.trim();
            if let Some(val) = trimmed.strip_prefix("aero_llink_relay=") {
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
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

async fn static_handler(State(state): State<AppState>, req: Request) -> Response {
    let uri: Uri = req.uri().clone();
    let mut path = uri.path().trim_start_matches('/').to_string();
    if path.is_empty() {
        path = "index.html".to_string();
    }
    if let Some(content) = EmbeddedAssets::get(&path) {
        let mime = guess_mime(&path);
        return ([(header::CONTENT_TYPE, mime)], content.data).into_response();
    }

    let target_uri = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    // 智能推断或提取目标中继站 ID：
    // 1. 优先从 Referer 或 Cookie 提取
    // 2. 如果缺少上下文但命中了 L 链特征路径：
    //    - 若是 Relay 模式：直通本地 "local"
    //    - 若是 Server 模式：自动 fallback 到首个活跃的在线中继站（如 "relay-a"）
    let effective_relay = extract_llink_relay_from_headers(req.headers()).or_else(|| {
        if is_llink_feature_path(&path) {
            match state.role {
                Role::Relay => Some("local".to_string()),
                Role::Server => global_proxy_hub().first_active_relay(),
            }
        } else {
            None
        }
    });

    if let Some(relay_id) = effective_relay {
        return dispatch_proxy(state.role, relay_id, ProxyTarget::LLink, target_uri, req, true)
            .await;
    }

    // 都不匹配时，才作为 SPA 路由回退到 index.html
    if let Some(index) = EmbeddedAssets::get("index.html") {
        ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], index.data).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Web UI not bundled").into_response()
    }
}

#[derive(serde::Deserialize)]
struct SetChannelRequest {
    channel: u8,
    state: bool,
}

async fn set_io_channel_handler(
    State(state): State<AppState>,
    Json(payload): Json<SetChannelRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let Some(io) = &state.io_controller else {
        return Err((StatusCode::BAD_REQUEST, "IO 控制器未启用".to_string()));
    };
    match io.set_channel(payload.channel, payload.state).await {
        Ok(()) => Ok(Json(serde_json::json!({ "ok": true }))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

#[derive(serde::Deserialize)]
struct SetAllRequest {
    state: bool,
}

async fn set_io_all_handler(
    State(state): State<AppState>,
    Json(payload): Json<SetAllRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let Some(io) = &state.io_controller else {
        return Err((StatusCode::BAD_REQUEST, "IO 控制器未启用".to_string()));
    };
    match io.set_all(payload.state).await {
        Ok(()) => Ok(Json(serde_json::json!({ "ok": true }))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

async fn get_io_status_handler(
    State(state): State<AppState>,
) -> Result<Json<relay_core::model::IoControllerSnapshot>, (StatusCode, String)> {
    let Some(io) = &state.io_controller else {
        return Err((StatusCode::BAD_REQUEST, "IO 控制器未启用".to_string()));
    };
    Ok(Json(io.snapshot()))
}

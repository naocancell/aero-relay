use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::http::{HeaderName, HeaderValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::transport::ProxyTarget;

pub fn global_proxy_hub() -> &'static ServerProxyHub {
    static HUB: OnceLock<ServerProxyHub> = OnceLock::new();
    HUB.get_or_init(ServerProxyHub::new)
}

#[derive(Debug, Clone)]
pub struct ProxyRequestCommand {
    pub request_id: u64,
    pub target: ProxyTarget,
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ProxyResponsePayload {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct ServerProxyHub {
    next_request_id: Arc<AtomicU64>,
    relays: Arc<Mutex<BTreeMap<String, (u64, mpsc::Sender<ProxyRequestCommand>)>>>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<ProxyResponsePayload>>>>,
}

impl ServerProxyHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_relay(
        &self,
        relay_id: String,
        connection_id: u64,
        sender: mpsc::Sender<ProxyRequestCommand>,
    ) {
        self.relays
            .lock()
            .expect("proxy hub relays mutex poisoned")
            .insert(relay_id, (connection_id, sender));
    }

    pub fn unregister_relay(&self, relay_id: &str, connection_id: u64) {
        let mut relays = self.relays.lock().expect("proxy hub relays mutex poisoned");
        if relays
            .get(relay_id)
            .is_some_and(|(id, _)| *id == connection_id)
        {
            relays.remove(relay_id);
        }
    }

    pub fn complete_request(&self, request_id: u64, response: ProxyResponsePayload) {
        if let Some(tx) = self
            .pending
            .lock()
            .expect("proxy hub pending mutex poisoned")
            .remove(&request_id)
        {
            let _ = tx.send(response);
        }
    }

    pub async fn forward_request(
        &self,
        relay_id: &str,
        target: ProxyTarget,
        method: String,
        uri: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<ProxyResponsePayload, (u16, String)> {
        let sender = {
            let relays = self.relays.lock().expect("proxy hub relays mutex poisoned");
            relays.get(relay_id).map(|(_, tx)| tx.clone())
        };

        let Some(sender) = sender else {
            return Err((
                502,
                format!("中继站 `{relay_id}` 当前未在线或未建立隧道连接"),
            ));
        };

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("proxy hub pending mutex poisoned")
            .insert(request_id, tx);

        let cmd = ProxyRequestCommand {
            request_id,
            target,
            method,
            uri,
            headers,
            body,
        };

        if sender.send(cmd).await.is_err() {
            self.pending
                .lock()
                .expect("proxy hub pending mutex poisoned")
                .remove(&request_id);
            return Err((502, format!("向中继站 `{relay_id}` 发送代理请求失败")));
        }

        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err((502, "中继站代理通道已关闭".to_string())),
            Err(_) => {
                self.pending
                    .lock()
                    .expect("proxy hub pending mutex poisoned")
                    .remove(&request_id);
                Err((504, format!("等待中继站 `{relay_id}` 响应超时 (10s)")))
            }
        }
    }
}

pub fn resolve_local_target_addr(target: ProxyTarget) -> String {
    match target {
        ProxyTarget::RelayWeb => {
            let listen = std::env::var("RELAY_LINK_WEB_LISTEN")
                .unwrap_or_else(|_| "0.0.0.0:7480".to_string());
            let port = listen
                .rsplit(':')
                .next()
                .and_then(|p| p.parse::<u16>().ok())
                .unwrap_or(7480);
            format!("127.0.0.1:{port}")
        }
        ProxyTarget::LLink => {
            std::env::var("RELAY_L_LINK_ADDR").unwrap_or_else(|_| "192.168.0.181:80".to_string())
        }
    }
}

pub fn spawn_llink_tcp_proxy(listen_addr: &str, target_addr: &str) {
    let listen = listen_addr.to_string();
    let target = target_addr.to_string();
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(&listen).await {
            Ok(l) => {
                tracing::info!(listen = %listen, target = %target, "L-link TCP proxy started");
                l
            }
            Err(e) => {
                tracing::warn!(listen = %listen, error = %e, "failed to bind L-link TCP proxy port");
                return;
            }
        };
        loop {
            let (mut client, peer) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(error = %e, "L-link TCP proxy accept error");
                    continue;
                }
            };
            let target_clone = target.clone();
            tokio::spawn(async move {
                match tokio::net::TcpStream::connect(&target_clone).await {
                    Ok(mut remote) => {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut remote).await;
                    }
                    Err(e) => {
                        tracing::debug!(peer = %peer, target = %target_clone, error = %e, "L-link TCP proxy connect upstream failed");
                    }
                }
            });
        }
    });
}

pub async fn execute_local_http_request(
    target: ProxyTarget,
    method: &str,
    uri: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> ProxyResponsePayload {
    let target_addr = resolve_local_target_addr(target);
    let clean_uri = if uri.is_empty() { "/" } else { uri };

    let connect_res =
        tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&target_addr)).await;

    let mut stream = match connect_res {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return error_response(
                502,
                format!("中继站无法连接本地目标 {target_addr}: {e}"),
            );
        }
        Err(_) => {
            return error_response(504, format!("中继站连接本地目标 {target_addr} 超时 (3s)"));
        }
    };

    let mut request_bytes = Vec::with_capacity(512 + body.len());
    request_bytes.extend_from_slice(format!("{method} {clean_uri} HTTP/1.0\r\n").as_bytes());
    request_bytes.extend_from_slice(format!("Host: {target_addr}\r\n").as_bytes());
    request_bytes.extend_from_slice(b"Connection: close\r\n");

    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "host" | "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        request_bytes.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }

    if !body.is_empty() || matches!(method, "POST" | "PUT" | "PATCH") {
        request_bytes
            .extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    }
    request_bytes.extend_from_slice(b"\r\n");
    request_bytes.extend_from_slice(body);

    if let Err(e) = stream.write_all(&request_bytes).await {
        return error_response(502, format!("向目标 {target_addr} 发送 HTTP 请求失败: {e}"));
    }
    let _ = stream.flush().await;

    let mut raw_resp = Vec::new();
    match tokio::time::timeout(Duration::from_secs(8), stream.read_to_end(&mut raw_resp)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            if raw_resp.is_empty() {
                return error_response(502, format!("读取目标 {target_addr} 响应失败: {e}"));
            }
        }
        Err(_) => {
            if raw_resp.is_empty() {
                return error_response(504, format!("读取目标 {target_addr} 响应超时 (8s)"));
            }
        }
    }

    parse_raw_http_response(&raw_resp)
}

/// Finds the blank line ending the headers. Embedded CGI programs often end
/// their own headers with bare newlines, so CRLF cannot be assumed.
fn find_header_end(raw: &[u8]) -> Option<(usize, usize)> {
    raw.iter().enumerate().find_map(|(index, byte)| {
        if *byte != b'\n' {
            return None;
        }
        let rest = &raw[index + 1..];
        if rest.starts_with(b"\n") {
            Some((index, index + 2))
        } else if rest.starts_with(b"\r\n") {
            Some((index, index + 3))
        } else {
            None
        }
    })
}

fn is_valid_header(name: &str, value: &str) -> bool {
    HeaderName::from_bytes(name.as_bytes()).is_ok() && HeaderValue::from_str(value).is_ok()
}

fn parse_raw_http_response(raw: &[u8]) -> ProxyResponsePayload {
    let header_end = if raw.starts_with(b"HTTP/") {
        find_header_end(raw)
    } else {
        None
    };
    let Some((header_end, body_start)) = header_end else {
        return ProxyResponsePayload {
            status: 200,
            headers: vec![("content-type".to_string(), "application/octet-stream".to_string())],
            body: raw.to_vec(),
        };
    };

    let header_part = String::from_utf8_lossy(&raw[..header_end]);
    let body_part = raw[body_start..].to_vec();

    let mut lines = header_part.lines();
    let status_line = lines.next().unwrap_or("HTTP/1.0 200 OK");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(200);

    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_string();
            let val = v.trim().to_string();
            let lower = key.to_ascii_lowercase();
            if matches!(lower.as_str(), "connection" | "transfer-encoding" | "content-length") {
                continue;
            }
            // CGI programs can print stray lines here, such as a timestamp.
            if !is_valid_header(&key, &val) {
                continue;
            }
            headers.push((key, val));
        }
    }

    ProxyResponsePayload {
        status,
        headers,
        body: body_part,
    }
}

fn error_response(status: u16, msg: String) -> ProxyResponsePayload {
    ProxyResponsePayload {
        status,
        headers: vec![("content-type".to_string(), "text/plain; charset=utf-8".to_string())],
        body: msg.into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_raw_http_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"ok\":true}";
        let parsed = parse_raw_http_response(raw);
        assert_eq!(parsed.status, 200);
        assert_eq!(parsed.headers, vec![("Content-Type".to_string(), "application/json".to_string())]);
        assert_eq!(parsed.body, b"{\"ok\":true}");
    }

    #[test]
    fn test_parse_cgi_response_with_stray_line_and_bare_newlines() {
        // Captured from the L-link device's Boa server and mpm.cgi.
        let raw = b"HTTP/1.0 200 OK\r\nServer: Boa/0.94.13\r\nConnection: close\r\nThu Nov  5 18:00:00 UTC 2026\nContent-type: text/html\n\n\r\n\r\n{\"ok\":true}";
        let parsed = parse_raw_http_response(raw);
        assert_eq!(parsed.status, 200);
        assert_eq!(
            parsed.headers,
            vec![
                ("Server".to_string(), "Boa/0.94.13".to_string()),
                ("Content-type".to_string(), "text/html".to_string()),
            ]
        );
        assert_eq!(parsed.body, b"\r\n\r\n{\"ok\":true}");
    }

    #[test]
    fn test_parse_response_without_status_line_is_body() {
        let parsed = parse_raw_http_response(b"{\"a\":1}\n\n{\"b\":2}");
        assert_eq!(parsed.status, 200);
        assert_eq!(parsed.body, b"{\"a\":1}\n\n{\"b\":2}");
    }
}

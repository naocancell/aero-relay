use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::http::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::transport::{ProxyTarget, RemoteMessage};

pub fn global_proxy_hub() -> &'static ServerProxyHub {
    static HUB: OnceLock<ServerProxyHub> = OnceLock::new();
    HUB.get_or_init(ServerProxyHub::new)
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
    relays: Arc<Mutex<BTreeMap<String, (u64, mpsc::Sender<RemoteMessage>)>>>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<ProxyResponsePayload>>>>,
    tcp_streams: Arc<Mutex<BTreeMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>,
}

impl ServerProxyHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_relay(
        &self,
        relay_id: String,
        connection_id: u64,
        sender: mpsc::Sender<RemoteMessage>,
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

    pub fn push_tcp_stream_data(&self, stream_id: u64, data: Vec<u8>) {
        if let Some(tx) = self
            .tcp_streams
            .lock()
            .expect("proxy hub tcp_streams mutex poisoned")
            .get(&stream_id)
            .cloned()
        {
            let _ = tx.send(data);
        }
    }

    pub fn close_tcp_stream(&self, stream_id: u64) {
        self.tcp_streams
            .lock()
            .expect("proxy hub tcp_streams mutex poisoned")
            .remove(&stream_id);
    }

    pub fn first_active_relay(&self) -> Option<String> {
        let relays = self.relays.lock().expect("proxy hub relays mutex poisoned");
        relays.keys().next().cloned()
    }

    pub async fn open_tcp_stream(
        &self,
        relay_id: &str,
    ) -> Result<(u64, mpsc::Sender<RemoteMessage>, mpsc::UnboundedReceiver<Vec<u8>>), (u16, String)> {
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

        let stream_id = self.next_request_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        self.tcp_streams
            .lock()
            .expect("proxy hub tcp_streams mutex poisoned")
            .insert(stream_id, tx);

        if sender
            .send(RemoteMessage::TcpProxyOpen { stream_id })
            .await
            .is_err()
        {
            self.close_tcp_stream(stream_id);
            return Err((502, format!("向中继站 `{relay_id}` 打开 TCP 代理流失败")));
        }

        Ok((stream_id, sender, rx))
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

        let cmd = RemoteMessage::HttpProxyRequest {
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

pub fn resolve_local_llink_ws_addr() -> String {
    let llink_addr = resolve_local_target_addr(ProxyTarget::LLink);
    let ip = llink_addr.split(':').next().unwrap_or("192.168.0.181");
    format!("{ip}:8000")
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

pub fn spawn_server_llink_tcp_proxy(listen_addr: &str) {
    let listen = listen_addr.to_string();
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(&listen).await {
            Ok(l) => {
                tracing::info!(listen = %listen, "Server L-link tunnel TCP proxy started");
                l
            }
            Err(e) => {
                tracing::warn!(listen = %listen, error = %e, "failed to bind Server L-link tunnel TCP proxy port");
                return;
            }
        };
        loop {
            let (client, _peer) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => continue,
            };
            tokio::spawn(async move {
                let Some(relay_id) = global_proxy_hub().first_active_relay() else {
                    return;
                };
                let Ok((stream_id, tunnel_tx, mut tunnel_rx)) =
                    global_proxy_hub().open_tcp_stream(&relay_id).await
                else {
                    return;
                };
                let (mut r, mut w) = client.into_split();
                let tx_clone = tunnel_tx.clone();
                let c2t = async move {
                    let mut buf = vec![0u8; 16384];
                    loop {
                        match r.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if tx_clone
                                    .send(RemoteMessage::TcpProxyData {
                                        stream_id,
                                        data: buf[..n].to_vec(),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                };
                let t2c = async move {
                    while let Some(data) = tunnel_rx.recv().await {
                        if w.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                };
                tokio::select! {
                    _ = c2t => {}
                    _ = t2c => {}
                }
                global_proxy_hub().close_tcp_stream(stream_id);
                let _ = tunnel_tx
                    .send(RemoteMessage::TcpProxyClose { stream_id })
                    .await;
            });
        }
    });
}

const UPSTREAM_WS_HANDSHAKE: &[u8] = b"GET / HTTP/1.1\r\n\
Host: 192.168.0.181:8000\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\r\n";

pub async fn bridge_llink_websocket(
    relay_id: String,
    mut client_io: TokioIo<hyper::upgrade::Upgraded>,
) {
    if relay_id == "local" {
        let target_addr = resolve_local_llink_ws_addr();
        let Ok(Ok(mut upstream)) =
            tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&target_addr)).await
        else {
            return;
        };
        if upstream.write_all(UPSTREAM_WS_HANDSHAKE).await.is_err() {
            return;
        }
        let mut header_buf = Vec::with_capacity(512);
        let mut tmp = [0u8; 512];
        loop {
            match tokio::time::timeout(Duration::from_secs(3), upstream.read(&mut tmp)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return,
                Ok(Ok(n)) => {
                    header_buf.extend_from_slice(&tmp[..n]);
                    if let Some((_, body_start)) = find_header_end(&header_buf) {
                        if body_start < header_buf.len()
                            && client_io.write_all(&header_buf[body_start..]).await.is_err()
                        {
                            return;
                        }
                        break;
                    }
                    if header_buf.len() > 8192 {
                        return;
                    }
                }
            }
        }
        let _ = tokio::io::copy_bidirectional(&mut client_io, &mut upstream).await;
    } else {
        let Ok((stream_id, tunnel_tx, mut tunnel_rx)) =
            global_proxy_hub().open_tcp_stream(&relay_id).await
        else {
            return;
        };
        if tunnel_tx
            .send(RemoteMessage::TcpProxyData {
                stream_id,
                data: UPSTREAM_WS_HANDSHAKE.to_vec(),
            })
            .await
            .is_err()
        {
            global_proxy_hub().close_tcp_stream(stream_id);
            return;
        }

        // Wait for upstream 101 handshake headers before bridging frames
        let mut header_buf = Vec::with_capacity(512);
        let mut handshake_ok = false;
        while let Ok(Some(chunk)) =
            tokio::time::timeout(Duration::from_secs(5), tunnel_rx.recv()).await
        {
            header_buf.extend_from_slice(&chunk);
            if let Some((_, body_start)) = find_header_end(&header_buf) {
                if body_start < header_buf.len()
                    && client_io.write_all(&header_buf[body_start..]).await.is_err()
                {
                    global_proxy_hub().close_tcp_stream(stream_id);
                    let _ = tunnel_tx
                        .send(RemoteMessage::TcpProxyClose { stream_id })
                        .await;
                    return;
                }
                handshake_ok = true;
                break;
            }
            if header_buf.len() > 8192 {
                break;
            }
        }

        if !handshake_ok {
            global_proxy_hub().close_tcp_stream(stream_id);
            let _ = tunnel_tx
                .send(RemoteMessage::TcpProxyClose { stream_id })
                .await;
            return;
        }

        let (mut client_r, mut client_w) = tokio::io::split(client_io);
        let tx_clone = tunnel_tx.clone();
        let c2t = async move {
            let mut buf = vec![0u8; 16384];
            loop {
                match client_r.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx_clone
                            .send(RemoteMessage::TcpProxyData {
                                stream_id,
                                data: buf[..n].to_vec(),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        };
        let t2c = async move {
            while let Some(data) = tunnel_rx.recv().await {
                if client_w.write_all(&data).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! {
            _ = c2t => {}
            _ = t2c => {}
        }
        global_proxy_hub().close_tcp_stream(stream_id);
        let _ = tunnel_tx
            .send(RemoteMessage::TcpProxyClose { stream_id })
            .await;
    }
}

pub fn compute_websocket_accept(sec_websocket_key: &str) -> String {
    let mut input = Vec::with_capacity(sec_websocket_key.trim().len() + 36);
    input.extend_from_slice(sec_websocket_key.trim().as_bytes());
    input.extend_from_slice(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let digest = sha1_digest(&input);
    base64_encode(&digest)
}

fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;

        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
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

    #[test]
    fn test_compute_websocket_accept_rfc6455() {
        assert_eq!(
            compute_websocket_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }
}

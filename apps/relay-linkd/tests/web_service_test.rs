use std::path::PathBuf;
use std::time::Duration;

use relay_core::config::Config;
use relay_core::model::{Role, SnapshotPayload};
use relay_linkd::http_proxy::{ProxyResponsePayload, global_proxy_hub};
use relay_linkd::web_service;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

async fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf).to_string();

    let first_line = resp.lines().next().unwrap_or_default();
    let status_code: u16 = first_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    (status_code, resp)
}

#[tokio::test]
async fn test_web_service_routes() {
    let (_snapshot_tx, snapshot_rx) = watch::channel(SnapshotPayload::RoleOnly {
        role: Role::Relay,
    });
    let (reload_tx, _reload_rx) = mpsc::channel(4);
    let config = Config {
        relay: None,
        server: None,
    };
    let config_path = PathBuf::from("config.toml");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    unsafe {
        std::env::set_var("RELAY_LINK_WEB_LISTEN", format!("127.0.0.1:{port}"));
    }

    tokio::spawn(async move {
        let _ = web_service::run(
            config,
            config_path,
            Role::Relay,
            snapshot_rx,
            reload_tx,
            None,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 1. 测试静态资源首页 GET /
    let (status, resp) = http_get(port, "/").await;
    assert_eq!(status, 200);
    assert!(resp.contains("text/html"));

    // 2. 测试 GET /api/snapshot
    let (status, resp) = http_get(port, "/api/snapshot").await;
    assert_eq!(status, 200);
    assert!(resp.contains("application/json"));
    assert!(resp.contains("role_only"));

    // 3. 测试 GET /api/config
    let (status, resp) = http_get(port, "/api/config").await;
    assert_eq!(status, 200);
    assert!(resp.contains("application/json"));

    // 4. 测试 GET /api/serial-ports
    let (status, resp) = http_get(port, "/api/serial-ports").await;
    assert_eq!(status, 200);
    assert!(resp.contains("application/json"));

    // 5. 测试通过服务端代理中转访问已连接的中继站 Web 接口与 L 链页面
    let (proxy_tx, mut proxy_rx) = mpsc::channel(8);
    global_proxy_hub().register_relay("relay-test".to_string(), 1, proxy_tx);
    tokio::spawn(async move {
        while let Some(cmd) = proxy_rx.recv().await {
            let body = format!("proxied:{:?}:{}", cmd.target, cmd.uri).into_bytes();
            global_proxy_hub().complete_request(
                cmd.request_id,
                ProxyResponsePayload {
                    status: 200,
                    headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
                    body,
                },
            );
        }
    });

    let (status, resp) = http_get(port, "/proxy/relay/relay-test/api/snapshot").await;
    assert_eq!(status, 200);
    assert!(resp.contains("proxied:RelayWeb:/api/snapshot"));

    let (status, resp) = http_get(port, "/proxy/llink/relay-test/index.html").await;
    assert_eq!(status, 200);
    assert!(resp.contains("proxied:LLink:/index.html"));
    assert!(resp.contains("aero_llink_relay=relay-test"));

    global_proxy_hub().unregister_relay("relay-test", 1);
}

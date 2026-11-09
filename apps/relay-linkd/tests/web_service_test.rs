use std::path::PathBuf;
use std::time::Duration;

use relay_core::config::Config;
use relay_core::model::{Role, SnapshotPayload};
use relay_linkd::http_proxy::{ProxyResponsePayload, global_proxy_hub};
use relay_linkd::web_service;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
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
    let _lock = ENV_LOCK.lock().unwrap();
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
                    headers: vec![
                        ("Content-Type".to_string(), "text/plain".to_string()),
                        ("Thu Nov  5 18".to_string(), "00:00 UTC\nbad".to_string()),
                    ],
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

    // 页面按钮打开的地址以 / 结尾，必须转发到目标根路径，不能落到管理页兜底逻辑
    let (status, resp) = http_get(port, "/proxy/llink/relay-test/").await;
    assert_eq!(status, 200);
    assert!(resp.contains("proxied:LLink:/"));
    assert!(!resp.contains("proxied:LLink:/proxy"));
    assert!(resp.contains("aero_llink_relay=relay-test"));

    let (status, resp) = http_get(port, "/proxy/relay/relay-test/").await;
    assert_eq!(status, 200);
    assert!(resp.contains("proxied:RelayWeb:/"));

    // 以 / 结尾的页面里，相对路径资源会解析到这个地址
    let (status, resp) = http_get(port, "/proxy/llink/relay-test/css/font.css").await;
    assert_eq!(status, 200);
    assert!(resp.contains("proxied:LLink:/css/font.css"));

    global_proxy_hub().unregister_relay("relay-test", 1);
}


async fn http_request(
    port: u16,
    method: &str,
    path: &str,
    auth_header: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let auth_line = if let Some(h) = auth_header {
        format!("Authorization: {h}\r\n")
    } else {
        String::new()
    };
    let (content_type, body_str) = if let Some(b) = body {
        (
            format!("Content-Type: application/json\r\nContent-Length: {}\r\n", b.len()),
            b,
        )
    } else {
        (String::new(), "")
    };
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{auth_line}{content_type}Connection: close\r\n\r\n{body_str}"
    );
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
async fn test_web_service_basic_auth() {
    let _lock = ENV_LOCK.lock().unwrap();
    use relay_core::config::{AuthConfig, RelayConfig};

    let (_snapshot_tx, snapshot_rx) = watch::channel(SnapshotPayload::RoleOnly {
        role: Role::Relay,
    });
    let (reload_tx, _reload_rx) = mpsc::channel(4);
    let config = Config {
        relay: Some(RelayConfig {
            id: "relay-auth".into(),
            server_addr: "127.0.0.1:7443".into(),
            heartbeat_interval_ms: 1000,
            data_timeout_ms: 3000,
            links: vec![],
            video: None,
            turntable: None,
            io_controller: None,
            auth: Some(AuthConfig {
                enabled: true,
                username: "operator".into(),
                password: "op123456".into(),
            }),
        }),
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

    // 1. 无认证请求，预期 401 拦截并返回 WWW-Authenticate 标头
    let (status, resp) = http_request(port, "GET", "/api/snapshot", None, None).await;
    assert_eq!(status, 401);
    assert!(resp.to_ascii_lowercase().contains("www-authenticate: basic"));

    // 2. 错误账密，预期 401
    // base64("admin:wrongpass") = "YWRtaW46d3JvbmdwYXNz"
    let (status, _) = http_request(port, "GET", "/api/snapshot", Some("Basic YWRtaW46d3JvbmdwYXNz"), None).await;
    assert_eq!(status, 401);

    // 3. 超管账号登录（admin / htfp8899）
    // base64("admin:htfp8899") = "YWRtaW46aHRmcDg4OTk="
    let super_auth = "Basic YWRtaW46aHRmcDg4OTk=";
    let (status, resp) = http_request(port, "GET", "/api/snapshot", Some(super_auth), None).await;
    assert_eq!(status, 200);
    assert!(resp.contains("role_only"));

    // 检查超管身份角色信息
    let (status, resp) = http_request(port, "GET", "/api/auth/me", Some(super_auth), None).await;
    assert_eq!(status, 200);
    assert!(resp.contains("\"is_super_admin\":true"));
    assert!(resp.contains("\"super_admin\""));

    // 4. 普通配置账号登录（operator / op123456）
    // base64("operator:op123456") = "b3BlcmF0b3I6b3AxMjM0NTY="
    let op_auth = "Basic b3BlcmF0b3I6b3AxMjM0NTY=";
    let (status, resp) = http_request(port, "GET", "/api/auth/me", Some(op_auth), None).await;
    assert_eq!(status, 200);
    assert!(resp.contains("\"is_super_admin\":false"));
    assert!(resp.contains("\"custom_user\""));

    // 5. 普通账号尝试向 /api/config 修改 auth 配置，必须被 403 Forbidden 拦截！
    let malicious_config = r#"{
        "relay": {
            "id": "relay-auth",
            "server_addr": "127.0.0.1:7443",
            "links": [],
            "auth": {
                "enabled": false,
                "username": "hacker",
                "password": "123"
            }
        },
        "server": null
    }"#;
    let (status, resp) = http_request(port, "POST", "/api/config", Some(op_auth), Some(malicious_config)).await;
    assert_eq!(status, 403);
    assert!(resp.contains("无权修改账号安全配置"));
}

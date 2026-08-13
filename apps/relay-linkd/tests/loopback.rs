use futures_util::{SinkExt, StreamExt};
use relay_linkd::transport::{RemoteCodec, RemoteMessage};
use tokio::io::duplex;
use tokio_util::codec::Framed;

#[tokio::test]
async fn relay_and_server_loopback_preserves_path_identity_and_payloads() {
    let (relay_io, server_io) = duplex(4096);
    let mut relay = Framed::new(relay_io, RemoteCodec::new());
    let mut server = Framed::new(server_io, RemoteCodec::new());

    relay
        .send(RemoteMessage::RelayHello {
            relay_id: "relay-a".into(),
        })
        .await
        .expect("relay hello");
    assert_eq!(
        server.next().await.expect("hello frame").expect("hello"),
        RemoteMessage::RelayHello {
            relay_id: "relay-a".into()
        }
    );

    relay
        .send(RemoteMessage::Heartbeat {
            relay_id: "relay-a".into(),
        })
        .await
        .expect("heartbeat");
    assert_eq!(
        server
            .next()
            .await
            .expect("heartbeat frame")
            .expect("heartbeat"),
        RemoteMessage::Heartbeat {
            relay_id: "relay-a".into()
        }
    );

    relay
        .send(RemoteMessage::AircraftData {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            payload: vec![0x00, 0xEB, 0x90, 0xFF, 0x7B, 0x7D],
        })
        .await
        .expect("aircraft data");
    assert_eq!(
        server
            .next()
            .await
            .expect("aircraft frame")
            .expect("aircraft"),
        RemoteMessage::AircraftData {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            payload: vec![0x00, 0xEB, 0x90, 0xFF, 0x7B, 0x7D],
        }
    );

    server
        .send(RemoteMessage::ActivePath {
            relay_id: "relay-a".into(),
            link_id: Some("udp-1".into()),
        })
        .await
        .expect("active path announcement");
    assert_eq!(
        relay
            .next()
            .await
            .expect("active path frame")
            .expect("active path"),
        RemoteMessage::ActivePath {
            relay_id: "relay-a".into(),
            link_id: Some("udp-1".into()),
        }
    );

    relay
        .send(RemoteMessage::LinkStatus {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            adapter_ready: true,
            last_tx_at: Some(123),
            tx_bytes: 7,
            tx_rate_bytes_per_second: 3.5,
            last_error: None,
        })
        .await
        .expect("link status");
    assert_eq!(
        server
            .next()
            .await
            .expect("link status frame")
            .expect("link status"),
        RemoteMessage::LinkStatus {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            adapter_ready: true,
            last_tx_at: Some(123),
            tx_bytes: 7,
            tx_rate_bytes_per_second: 3.5,
            last_error: None,
        }
    );

    server
        .send(RemoteMessage::ControlData {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            payload: vec![0x00, 0x01, 0xFE, 0xFF],
        })
        .await
        .expect("control data");
    assert_eq!(
        relay.next().await.expect("control frame").expect("control"),
        RemoteMessage::ControlData {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            payload: vec![0x00, 0x01, 0xFE, 0xFF],
        }
    );
}

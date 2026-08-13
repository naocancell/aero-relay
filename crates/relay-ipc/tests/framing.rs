use bytes::{BufMut, BytesMut};
use relay_core::config::Config;
use relay_core::model::{Role, ServerSnapshot, VideoSnapshot, VideoState};
use relay_ipc::{IpcMessage, JsonFrameCodec, SnapshotPayload, fixed_endpoint};
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn fixed_endpoint_matches_the_platform_control_channel() {
    let expected = if cfg!(windows) {
        r"\\.\pipe\relay-link-control"
    } else {
        "/run/relay-link/control.sock"
    };
    assert_eq!(fixed_endpoint(), expected);
}

#[test]
fn fragmented_frame_is_emitted_only_after_the_payload_is_complete() {
    let message = IpcMessage::GetSnapshot;
    let mut encoder = JsonFrameCodec::new();
    let mut encoded = BytesMut::new();
    encoder
        .encode(message.clone(), &mut encoded)
        .expect("encode");

    let split = encoded.len() - 2;
    let mut input = BytesMut::from(&encoded[..split]);
    let mut decoder = JsonFrameCodec::new();
    assert!(
        decoder
            .decode(&mut input)
            .expect("partial frame is valid")
            .is_none()
    );

    input.extend_from_slice(&encoded[split..]);
    assert_eq!(
        decoder.decode(&mut input).expect("complete frame"),
        Some(message)
    );
}

#[test]
fn coalesced_frames_are_decoded_independently() {
    let first = IpcMessage::Subscribe;
    let second = IpcMessage::Error {
        message: "background service unavailable".into(),
    };
    let mut encoder = JsonFrameCodec::new();
    let mut input = BytesMut::new();
    encoder
        .encode(first.clone(), &mut input)
        .expect("encode first");
    encoder
        .encode(second.clone(), &mut input)
        .expect("encode second");

    let mut decoder = JsonFrameCodec::new();
    assert_eq!(
        decoder.decode(&mut input).expect("first frame"),
        Some(first)
    );
    assert_eq!(
        decoder.decode(&mut input).expect("second frame"),
        Some(second)
    );
    assert!(
        decoder
            .decode(&mut input)
            .expect("no third frame")
            .is_none()
    );
}

#[test]
fn invalid_json_is_rejected_as_invalid_data() {
    let mut input = BytesMut::new();
    input.put_u32(3);
    input.extend_from_slice(&[0xff, 0xfe, 0xfd]);

    let mut decoder = JsonFrameCodec::new();
    let error = decoder
        .decode(&mut input)
        .expect_err("invalid JSON must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn valid_snapshot_message_round_trips() {
    let message = IpcMessage::Snapshot {
        snapshot: SnapshotPayload::RoleOnly { role: Role::Relay },
    };
    let mut encoder = JsonFrameCodec::new();
    let mut encoded = BytesMut::new();
    encoder
        .encode(message.clone(), &mut encoded)
        .expect("encode");

    let mut decoder = JsonFrameCodec::new();
    assert_eq!(decoder.decode(&mut encoded).expect("decode"), Some(message));
}

#[test]
fn frame_larger_than_limit_is_rejected() {
    let mut input = BytesMut::new();
    input.put_u32((relay_ipc::MAX_FRAME_LENGTH + 1) as u32);
    input.resize(input.len() + relay_ipc::MAX_FRAME_LENGTH + 1, 0);

    let mut decoder = JsonFrameCodec::new();
    let error = decoder
        .decode(&mut input)
        .expect_err("oversized frame must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn snapshot_role_discriminator_uses_snake_case() {
    let snapshot = SnapshotPayload::Server(ServerSnapshot {
        role: Role::Server,
        ground_station_connected: false,
        active_path: None,
        relays: Vec::new(),
        switches: Vec::new(),
        uptime_seconds: 0,
        recent_error: None,
        video: VideoSnapshot::default(),
    });

    let value = serde_json::to_value(snapshot).expect("snapshot should serialize");

    assert_eq!(value["role"], "server");
}

#[test]
fn video_health_is_included_in_the_ipc_snapshot_without_media_urls() {
    let snapshot = SnapshotPayload::Server(ServerSnapshot {
        role: Role::Server,
        ground_station_connected: false,
        active_path: None,
        relays: Vec::new(),
        switches: Vec::new(),
        uptime_seconds: 0,
        recent_error: None,
        video: VideoSnapshot {
            state: VideoState::Online,
            worker_running: true,
            stream_ready: true,
            restart_count: 2,
            last_progress_at: Some("1234".into()),
            last_error: None,
        },
    });

    let value = serde_json::to_value(snapshot).expect("snapshot should serialize");

    assert_eq!(value["state"]["video"]["state"], "online");
    assert_eq!(value["state"]["video"]["worker_running"], true);
    assert_eq!(value["state"]["video"]["stream_ready"], true);
    assert!(value["state"]["video"].get("source_url").is_none());
    assert!(value["state"]["video"].get("publish_url").is_none());
}

#[test]
fn config_save_message_round_trips() {
    let messages = [
        IpcMessage::SaveConfig {
            config: Box::new(Config {
                relay: None,
                server: None,
            }),
        },
        IpcMessage::ConfigSaved,
    ];

    for message in messages {
        let mut encoder = JsonFrameCodec::new();
        let mut encoded = BytesMut::new();
        encoder
            .encode(message.clone(), &mut encoded)
            .expect("encode config message");

        let mut decoder = JsonFrameCodec::new();
        assert_eq!(
            decoder.decode(&mut encoded).expect("decode config message"),
            Some(message)
        );
    }
}

#[test]
fn serial_port_and_restart_messages_round_trip() {
    let messages = [
        IpcMessage::GetSerialPorts,
        IpcMessage::SerialPorts {
            ports: vec!["COM3".into()],
        },
        IpcMessage::RestartBackend,
        IpcMessage::BackendRestarted,
    ];

    for message in messages {
        let mut encoder = JsonFrameCodec::new();
        let mut encoded = BytesMut::new();
        encoder
            .encode(message.clone(), &mut encoded)
            .expect("encode serial/restart message");

        let mut decoder = JsonFrameCodec::new();
        assert_eq!(
            decoder
                .decode(&mut encoded)
                .expect("decode serial/restart message"),
            Some(message)
        );
    }
}

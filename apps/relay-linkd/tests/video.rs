use std::path::PathBuf;
use std::time::Duration;

use relay_core::config::RelayVideoConfig;
use relay_linkd::video::{
    FfmpegProgressTracker, MediaToolPaths, ffmpeg_arguments, media_mtx_path_is_ready, retry_delay,
};

fn relay_video_config() -> RelayVideoConfig {
    RelayVideoConfig {
        source_url: "rtsp://camera.example/live".into(),
        publish_url: "rtmp://server.example/live/uav-1".into(),
        startup_timeout_ms: 15_000,
        stall_timeout_ms: 10_000,
        restart_max_delay_ms: 30_000,
    }
}

#[test]
fn media_tools_are_resolved_relative_to_the_daemon_executable() {
    let executable = if cfg!(windows) {
        PathBuf::from(r"C:\Program Files\Relay Link\relay-linkd.exe")
    } else {
        PathBuf::from("/opt/relay-link/relay-linkd")
    };
    let paths = MediaToolPaths::from_executable(&executable).expect("derive media paths");
    let install_directory = executable.parent().expect("executable parent");
    let executable_suffix = if cfg!(windows) { ".exe" } else { "" };

    assert_eq!(
        paths.ffmpeg,
        install_directory
            .join("media")
            .join("ffmpeg")
            .join(format!("ffmpeg{executable_suffix}"))
    );
    assert_eq!(
        paths.mediamtx,
        install_directory
            .join("media")
            .join("mediamtx")
            .join(format!("mediamtx{executable_suffix}"))
    );
    assert_eq!(
        paths.mediamtx_config,
        install_directory.join("config").join("mediamtx.yml")
    );
}

#[test]
fn ffmpeg_uses_direct_rtsp_to_rtmp_remuxing_with_bounded_socket_io() {
    let arguments = ffmpeg_arguments(&relay_video_config())
        .into_iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert_eq!(
        arguments,
        vec![
            "-hide_banner",
            "-nostats",
            "-loglevel",
            "warning",
            "-stats_period",
            "1",
            "-progress",
            "pipe:1",
            "-rtsp_transport",
            "tcp",
            "-timeout",
            "10000000",
            "-i",
            "rtsp://camera.example/live",
            "-c",
            "copy",
            "-rw_timeout",
            "10000000",
            "-f",
            "flv",
            "rtmp://server.example/live/uav-1",
        ]
    );
}

#[test]
fn retry_delay_is_exponential_and_capped() {
    let cap = Duration::from_secs(5);

    assert_eq!(retry_delay(0, cap), Duration::from_secs(1));
    assert_eq!(retry_delay(1, cap), Duration::from_secs(1));
    assert_eq!(retry_delay(2, cap), Duration::from_secs(2));
    assert_eq!(retry_delay(3, cap), Duration::from_secs(4));
    assert_eq!(retry_delay(4, cap), Duration::from_secs(5));
    assert_eq!(retry_delay(100, cap), Duration::from_secs(5));
}

#[test]
fn mediamtx_path_list_reports_only_the_configured_ready_stream() {
    let body = br#"{
        "itemCount": 2,
        "pageCount": 1,
        "items": [
            { "name": "uav-1", "ready": true },
            { "name": "other", "ready": false }
        ]
    }"#;

    assert!(media_mtx_path_is_ready(body, "uav-1").expect("valid response"));
    assert!(!media_mtx_path_is_ready(body, "other").expect("valid response"));
    assert!(!media_mtx_path_is_ready(body, "missing").expect("valid response"));
}

fn feed_block(
    tracker: &mut FfmpegProgressTracker,
    total_size: &str,
    out_time_us: &str,
) -> Option<bool> {
    let block = [
        "frame=0".to_string(),
        format!("total_size={total_size}"),
        format!("out_time_us={out_time_us}"),
        "speed=1.0x".to_string(),
        "progress=continue".to_string(),
    ];
    let mut result = None;
    for line in block {
        result = tracker.observe_line(&line);
    }
    result
}

#[test]
fn frozen_ffmpeg_progress_blocks_are_not_liveness() {
    let mut tracker = FfmpegProgressTracker::default();
    assert_eq!(feed_block(&mut tracker, "9558961", "10200000"), Some(true));
    // FFmpeg 7+ keeps printing progress=continue while its RTMP socket is
    // blocked; the byte counter is frozen, so this must not count as alive.
    assert_eq!(feed_block(&mut tracker, "9558961", "10200000"), Some(false));
    // The output clock may creep a little after the socket blocked.
    assert_eq!(feed_block(&mut tracker, "9558961", "10280000"), Some(false));
    assert_eq!(feed_block(&mut tracker, "9600000", "10320000"), Some(true));
}

#[test]
fn ffmpeg_progress_without_output_bytes_is_not_liveness() {
    let mut tracker = FfmpegProgressTracker::default();
    assert_eq!(feed_block(&mut tracker, "0", "0"), Some(false));
    assert_eq!(feed_block(&mut tracker, "N/A", "N/A"), Some(false));
}

#[test]
fn ffmpeg_output_time_is_used_only_when_byte_count_is_unavailable() {
    let mut tracker = FfmpegProgressTracker::default();
    assert_eq!(feed_block(&mut tracker, "N/A", "1000000"), Some(true));
    assert_eq!(feed_block(&mut tracker, "N/A", "1000000"), Some(false));
    assert_eq!(feed_block(&mut tracker, "N/A", "2000000"), Some(true));
}

#[test]
fn ffmpeg_progress_tracker_ignores_non_progress_lines() {
    let mut tracker = FfmpegProgressTracker::default();
    assert_eq!(tracker.observe_line("total_size=100"), None);
    assert_eq!(tracker.observe_line("garbage"), None);
    assert_eq!(tracker.observe_line("progress=continue"), Some(true));
}

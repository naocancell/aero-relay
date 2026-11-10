use std::{
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use relay_core::{
    config::{RelayVideoConfig, ServerVideoConfig},
    model::{VIDEO_OWNERSHIP_LEASE_TIMEOUT, VideoSnapshot, VideoState},
};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, ChildStdout, Command},
    sync::watch,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};
use tracing::{debug, warn};

const FFMPEG_DIRECTORY: &str = "ffmpeg";
const MEDIAMTX_DIRECTORY: &str = "mediamtx";
const MEDIAMTX_CONFIG_FILE: &str = "mediamtx.yml";
const SUPERVISOR_TICK: Duration = Duration::from_millis(250);
const MAX_CAPTURED_WORKER_OUTPUT: usize = 512;
/// Lower bound for FFmpeg's own socket I/O timeouts so that a tiny configured
/// stall timeout cannot make normal RTSP/RTMP round trips fail.
const MIN_FFMPEG_IO_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a media worker gets to exit after SIGTERM before the whole process
/// group is killed. A graceful exit lets FFmpeg send RTSP TEARDOWN so cameras
/// with a small session limit do not accumulate orphaned sessions.
#[cfg(unix)]
const GRACEFUL_STOP_TIMEOUT: Duration = Duration::from_millis(1500);

/// Fixed, executable-relative paths for the media tools. The service never
/// accepts an arbitrary executable path from TOML or from the management UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaToolPaths {
    pub ffmpeg: PathBuf,
    pub mediamtx: PathBuf,
    pub mediamtx_config: PathBuf,
}

impl MediaToolPaths {
    pub fn from_executable(executable_path: impl AsRef<Path>) -> io::Result<Self> {
        let executable_path = executable_path.as_ref();
        let install_directory = executable_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "relay-linkd executable has no parent directory",
            )
        })?;
        let executable_suffix = if cfg!(windows) { ".exe" } else { "" };
        Ok(Self {
            ffmpeg: install_directory
                .join("media")
                .join(FFMPEG_DIRECTORY)
                .join(format!("ffmpeg{executable_suffix}")),
            mediamtx: install_directory
                .join("media")
                .join(MEDIAMTX_DIRECTORY)
                .join(format!("mediamtx{executable_suffix}")),
            mediamtx_config: install_directory.join("config").join(MEDIAMTX_CONFIG_FILE),
        })
    }

    fn from_current_executable() -> io::Result<Self> {
        Self::from_executable(std::env::current_exe()?)
    }
}

/// Internal health keeps a monotonic timestamp for supervision while its public
/// snapshot uses epoch milliseconds so the UI can display it directly.
#[derive(Debug)]
struct VideoHealthState {
    snapshot: VideoSnapshot,
    last_liveness_at: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct VideoHealth {
    inner: Arc<Mutex<VideoHealthState>>,
}

impl VideoHealth {
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VideoHealthState {
                snapshot: VideoSnapshot::default(),
                last_liveness_at: None,
            })),
        }
    }

    pub fn configured() -> Self {
        let health = Self::disabled();
        health.update(|state| state.snapshot.state = VideoState::Starting);
        health
    }

    pub fn snapshot(&self) -> VideoSnapshot {
        self.lock().snapshot.clone()
    }

    fn mark_standby(&self) {
        self.update(|state| {
            state.snapshot.state = VideoState::Standby;
            state.snapshot.worker_running = false;
            state.snapshot.stream_ready = false;
            state.snapshot.restart_count = 0;
            state.snapshot.last_error = None;
            state.last_liveness_at = None;
        });
    }

    fn mark_starting(&self) {
        self.update(|state| {
            state.snapshot.state = VideoState::Starting;
            state.snapshot.worker_running = true;
            state.snapshot.stream_ready = false;
            state.snapshot.last_error = None;
            state.last_liveness_at = None;
        });
    }

    fn mark_liveness(&self, video_state: VideoState, stream_ready: bool) {
        self.update(|state| {
            state.snapshot.state = video_state;
            state.snapshot.worker_running = true;
            state.snapshot.stream_ready = stream_ready;
            state.snapshot.last_progress_at = Some(unix_timestamp_ms().to_string());
            state.snapshot.last_error = None;
            state.last_liveness_at = Some(Instant::now());
        });
    }

    fn mark_transient_error(&self, message: String) {
        self.update(|state| state.snapshot.last_error = Some(message));
    }

    fn mark_stalled(&self, message: String) {
        self.update(|state| {
            state.snapshot.state = VideoState::Stalled;
            state.snapshot.stream_ready = false;
            state.snapshot.last_error = Some(message);
        });
    }

    fn mark_backoff(&self, restart_count: u64, message: String) {
        self.update(|state| {
            state.snapshot.state = VideoState::Backoff;
            state.snapshot.worker_running = false;
            state.snapshot.stream_ready = false;
            state.snapshot.restart_count = restart_count;
            state.snapshot.last_error = Some(message);
        });
    }

    fn mark_stopped(&self) {
        self.update(|state| {
            state.snapshot.worker_running = false;
            state.snapshot.stream_ready = false;
        });
    }

    fn liveness_age(&self, now: Instant) -> Option<Duration> {
        self.lock()
            .last_liveness_at
            .and_then(|then| now.checked_duration_since(then))
    }

    fn update(&self, update: impl FnOnce(&mut VideoHealthState)) {
        update(&mut self.lock());
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VideoHealthState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// FFmpeg socket I/O timeout in microseconds, derived from the stall timeout.
fn ffmpeg_io_timeout_us(config: &RelayVideoConfig) -> String {
    Duration::from_millis(config.stall_timeout_ms)
        .max(MIN_FFMPEG_IO_TIMEOUT)
        .as_micros()
        .to_string()
}

/// `-timeout` bounds RTSP socket reads from the camera and `-rw_timeout`
/// bounds RTMP socket writes to the server. Without them FFmpeg blocks forever
/// on a frozen camera session or a black-holed uplink while still printing
/// `progress=continue`, so the process never exits by itself.
pub fn ffmpeg_arguments(config: &RelayVideoConfig) -> Vec<OsString> {
    let io_timeout_us = ffmpeg_io_timeout_us(config);
    [
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
    ]
    .into_iter()
    .map(OsString::from)
    .chain(std::iter::once(OsString::from(&io_timeout_us)))
    .chain(std::iter::once(OsString::from("-i")))
    .chain(std::iter::once(OsString::from(&config.source_url)))
    .chain(
        ["-c", "copy", "-rw_timeout"]
            .into_iter()
            .map(OsString::from),
    )
    .chain(std::iter::once(OsString::from(&io_timeout_us)))
    .chain(["-f", "flv"].into_iter().map(OsString::from))
    .chain(std::iter::once(OsString::from(&config.publish_url)))
    .collect()
}

/// Tracks FFmpeg `-progress` blocks. FFmpeg 7+ emits a `progress=continue`
/// block every stats period even when no media is moving, so a block only
/// counts as liveness when the muxed byte count (or, if FFmpeg cannot report
/// bytes, the output time) has advanced beyond everything seen before.
#[derive(Debug, Default)]
pub struct FfmpegProgressTracker {
    block_total_size: Option<u64>,
    block_out_time_us: Option<i64>,
    max_total_size: u64,
    max_out_time_us: i64,
}

impl FfmpegProgressTracker {
    /// Feed one `key=value` line. Returns `Some(advanced)` when a progress
    /// block ends and `None` for every other line.
    pub fn observe_line(&mut self, line: &str) -> Option<bool> {
        let (key, value) = line.trim().split_once('=')?;
        let value = value.trim();
        match key {
            "total_size" => self.block_total_size = value.parse().ok(),
            "out_time_us" => self.block_out_time_us = value.parse().ok(),
            "progress" => {
                let total_size = self.block_total_size.take();
                let out_time_us = self.block_out_time_us.take();
                let size_advanced = total_size.is_some_and(|size| size > self.max_total_size);
                let time_advanced = out_time_us.is_some_and(|time| time > self.max_out_time_us);
                if let Some(size) = total_size {
                    self.max_total_size = self.max_total_size.max(size);
                }
                if let Some(time) = out_time_us {
                    self.max_out_time_us = self.max_out_time_us.max(time);
                }
                // Bytes are authoritative: with `-c copy` they grow with every
                // packet, while the output clock can creep for a moment after
                // the RTMP socket has already blocked.
                return Some(if total_size.is_some() {
                    size_advanced
                } else {
                    time_advanced
                });
            }
            _ => {}
        }
        None
    }
}

/// The first retry is one second. Subsequent failures double the delay until
/// the configured cap is reached, without overflowing for a long-lived worker.
pub fn retry_delay(restart_count: u64, max_delay: Duration) -> Duration {
    let exponent = restart_count.saturating_sub(1).min(20) as u32;
    let seconds = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    Duration::from_secs(seconds).min(max_delay)
}

#[derive(Debug, Deserialize)]
struct MediaMtxPathList {
    #[serde(default)]
    items: Vec<MediaMtxPath>,
}

#[derive(Debug, Deserialize)]
struct MediaMtxPath {
    name: String,
    #[serde(default)]
    ready: bool,
}

pub fn media_mtx_path_is_ready(body: &[u8], stream_path: &str) -> Result<bool, serde_json::Error> {
    let paths: MediaMtxPathList = serde_json::from_slice(body)?;
    Ok(paths
        .items
        .into_iter()
        .any(|path| path.name == stream_path && path.ready))
}

/// Run the relay-side worker until the parent runtime shuts down. A worker
/// failure is isolated from aircraft/control forwarding and is retried in place.
pub async fn supervise_relay_video(
    config: RelayVideoConfig,
    health: VideoHealth,
    ownership: watch::Receiver<Option<Instant>>,
    shutdown: watch::Receiver<bool>,
) {
    let paths = match MediaToolPaths::from_current_executable() {
        Ok(paths) => paths,
        Err(error) => {
            health.mark_backoff(1, format!("unable to resolve media tool paths: {error}"));
            return;
        }
    };
    supervise_relay_video_with_paths(
        config,
        paths,
        health,
        ownership,
        VIDEO_OWNERSHIP_LEASE_TIMEOUT,
        shutdown,
    )
    .await;
}

/// Run the server-side MediaMTX worker until the parent runtime shuts down.
/// MediaMTX's local API is the source of truth for whether the expected stream
/// has actually reached the server.
pub async fn supervise_server_video(
    config: ServerVideoConfig,
    health: VideoHealth,
    shutdown: watch::Receiver<bool>,
) {
    let paths = match MediaToolPaths::from_current_executable() {
        Ok(paths) => paths,
        Err(error) => {
            health.mark_backoff(1, format!("unable to resolve media tool paths: {error}"));
            return;
        }
    };
    supervise_server_video_with_paths(config, paths, health, shutdown).await;
}

async fn supervise_relay_video_with_paths(
    config: RelayVideoConfig,
    paths: MediaToolPaths,
    health: VideoHealth,
    mut ownership: watch::Receiver<Option<Instant>>,
    ownership_lease_timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut restart_count = 0_u64;
    loop {
        if *shutdown.borrow() {
            health.mark_stopped();
            return;
        }
        if !has_current_video_ownership(&ownership, ownership_lease_timeout) {
            restart_count = 0;
            health.mark_standby();
            tokio::select! {
                changed = ownership.changed() => {
                    if changed.is_err() {
                        health.mark_stopped();
                        return;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        health.mark_stopped();
                        return;
                    }
                }
            }
            continue;
        }

        let run_started_at = Instant::now();
        match run_ffmpeg_once(
            &config,
            &paths,
            &health,
            &mut ownership,
            ownership_lease_timeout,
            &mut shutdown,
        )
        .await
        {
            WorkerOutcome::Shutdown => {
                health.mark_stopped();
                return;
            }
            WorkerOutcome::Standby => {
                restart_count = 0;
                health.mark_standby();
            }
            WorkerOutcome::Restart(reason) => {
                if run_started_at.elapsed() >= Duration::from_secs(30) {
                    restart_count = 0;
                }
                restart_count = restart_count.saturating_add(1);
                let delay = retry_delay(
                    restart_count,
                    Duration::from_millis(config.restart_max_delay_ms),
                );
                let message = format!("{reason}; retrying in {} ms", delay.as_millis());
                health.mark_backoff(restart_count, message.clone());
                warn!(restart_count, retry_delay_ms = delay.as_millis(), reason = %reason, "relay video worker will be restarted");
                match wait_for_relay_retry(
                    delay,
                    &mut ownership,
                    ownership_lease_timeout,
                    &mut shutdown,
                )
                .await
                {
                    RelayRetryOutcome::Retry => {}
                    RelayRetryOutcome::Standby => {
                        restart_count = 0;
                        health.mark_standby();
                    }
                    RelayRetryOutcome::Shutdown => {
                        health.mark_stopped();
                        return;
                    }
                }
            }
        }
    }
}

async fn supervise_server_video_with_paths(
    config: ServerVideoConfig,
    paths: MediaToolPaths,
    health: VideoHealth,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut restart_count = 0_u64;
    loop {
        if *shutdown.borrow() {
            health.mark_stopped();
            return;
        }

        let run_started_at = Instant::now();
        match run_mediamtx_once(&config, &paths, &health, &mut shutdown).await {
            WorkerOutcome::Shutdown => {
                health.mark_stopped();
                return;
            }
            WorkerOutcome::Standby => unreachable!("MediaMTX is not gated by relay ownership"),
            WorkerOutcome::Restart(reason) => {
                if run_started_at.elapsed() >= Duration::from_secs(30) {
                    restart_count = 0;
                }
                restart_count = restart_count.saturating_add(1);
                let delay = retry_delay(
                    restart_count,
                    Duration::from_millis(config.restart_max_delay_ms),
                );
                let message = format!("{reason}; retrying in {} ms", delay.as_millis());
                health.mark_backoff(restart_count, message.clone());
                warn!(restart_count, retry_delay_ms = delay.as_millis(), reason = %reason, "MediaMTX video worker will be restarted");
                if wait_for_shutdown(delay, &mut shutdown).await {
                    health.mark_stopped();
                    return;
                }
            }
        }
    }
}

enum WorkerOutcome {
    Shutdown,
    Standby,
    Restart(String),
}

enum RelayRetryOutcome {
    Retry,
    Standby,
    Shutdown,
}

async fn run_ffmpeg_once(
    config: &RelayVideoConfig,
    paths: &MediaToolPaths,
    health: &VideoHealth,
    ownership: &mut watch::Receiver<Option<Instant>>,
    ownership_lease_timeout: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> WorkerOutcome {
    if !has_current_video_ownership(ownership, ownership_lease_timeout) {
        return WorkerOutcome::Standby;
    }
    if let Err(error) = ensure_regular_file(&paths.ffmpeg, "FFmpeg executable") {
        return WorkerOutcome::Restart(error.to_string());
    }
    let managed_child = {
        let ownership_guard = ownership.borrow();
        if !video_ownership_is_current(*ownership_guard, ownership_lease_timeout, Instant::now()) {
            return WorkerOutcome::Standby;
        }
        health.mark_starting();
        let mut command = managed_command(&paths.ffmpeg);
        command.args(ffmpeg_arguments(config));
        ManagedChild::spawn(command)
    };
    let mut managed_child = match managed_child {
        Ok(child) => child,
        Err(error) => return WorkerOutcome::Restart(format!("failed to start FFmpeg: {error}")),
    };
    if !has_current_video_ownership(ownership, ownership_lease_timeout) {
        managed_child.kill_and_wait().await;
        return WorkerOutcome::Standby;
    }
    let Some(stdout) = managed_child.child.stdout.take() else {
        managed_child.kill_and_wait().await;
        return WorkerOutcome::Restart("FFmpeg progress pipe was not available".into());
    };
    let latest_output = Arc::new(Mutex::new(None));
    let progress_task = spawn_ffmpeg_progress_drain(stdout, health.clone());
    let stderr_task = managed_child
        .child
        .stderr
        .take()
        .map(|stderr| spawn_worker_output_drain(stderr, latest_output.clone()));
    let started_at = Instant::now();
    let mut ticker = time::interval(SUPERVISOR_TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let outcome = loop {
        let Some(ownership_deadline) = video_ownership_deadline(ownership, ownership_lease_timeout)
        else {
            managed_child.kill_and_wait().await;
            break WorkerOutcome::Standby;
        };
        tokio::select! {
            status = managed_child.child.wait() => {
                break WorkerOutcome::Restart(child_exit_reason("FFmpeg", status, &latest_output));
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Shutdown;
                }
            }
            changed = ownership.changed() => {
                if changed.is_err()
                    || !has_current_video_ownership(ownership, ownership_lease_timeout)
                {
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Standby;
                }
            }
            _ = time::sleep_until(time::Instant::from_std(ownership_deadline)) => {
                if !has_current_video_ownership(ownership, ownership_lease_timeout) {
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Standby;
                }
            }
            _ = ticker.tick() => {
                let now = Instant::now();
                let liveness_age = health.liveness_age(now);
                if liveness_age.is_none()
                    && now.duration_since(started_at) >= Duration::from_millis(config.startup_timeout_ms)
                {
                    let reason = format!(
                        "FFmpeg did not report progress within {} ms",
                        config.startup_timeout_ms
                    );
                    health.mark_stalled(reason.clone());
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Restart(reason);
                }
                if let Some(age) = liveness_age
                    && age >= Duration::from_millis(config.stall_timeout_ms)
                {
                    let reason = format!(
                        "FFmpeg media output stopped advancing for {} ms",
                        config.stall_timeout_ms
                    );
                    health.mark_stalled(reason.clone());
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Restart(reason);
                }
            }
        }
    };
    progress_task.abort();
    if let Some(task) = stderr_task {
        task.abort();
    }
    outcome
}

async fn run_mediamtx_once(
    config: &ServerVideoConfig,
    paths: &MediaToolPaths,
    health: &VideoHealth,
    shutdown: &mut watch::Receiver<bool>,
) -> WorkerOutcome {
    if let Err(error) = ensure_regular_file(&paths.mediamtx, "MediaMTX executable") {
        return WorkerOutcome::Restart(error.to_string());
    }
    if let Err(error) = ensure_regular_file(&paths.mediamtx_config, "MediaMTX configuration") {
        return WorkerOutcome::Restart(error.to_string());
    }

    health.mark_starting();
    let mut command = managed_command(&paths.mediamtx);
    command.arg(&paths.mediamtx_config);
    if let Some(config_directory) = paths.mediamtx_config.parent() {
        command.current_dir(config_directory);
    }
    let mut managed_child = match ManagedChild::spawn(command) {
        Ok(child) => child,
        Err(error) => return WorkerOutcome::Restart(format!("failed to start MediaMTX: {error}")),
    };
    let latest_output = Arc::new(Mutex::new(None));
    let stdout_task = managed_child
        .child
        .stdout
        .take()
        .map(|stdout| spawn_worker_output_drain(stdout, latest_output.clone()));
    let stderr_task = managed_child
        .child
        .stderr
        .take()
        .map(|stderr| spawn_worker_output_drain(stderr, latest_output.clone()));
    let started_at = Instant::now();
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let api_timeout = poll_interval
        .max(Duration::from_millis(250))
        .min(Duration::from_secs(2));
    let mut ticker = time::interval(poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let outcome = loop {
        tokio::select! {
            status = managed_child.child.wait() => {
                break WorkerOutcome::Restart(child_exit_reason("MediaMTX", status, &latest_output));
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    managed_child.kill_and_wait().await;
                    break WorkerOutcome::Shutdown;
                }
            }
            _ = ticker.tick() => {
                match media_mtx_path_is_ready_from_api(
                    &config.control_api_addr,
                    &config.stream_path,
                    api_timeout,
                ).await {
                    Ok(stream_ready) => {
                        health.mark_liveness(
                            if stream_ready {
                                VideoState::Online
                            } else {
                                VideoState::WaitingForPublisher
                            },
                            stream_ready,
                        );
                    }
                    Err(error) => {
                        let message = format!("MediaMTX local API check failed: {error}");
                        health.mark_transient_error(message);
                        let now = Instant::now();
                        let unhealthy_for = health
                            .liveness_age(now)
                            .unwrap_or_else(|| now.duration_since(started_at));
                        let allowed = if health.liveness_age(now).is_some() {
                            config.stall_timeout_ms
                        } else {
                            config.startup_timeout_ms
                        };
                        if unhealthy_for >= Duration::from_millis(allowed) {
                            let reason = format!("MediaMTX local API was unavailable for {allowed} ms");
                            health.mark_stalled(reason.clone());
                            managed_child.kill_and_wait().await;
                            break WorkerOutcome::Restart(reason);
                        }
                    }
                }
            }
        }
    };
    if let Some(task) = stdout_task {
        task.abort();
    }
    if let Some(task) = stderr_task {
        task.abort();
    }
    outcome
}

fn managed_command(executable: &Path) -> Command {
    let mut command = Command::new(executable);
    command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

struct ManagedChild {
    child: Child,
    #[cfg(unix)]
    process_group_id: i32,
    #[cfg(windows)]
    _job: WindowsJob,
}

impl ManagedChild {
    fn spawn(mut command: Command) -> io::Result<Self> {
        #[cfg(windows)]
        {
            let mut child = command.spawn()?;
            match WindowsJob::attach(&child) {
                Ok(job) => Ok(Self { child, _job: job }),
                Err(error) => {
                    let _ = child.start_kill();
                    Err(error)
                }
            }
        }
        #[cfg(unix)]
        {
            command.process_group(0);
            let child = command.spawn()?;
            let process_group_id = child.id().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "media worker exited before its Unix process group could be recorded",
                )
            })? as i32;
            Ok(Self {
                child,
                process_group_id,
            })
        }
        #[cfg(all(not(windows), not(unix)))]
        {
            Ok(Self {
                child: command.spawn()?,
            })
        }
    }

    async fn kill_and_wait(&mut self) {
        #[cfg(unix)]
        {
            if let Err(error) = signal_process_group(self.process_group_id, libc::SIGTERM) {
                debug!(%error, "failed to request graceful media worker shutdown");
            }
            if time::timeout(GRACEFUL_STOP_TIMEOUT, self.child.wait())
                .await
                .is_err()
            {
                debug!("media worker ignored SIGTERM; killing its process group");
            }
            // Also reaps any helper processes left in the group.
            if let Err(error) = terminate_process_group(self.process_group_id) {
                debug!(%error, "failed to request media worker process-group shutdown");
            }
        }
        if let Err(error) = self.child.start_kill() {
            debug!(%error, "failed to request media worker shutdown");
        }
        if let Err(error) = self.child.wait().await {
            debug!(%error, "failed to reap media worker");
        }
    }
}

#[cfg(unix)]
impl Drop for ManagedChild {
    fn drop(&mut self) {
        let _ = terminate_process_group(self.process_group_id);
    }
}

#[cfg(unix)]
fn terminate_process_group(process_group_id: i32) -> io::Result<()> {
    signal_process_group(process_group_id, libc::SIGKILL)
}

#[cfg(unix)]
fn signal_process_group(process_group_id: i32, signal: libc::c_int) -> io::Result<()> {
    let result = unsafe { libc::kill(-process_group_id, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(windows)]
struct WindowsJob {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
unsafe impl Send for WindowsJob {}

#[cfg(windows)]
impl WindowsJob {
    fn attach(child: &Child) -> io::Result<Self> {
        use std::ffi::c_void;
        use windows_sys::Win32::{
            Foundation::HANDLE,
            System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            },
        };

        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self { handle };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                job.handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
                std::mem::size_of_val(&limits) as u32,
            )
        };
        if configured == 0 {
            return Err(io::Error::last_os_error());
        }
        let process_handle = child.raw_handle().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "media worker exited before it could be assigned to a Windows Job Object",
            )
        })? as HANDLE;
        if unsafe { AssignProcessToJobObject(job.handle, process_handle) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        unsafe {
            let _ = windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

fn spawn_ffmpeg_progress_drain(stdout: ChildStdout, health: VideoHealth) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut tracker = FfmpegProgressTracker::default();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if tracker.observe_line(&line) == Some(true) {
                        health.mark_liveness(VideoState::Publishing, true);
                    }
                }
                Ok(None) | Err(_) => return,
            }
        }
    })
}

fn spawn_worker_output_drain<R>(
    reader: R,
    latest_output: Arc<Mutex<Option<String>>>,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let line = redact_media_urls(&line);
                    let line = line
                        .chars()
                        .take(MAX_CAPTURED_WORKER_OUTPUT)
                        .collect::<String>();
                    if !line.trim().is_empty() {
                        *latest_output
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(line);
                    }
                }
                Ok(None) | Err(_) => return,
            }
        }
    })
}

fn child_exit_reason(
    worker_name: &str,
    status: io::Result<std::process::ExitStatus>,
    latest_output: &Arc<Mutex<Option<String>>>,
) -> String {
    let output = latest_output
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let suffix = output.map(|line| format!(": {line}")).unwrap_or_default();
    match status {
        Ok(status) if status.success() => format!("{worker_name} exited unexpectedly{suffix}"),
        Ok(status) => format!("{worker_name} exited with {status}{suffix}"),
        Err(error) => format!("failed while waiting for {worker_name}: {error}{suffix}"),
    }
}

async fn wait_for_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

async fn wait_for_relay_retry(
    delay: Duration,
    ownership: &mut watch::Receiver<Option<Instant>>,
    ownership_lease_timeout: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> RelayRetryOutcome {
    let retry_at = time::Instant::now() + delay;
    loop {
        let Some(ownership_deadline) = video_ownership_deadline(ownership, ownership_lease_timeout)
        else {
            return RelayRetryOutcome::Standby;
        };
        tokio::select! {
            _ = time::sleep_until(retry_at) => return RelayRetryOutcome::Retry,
            _ = time::sleep_until(time::Instant::from_std(ownership_deadline)) => {
                if !has_current_video_ownership(ownership, ownership_lease_timeout) {
                    return RelayRetryOutcome::Standby;
                }
            }
            changed = ownership.changed() => {
                if changed.is_err() {
                    return RelayRetryOutcome::Standby;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return RelayRetryOutcome::Shutdown;
                }
            }
        }
    }
}

fn video_ownership_deadline(
    ownership: &watch::Receiver<Option<Instant>>,
    ownership_lease_timeout: Duration,
) -> Option<Instant> {
    (*ownership.borrow()).and_then(|renewed_at| renewed_at.checked_add(ownership_lease_timeout))
}

fn has_current_video_ownership(
    ownership: &watch::Receiver<Option<Instant>>,
    ownership_lease_timeout: Duration,
) -> bool {
    video_ownership_is_current(*ownership.borrow(), ownership_lease_timeout, Instant::now())
}

fn video_ownership_is_current(
    renewed_at: Option<Instant>,
    ownership_lease_timeout: Duration,
    now: Instant,
) -> bool {
    renewed_at
        .and_then(|renewed_at| renewed_at.checked_add(ownership_lease_timeout))
        .is_some_and(|deadline| deadline > now)
}

fn ensure_regular_file(path: &Path, label: &str) -> io::Result<()> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("{label} is unavailable at {}: {error}", path.display()),
        )
    })?;
    if metadata.is_file() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} is not a file: {}", path.display()),
        ))
    }
}

async fn media_mtx_path_is_ready_from_api(
    control_api_addr: &str,
    stream_path: &str,
    request_timeout: Duration,
) -> io::Result<bool> {
    let mut stream = time::timeout(request_timeout, TcpStream::connect(control_api_addr))
        .await
        .map_err(|_| timeout_error("connecting to MediaMTX local API"))??;
    let request = format!(
        "GET /v3/paths/list HTTP/1.1\r\nHost: {control_api_addr}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    time::timeout(request_timeout, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| timeout_error("writing to MediaMTX local API"))??;
    let mut response = Vec::new();
    time::timeout(request_timeout, stream.read_to_end(&mut response))
        .await
        .map_err(|_| timeout_error("reading from MediaMTX local API"))??;
    let body = http_json_body(&response)?;
    media_mtx_path_is_ready(body, stream_path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "MediaMTX API returned invalid JSON",
        )
    })
}

fn http_json_body(response: &[u8]) -> io::Result<&[u8]> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "MediaMTX API response had no headers",
            )
        })?;
    let status_end = response[..header_end]
        .windows(2)
        .position(|window| window == b"\r\n")
        .unwrap_or(header_end);
    let status = std::str::from_utf8(&response[..status_end]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "MediaMTX API response status was not UTF-8",
        )
    })?;
    if status.split_whitespace().nth(1) != Some("200") {
        return Err(io::Error::other(format!("MediaMTX API returned {status}")));
    }
    Ok(&response[header_end + 4..])
}

fn timeout_error(operation: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("timed out while {operation}"),
    )
}

fn redact_media_urls(message: &str) -> String {
    let mut redacted = String::new();
    let mut remaining = message;
    loop {
        let next = ["rtsp://", "rtmp://"]
            .into_iter()
            .filter_map(|scheme| remaining.find(scheme).map(|offset| (offset, scheme)))
            .min_by_key(|(offset, _)| *offset);
        let Some((offset, scheme)) = next else {
            redacted.push_str(remaining);
            return redacted;
        };
        redacted.push_str(&remaining[..offset]);
        redacted.push_str(scheme);
        redacted.push_str("<redacted>");
        let after_scheme = &remaining[offset + scheme.len()..];
        let endpoint_end = after_scheme
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(character, '\"' | '\'' | ')' | '(' | '[' | ']')
            })
            .unwrap_or(after_scheme.len());
        remaining = &after_scheme[endpoint_end..];
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay_config() -> RelayVideoConfig {
        RelayVideoConfig {
            source_url: "rtsp://camera.example/live".into(),
            publish_url: "rtmp://server.example/uav-1".into(),
            startup_timeout_ms: 100,
            stall_timeout_ms: 100,
            restart_max_delay_ms: 1_000,
        }
    }

    fn missing_media_paths() -> MediaToolPaths {
        let missing_root = std::env::temp_dir().join(format!(
            "relay-linkd-video-missing-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        MediaToolPaths {
            ffmpeg: missing_root.join("ffmpeg.exe"),
            mediamtx: missing_root.join("mediamtx.exe"),
            mediamtx_config: missing_root.join("mediamtx.yml"),
        }
    }

    #[tokio::test]
    async fn missing_ffmpeg_enters_backoff_without_stopping_the_supervisor() {
        let health = VideoHealth::configured();
        let paths = missing_media_paths();
        let (_active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            paths,
            health.clone(),
            active_rx,
            Duration::from_secs(5),
            shutdown_rx,
        ));

        time::timeout(Duration::from_millis(100), async {
            loop {
                let snapshot = health.snapshot();
                if snapshot.state == VideoState::Backoff {
                    assert!(!snapshot.worker_running);
                    assert_eq!(snapshot.restart_count, 1);
                    assert!(snapshot.last_error.is_some());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("missing FFmpeg should be reported promptly");

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[tokio::test]
    async fn inactive_relay_waits_in_standby_before_starting_ffmpeg() {
        let health = VideoHealth::configured();
        let (active_tx, active_rx) = watch::channel(None);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            missing_media_paths(),
            health.clone(),
            active_rx,
            Duration::from_secs(5),
            shutdown_rx,
        ));

        time::timeout(Duration::from_millis(100), async {
            loop {
                let snapshot = health.snapshot();
                if snapshot.state == VideoState::Standby {
                    assert!(!snapshot.worker_running);
                    assert_eq!(snapshot.restart_count, 0);
                    assert!(snapshot.last_error.is_none());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inactive relay should enter standby promptly");

        active_tx
            .send(Some(Instant::now()))
            .expect("select relay for video");
        time::timeout(Duration::from_millis(100), async {
            loop {
                if health.snapshot().state == VideoState::Backoff {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("selected relay should attempt to start FFmpeg");

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[tokio::test]
    async fn deselection_interrupts_video_retry_and_returns_to_standby() {
        let health = VideoHealth::configured();
        let (active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            missing_media_paths(),
            health.clone(),
            active_rx,
            Duration::from_secs(5),
            shutdown_rx,
        ));

        time::timeout(Duration::from_millis(100), async {
            loop {
                if health.snapshot().state == VideoState::Backoff {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("missing FFmpeg should enter backoff");

        active_tx.send(None).expect("deselect relay for video");
        time::timeout(Duration::from_millis(100), async {
            loop {
                let snapshot = health.snapshot();
                if snapshot.state == VideoState::Standby {
                    assert!(!snapshot.worker_running);
                    assert_eq!(snapshot.restart_count, 0);
                    assert!(snapshot.last_error.is_none());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deselected relay should leave backoff promptly");

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[tokio::test]
    async fn ownership_renewal_does_not_bypass_ffmpeg_backoff() {
        let health = VideoHealth::configured();
        let (active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            missing_media_paths(),
            health.clone(),
            active_rx,
            Duration::from_secs(5),
            shutdown_rx,
        ));

        time::timeout(Duration::from_millis(100), async {
            loop {
                if health.snapshot().state == VideoState::Backoff {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("missing FFmpeg should enter backoff");

        active_tx
            .send(Some(Instant::now()))
            .expect("renew video ownership");
        time::sleep(Duration::from_millis(50)).await;
        let snapshot = health.snapshot();
        assert_eq!(snapshot.state, VideoState::Backoff);
        assert_eq!(snapshot.restart_count, 1);

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[tokio::test]
    async fn expired_ownership_lease_interrupts_ffmpeg_backoff() {
        let health = VideoHealth::configured();
        let (_active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            missing_media_paths(),
            health.clone(),
            active_rx,
            Duration::from_millis(50),
            shutdown_rx,
        ));

        time::timeout(Duration::from_millis(200), async {
            loop {
                if health.snapshot().state == VideoState::Standby {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expired ownership should return to standby");

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[tokio::test]
    async fn revoked_ownership_prevents_an_ffmpeg_start_attempt() {
        let health = VideoHealth::configured();
        let (_active_tx, mut active_rx) = watch::channel(None);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let outcome = run_ffmpeg_once(
            &relay_config(),
            &missing_media_paths(),
            &health,
            &mut active_rx,
            Duration::from_secs(5),
            &mut shutdown_rx,
        )
        .await;

        assert!(matches!(outcome, WorkerOutcome::Standby));
        assert!(health.snapshot().last_error.is_none());
    }

    #[test]
    fn worker_output_redacts_camera_and_publish_credentials() {
        let message = "opening rtsp://pilot:camera-secret@camera.example/live then rtmp://push:server-secret@server.example/live";

        assert_eq!(
            redact_media_urls(message),
            "opening rtsp://<redacted> then rtmp://<redacted>"
        );
    }

    #[test]
    fn restarting_a_worker_discards_liveness_from_the_previous_process() {
        let health = VideoHealth::configured();
        health.mark_liveness(VideoState::Publishing, true);

        health.mark_starting();

        assert!(health.liveness_age(Instant::now()).is_none());
    }

    #[cfg(unix)]
    fn fake_ffmpeg_paths(name: &str, script: &str) -> MediaToolPaths {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!(
            "relay-linkd-video-{name}-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        std::fs::create_dir_all(&root).expect("create fake media directory");
        let ffmpeg = root.join("ffmpeg");
        std::fs::write(&ffmpeg, format!("#!/bin/sh\n{script}\n")).expect("write fake FFmpeg");
        std::fs::set_permissions(&ffmpeg, std::fs::Permissions::from_mode(0o755))
            .expect("make fake FFmpeg executable");
        MediaToolPaths {
            ffmpeg,
            mediamtx: root.join("mediamtx"),
            mediamtx_config: root.join("mediamtx.yml"),
        }
    }

    #[cfg(unix)]
    fn stall_test_config() -> RelayVideoConfig {
        RelayVideoConfig {
            startup_timeout_ms: 2_000,
            stall_timeout_ms: 300,
            ..relay_config()
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn frozen_ffmpeg_progress_is_detected_as_a_stall_and_restarted() {
        // Mirrors FFmpeg 7+ behaviour on a frozen camera or black-holed RTMP
        // uplink: progress=continue keeps coming, but the byte count is stuck.
        let paths = fake_ffmpeg_paths(
            "frozen",
            "while :; do printf 'total_size=1000\\nout_time_us=5000000\\nprogress=continue\\n'; sleep 0.05; done",
        );
        let health = VideoHealth::configured();
        let (_active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            stall_test_config(),
            paths,
            health.clone(),
            active_rx,
            Duration::from_secs(60),
            shutdown_rx,
        ));

        time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = health.snapshot();
                if snapshot.restart_count >= 1 {
                    let error = snapshot.last_error.unwrap_or_default();
                    assert!(
                        error.contains("stopped advancing"),
                        "unexpected error: {error}"
                    );
                    break;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a frozen FFmpeg must be restarted even though it keeps printing progress");

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(5), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn advancing_ffmpeg_progress_keeps_the_worker_alive() {
        let paths = fake_ffmpeg_paths(
            "advancing",
            "i=0; while :; do i=$((i+100)); printf 'total_size=%s\\nout_time_us=%s\\nprogress=continue\\n' $i $i; sleep 0.05; done",
        );
        let health = VideoHealth::configured();
        let (_active_tx, active_rx) = watch::channel(Some(Instant::now()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            stall_test_config(),
            paths,
            health.clone(),
            active_rx,
            Duration::from_secs(60),
            shutdown_rx,
        ));

        time::sleep(Duration::from_millis(1_200)).await;
        let snapshot = health.snapshot();
        assert_eq!(snapshot.state, VideoState::Publishing);
        assert_eq!(snapshot.restart_count, 0);
        assert!(snapshot.last_error.is_none());

        shutdown_tx.send(true).expect("signal supervisor shutdown");
        time::timeout(Duration::from_secs(5), task)
            .await
            .expect("supervisor should stop")
            .expect("supervisor task should not panic");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_stop_gives_the_worker_a_graceful_sigterm_first() {
        let marker = std::env::temp_dir().join(format!(
            "relay-linkd-video-sigterm-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        let mut command = managed_command(Path::new("sh"));
        command.args([
            "-c",
            &format!(
                "trap 'touch {}; exit 0' TERM; while :; do sleep 0.05; done",
                marker.display()
            ),
        ]);
        let mut worker = ManagedChild::spawn(command).expect("spawn shell worker");
        time::sleep(Duration::from_millis(100)).await;

        worker.kill_and_wait().await;

        assert!(
            marker.exists(),
            "worker should have received SIGTERM before SIGKILL"
        );
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_stop_kills_a_worker_that_ignores_sigterm() {
        let mut command = managed_command(Path::new("sh"));
        command.args(["-c", "trap '' TERM; while :; do sleep 0.05; done"]);
        let mut worker = ManagedChild::spawn(command).expect("spawn shell worker");
        let process_group_id = worker.process_group_id;
        time::sleep(Duration::from_millis(100)).await;

        time::timeout(Duration::from_secs(5), worker.kill_and_wait())
            .await
            .expect("SIGKILL fallback should stop a worker that ignores SIGTERM");

        // Orphaned helpers are reaped by init asynchronously; allow a moment.
        time::timeout(Duration::from_secs(2), async {
            while unsafe { libc::kill(-process_group_id, 0) } == 0 {
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("worker process group should no longer exist");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_shutdown_kills_the_entire_media_process_group() {
        let mut command = managed_command(Path::new("sh"));
        command.args(["-c", "sleep 30 & wait"]);
        let mut worker = ManagedChild::spawn(command).expect("spawn shell worker");
        let process_group_id = worker.process_group_id;

        worker.kill_and_wait().await;

        let probe = unsafe { libc::kill(-process_group_id, 0) };
        assert_eq!(probe, -1, "worker process group should no longer exist");
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
}

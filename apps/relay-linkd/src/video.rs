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
    model::{VideoSnapshot, VideoState},
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

pub fn ffmpeg_arguments(config: &RelayVideoConfig) -> Vec<OsString> {
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
        "-i",
    ]
    .into_iter()
    .map(OsString::from)
    .chain(std::iter::once(OsString::from(&config.source_url)))
    .chain(["-c", "copy", "-f", "flv"].into_iter().map(OsString::from))
    .chain(std::iter::once(OsString::from(&config.publish_url)))
    .collect()
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
    shutdown: watch::Receiver<bool>,
) {
    let paths = match MediaToolPaths::from_current_executable() {
        Ok(paths) => paths,
        Err(error) => {
            health.mark_backoff(1, format!("unable to resolve media tool paths: {error}"));
            return;
        }
    };
    supervise_relay_video_with_paths(config, paths, health, shutdown).await;
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
    mut shutdown: watch::Receiver<bool>,
) {
    let mut restart_count = 0_u64;
    loop {
        if *shutdown.borrow() {
            health.mark_stopped();
            return;
        }

        match run_ffmpeg_once(&config, &paths, &health, &mut shutdown).await {
            WorkerOutcome::Shutdown => {
                health.mark_stopped();
                return;
            }
            WorkerOutcome::Restart(reason) => {
                restart_count = restart_count.saturating_add(1);
                let delay = retry_delay(
                    restart_count,
                    Duration::from_millis(config.restart_max_delay_ms),
                );
                let message = format!("{reason}; retrying in {} ms", delay.as_millis());
                health.mark_backoff(restart_count, message.clone());
                warn!(restart_count, retry_delay_ms = delay.as_millis(), reason = %reason, "relay video worker will be restarted");
                if wait_for_shutdown(delay, &mut shutdown).await {
                    health.mark_stopped();
                    return;
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

        match run_mediamtx_once(&config, &paths, &health, &mut shutdown).await {
            WorkerOutcome::Shutdown => {
                health.mark_stopped();
                return;
            }
            WorkerOutcome::Restart(reason) => {
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
    Restart(String),
}

async fn run_ffmpeg_once(
    config: &RelayVideoConfig,
    paths: &MediaToolPaths,
    health: &VideoHealth,
    shutdown: &mut watch::Receiver<bool>,
) -> WorkerOutcome {
    if let Err(error) = ensure_regular_file(&paths.ffmpeg, "FFmpeg executable") {
        return WorkerOutcome::Restart(error.to_string());
    }

    health.mark_starting();
    let mut command = managed_command(&paths.ffmpeg);
    command.args(ffmpeg_arguments(config));
    let mut managed_child = match ManagedChild::spawn(command) {
        Ok(child) => child,
        Err(error) => return WorkerOutcome::Restart(format!("failed to start FFmpeg: {error}")),
    };
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
                        "FFmpeg progress stopped for {} ms",
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
        if let Err(error) = terminate_process_group(self.process_group_id) {
            debug!(%error, "failed to request media worker process-group shutdown");
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
    let result = unsafe { libc::kill(-process_group_id, libc::SIGKILL) };
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
        loop {
            match lines.next_line().await {
                Ok(Some(line)) if line.starts_with("progress=") => {
                    health.mark_liveness(VideoState::Publishing, true);
                }
                Ok(Some(_)) => {}
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

    #[tokio::test]
    async fn missing_ffmpeg_enters_backoff_without_stopping_the_supervisor() {
        let health = VideoHealth::configured();
        let missing_root = std::env::temp_dir().join(format!(
            "relay-linkd-video-missing-{}-{}",
            std::process::id(),
            unix_timestamp_ms()
        ));
        let paths = MediaToolPaths {
            ffmpeg: missing_root.join("ffmpeg.exe"),
            mediamtx: missing_root.join("mediamtx.exe"),
            mediamtx_config: missing_root.join("mediamtx.yml"),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(supervise_relay_video_with_paths(
            relay_config(),
            paths,
            health.clone(),
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

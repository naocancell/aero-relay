use std::io;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use relay_core::config::Config;
use relay_core::model::{RelaySnapshot, Role, ServerSnapshot};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

pub const MAX_FRAME_LENGTH: usize = 1024 * 1024;

pub fn fixed_endpoint() -> String {
    if cfg!(windows) {
        r"\\.\pipe\relay-link-control".into()
    } else {
        "/run/relay-link/control.sock".into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum IpcMessage {
    GetSnapshot,
    Snapshot { snapshot: SnapshotPayload },
    Subscribe,
    StateChanged { snapshot: SnapshotPayload },
    GetConfig,
    Config { config: Box<Config> },
    SaveConfig { config: Box<Config> },
    ConfigSaved,
    GetSerialPorts,
    SerialPorts { ports: Vec<String> },
    RestartBackend,
    BackendRestarted,
    Error { message: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", content = "state")]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPayload {
    RoleOnly { role: Role },
    Relay(RelaySnapshot),
    Server(ServerSnapshot),
}

pub struct JsonFrameCodec {
    inner: LengthDelimitedCodec,
}

impl JsonFrameCodec {
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .length_field_length(4)
                .length_field_offset(0)
                .num_skip(4)
                .max_frame_length(MAX_FRAME_LENGTH)
                .big_endian()
                .new_codec(),
        }
    }
}

impl Default for JsonFrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for JsonFrameCodec {
    type Item = IpcMessage;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(frame) = self.inner.decode(source)? else {
            return Ok(None);
        };
        serde_json::from_slice(&frame)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

impl Encoder<IpcMessage> for JsonFrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: IpcMessage, destination: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = serde_json::to_vec(&item)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.inner.encode(payload.into(), destination)
    }
}

#[cfg(unix)]
pub struct LocalIpcListener {
    path: PathBuf,
    listener: tokio::net::UnixListener,
}

#[cfg(windows)]
pub struct LocalIpcListener {
    endpoint: String,
    first_pipe_instance: bool,
}

impl LocalIpcListener {
    pub fn bind(endpoint: impl AsRef<Path>) -> io::Result<Self> {
        #[cfg(unix)]
        {
            let path = endpoint.as_ref().to_path_buf();
            prepare_socket_path(&path)?;
            let listener = tokio::net::UnixListener::bind(&path)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))?;
            return Ok(Self { path, listener });
        }

        #[cfg(windows)]
        {
            return Ok(Self {
                endpoint: endpoint.as_ref().to_string_lossy().into_owned(),
                first_pipe_instance: true,
            });
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "local IPC is unsupported on this platform",
        ))
    }

    pub async fn accept(&mut self) -> io::Result<LocalIpcStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.listener.accept().await?;
            return Ok(LocalIpcStream::Unix(stream));
        }

        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;

            let mut options = ServerOptions::new();
            options.first_pipe_instance(self.first_pipe_instance);
            options.reject_remote_clients(true);
            let server = create_restricted_pipe(&options, &self.endpoint)?;
            self.first_pipe_instance = false;
            server.connect().await?;
            return Ok(LocalIpcStream::Windows(server));
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "local IPC is unsupported on this platform",
        ))
    }
}

#[cfg(windows)]
fn create_restricted_pipe(
    options: &tokio::net::windows::named_pipe::ServerOptions,
    endpoint: &str,
) -> io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    use std::ffi::OsStr;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};

    let descriptor_text: Vec<u16> = OsStr::new("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGW;;;IU)")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    let mut descriptor_size = 0u32;
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            descriptor_text.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            &mut descriptor_size,
        )
    };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let result = unsafe {
        options.create_with_security_attributes_raw(
            endpoint,
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast(),
        )
    };
    unsafe {
        let _ = LocalFree(descriptor);
    }
    result
}

#[cfg(unix)]
impl Drop for LocalIpcListener {
    fn drop(&mut self) {
        let _ = remove_socket_if_safe(&self.path);
    }
}

pub struct LocalIpcClient;

impl LocalIpcClient {
    pub async fn connect(endpoint: impl AsRef<Path>) -> io::Result<LocalIpcStream> {
        #[cfg(unix)]
        {
            return tokio::net::UnixStream::connect(endpoint)
                .await
                .map(LocalIpcStream::Unix);
        }

        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ClientOptions;

            let client = ClientOptions::new().open(endpoint.as_ref())?;
            return Ok(LocalIpcStream::Client(client));
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "local IPC is unsupported on this platform",
        ))
    }
}

pub enum LocalIpcStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    Windows(tokio::net::windows::named_pipe::NamedPipeServer),
    #[cfg(windows)]
    Client(tokio::net::windows::named_pipe::NamedPipeClient),
}

#[cfg(unix)]
fn prepare_socket_path(path: &Path) -> io::Result<()> {
    if !path.exists() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        return Ok(());
    }
    remove_socket_if_safe(path)
}

#[cfg(unix)]
fn remove_socket_if_safe(path: &Path) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "IPC path exists and is not a Unix socket: {}",
                path.display()
            ),
        ));
    }
    std::fs::remove_file(path)
}

impl AsyncRead for LocalIpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(windows)]
            Self::Windows(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(windows)]
            Self::Client(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for LocalIpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(windows)]
            Self::Windows(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(windows)]
            Self::Client(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(windows)]
            Self::Windows(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(windows)]
            Self::Client(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(windows)]
            Self::Windows(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(windows)]
            Self::Client(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}

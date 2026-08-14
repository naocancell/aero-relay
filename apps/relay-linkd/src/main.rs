#[cfg(any(windows, test))]
use std::ffi::OsString;
#[cfg(windows)]
use std::time::Duration;
use std::{
    io,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use relay_core::config::{Config, load_config};
use relay_core::model::Role;
use relay_ipc::{SnapshotPayload, fixed_endpoint};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use relay_linkd::{ipc_service, logging, relay, server};

#[derive(Debug, Parser)]
#[command(
    name = "relay-linkd",
    version,
    about = "Headless relay-link communication service"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[cfg(windows)]
const SERVICE_NAME: &str = "RelayLinkD";
#[cfg(any(windows, test))]
const ERROR_FAILED_SERVICE_CONTROLLER_CONNECT: i32 = 1063;

#[derive(Debug, Subcommand)]
enum Command {
    Relay,
    Server,
}

fn main() {
    let log_path = match init_logging() {
        Ok(log_path) => log_path,
        Err(error) => {
            eprintln!("failed to initialize relay-linkd file logging: {error}");
            std::process::exit(1);
        }
    };
    info!(path = %log_path.display(), "file logging initialized");

    #[cfg(windows)]
    match windows_service::service_dispatcher::start(SERVICE_NAME, ffi_service_main) {
        Ok(()) => return,
        Err(error) => {
            let error_code = windows_service_error_code(&error);
            if !is_console_fallback_error_code(error_code) {
                error!(?error_code, %error, "failed to start RelayLinkD as a Windows service");
                std::process::exit(1);
            }
        }
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create Tokio runtime");
    let result = runtime.block_on(async move {
        let ctrl_c_shutdown = shutdown_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = ctrl_c_shutdown.send(true);
            }
        });
        run(Cli::parse(), shutdown_rx).await
    });
    if let Err(error) = result {
        error!(%error, "relay-linkd stopped with an error");
        std::process::exit(1);
    }
}

fn init_logging() -> Result<PathBuf, io::Error> {
    let executable_path = std::env::current_exe()?;
    let log_path = logging::log_file_path(&executable_path)?;
    let log_file = logging::open_log_file(executable_path)?;
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(log_file)
                .with_ansi(false)
                .with_target(false)
                .compact(),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::stderr)
                .with_target(false)
                .compact(),
        )
        .init();
    Ok(log_path)
}

async fn run(
    cli: Cli,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_ready(cli, shutdown_rx, None).await
}

async fn run_with_ready(
    cli: Cli,
    shutdown_rx: watch::Receiver<bool>,
    ready_tx: Option<oneshot::Sender<()>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = installed_config_path()?;
    let config = load_config(&config_path)?;

    match cli.command {
        Command::Relay => {
            let relay_config = config
                .relay
                .clone()
                .ok_or("config does not contain a [relay] section")?;
            let ipc_endpoint = fixed_endpoint();
            let (snapshot_tx, snapshot_rx) =
                tokio::sync::watch::channel(SnapshotPayload::RoleOnly { role: Role::Relay });
            let (reload_tx, reload_rx) = mpsc::channel(4);
            spawn_ipc(
                config.clone(),
                config_path.clone(),
                ipc_endpoint,
                Role::Relay,
                snapshot_rx,
                reload_tx,
            );
            info!(relay_id = %relay_config.id, "starting relay backend");
            relay::run(relay_config, snapshot_tx, shutdown_rx, reload_rx, ready_tx).await?;
        }
        Command::Server => {
            let server_config = config
                .server
                .clone()
                .ok_or("config does not contain a [server] section")?;
            let ipc_endpoint = fixed_endpoint();
            let (snapshot_tx, snapshot_rx) =
                tokio::sync::watch::channel(SnapshotPayload::RoleOnly { role: Role::Server });
            let (reload_tx, reload_rx) = mpsc::channel(4);
            spawn_ipc(
                config.clone(),
                config_path,
                ipc_endpoint,
                Role::Server,
                snapshot_rx,
                reload_tx,
            );
            info!(server_id = %server_config.id, "starting server backend");
            server::run(server_config, snapshot_tx, shutdown_rx, reload_rx, ready_tx).await?;
        }
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn parse_launch_arguments<I>(arguments: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = OsString>,
{
    resolve_service_arguments(arguments, std::env::args_os())
}

#[cfg(any(windows, test))]
fn resolve_service_arguments<I, F>(service_args: I, process_args: F) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = OsString>,
    F: IntoIterator<Item = OsString>,
{
    let process_passed: Vec<OsString> = process_args.into_iter().skip(1).collect();
    let mut argv = vec![OsString::from("relay-linkd")];
    if !process_passed.is_empty() {
        argv.extend(process_passed);
    } else {
        argv.extend(service_args.into_iter().skip(1));
    }
    Cli::try_parse_from(argv)
}


fn installed_config_path() -> Result<PathBuf, io::Error> {
    fixed_config_path(std::env::current_exe()?)
}

fn fixed_config_path(executable_path: impl AsRef<Path>) -> Result<PathBuf, io::Error> {
    let executable_path = executable_path.as_ref();
    let install_dir = executable_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "relay-linkd executable has no parent directory",
        )
    })?;
    Ok(install_dir.join("config").join("config.toml"))
}

#[cfg(any(windows, test))]
fn is_console_fallback_error_code(error_code: Option<i32>) -> bool {
    error_code == Some(ERROR_FAILED_SERVICE_CONTROLLER_CONNECT)
}

#[cfg(windows)]
fn windows_service_error_code(error: &windows_service::Error) -> Option<i32> {
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .and_then(std::io::Error::raw_os_error)
}

#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, windows_service_main);

#[cfg(windows)]
fn windows_service_main(arguments: Vec<OsString>) {
    if let Err(error) = run_windows_service(arguments) {
        error!(%error, "relay-linkd Windows service stopped with an error");
    }
}

#[cfg(windows)]
fn run_windows_service(
    arguments: Vec<OsString>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let event_handler = move |event| -> ServiceControlHandlerResult {
        match event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                let _ = shutdown_tx.send(true);
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };
    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(10),
        process_id: None,
    })?;

    let cli = match parse_launch_arguments(arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = status_handle.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::Stopped,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: ServiceExitCode::Win32(1),
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: None,
            });
            return Err(error.into());
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let mut status_shutdown = shutdown_rx.clone();
    let status_handle_for_shutdown = status_handle;
    let status_task = runtime.spawn(async move {
        if status_shutdown.changed().await.is_ok() && *status_shutdown.borrow() {
            let _ = status_handle_for_shutdown.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::StopPending,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 1,
                wait_hint: Duration::from_secs(10),
                process_id: None,
            });
        }
    });

    let (ready_tx, ready_rx) = oneshot::channel();
    let backend_task = runtime.spawn(run_with_ready(cli, shutdown_rx, Some(ready_tx)));
    let startup_result = runtime.block_on(ready_rx);
    let result = if startup_result.is_ok() {
        if let Err(error) = status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Running,
            controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        }) {
            backend_task.abort();
            let _ = runtime.block_on(backend_task);
            Err(error.into())
        } else {
            match runtime.block_on(backend_task) {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            }
        }
    } else {
        match runtime.block_on(backend_task) {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        }
    };
    status_task.abort();
    let exit_code = if result.is_ok() { 0 } else { 1 };
    let _ = status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(exit_code),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    });
    result
}

fn spawn_ipc(
    config: Config,
    config_path: PathBuf,
    endpoint: String,
    role: Role,
    snapshot_rx: tokio::sync::watch::Receiver<SnapshotPayload>,
    reload_tx: mpsc::Sender<ipc_service::ReloadRequest>,
) {
    tokio::spawn(async move {
        if let Err(error) =
            ipc_service::run(endpoint, config, config_path, role, snapshot_rx, reload_tx).await
        {
            warn!(%error, "local IPC service stopped; aircraft forwarding remains independent");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn startup_modes_use_fixed_config_path() {
        let relay = resolve_service_arguments(
            [OsString::from("RelayLinkD")],
            [
                OsString::from(r"C:\Program Files\Relay Link\relay-linkd.exe"),
                OsString::from("relay"),
            ],
        )
        .expect("relay mode should not require a config argument");
        assert!(matches!(relay.command, Command::Relay));

        let server = resolve_service_arguments(
            [OsString::from("RelayLinkD")],
            [
                OsString::from(r"C:\Program Files\Relay Link\relay-linkd.exe"),
                OsString::from("server"),
            ],
        )
        .expect("server mode should not require a config argument");
        assert!(matches!(server.command, Command::Server));

        assert!(
            resolve_service_arguments(
                [OsString::from("RelayLinkD")],
                [
                    OsString::from(r"C:\Program Files\Relay Link\relay-linkd.exe"),
                    OsString::from("relay"),
                    OsString::from("--config"),
                    OsString::from("old.toml"),
                ],
            )
            .is_err()
        );

        assert_eq!(
            fixed_config_path(r"C:\Program Files\Relay Link\relay-linkd.exe")
                .expect("installed config path"),
            PathBuf::from(r"C:\Program Files\Relay Link\config\config.toml")
        );
    }

    #[test]
    fn resolve_service_arguments_prefers_registered_process_args() {
        let cli = resolve_service_arguments(
            [OsString::from("RelayLinkD"), OsString::from("server")],
            [
                OsString::from(r"C:\Program Files\Relay Link\relay-linkd.exe"),
                OsString::from("relay"),
            ],
        )
        .expect("registered process arguments should take precedence");
        assert!(matches!(cli.command, Command::Relay));
    }

    #[test]
    fn resolve_service_arguments_falls_back_when_process_args_empty() {
        let cli = resolve_service_arguments(
            [OsString::from("RelayLinkD"), OsString::from("server")],
            [OsString::from(r"C:\Program Files\Relay Link\relay-linkd.exe")],
        )
        .expect("should fallback to service args if process args have no subcommand");
        assert!(matches!(cli.command, Command::Server));
    }

    #[test]
    fn only_service_controller_connection_failure_falls_back_to_console() {
        assert!(is_console_fallback_error_code(Some(1063)));
        assert!(!is_console_fallback_error_code(Some(5)));
        assert!(!is_console_fallback_error_code(None));
    }
}


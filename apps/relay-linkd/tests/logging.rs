use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use relay_linkd::logging::{log_file_path, open_log_file};

fn temporary_test_directory() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "relay-linkd-logging-{}-{nonce}",
        std::process::id()
    ))
}

#[test]
fn executable_relative_log_file_is_created_and_appended() {
    let temporary_directory = temporary_test_directory();
    let executable_path = temporary_directory.join("relay-linkd.exe");
    let log_path =
        log_file_path(&executable_path).expect("log path should be derived from executable");
    assert_eq!(
        log_path,
        temporary_directory.join("log").join("relay-linkd.log")
    );

    fs::create_dir_all(&temporary_directory).expect("create temporary directory");
    {
        let mut first = open_log_file(&executable_path).expect("open first log file handle");
        writeln!(first, "first entry").expect("write first entry");
    }
    {
        let mut second = open_log_file(&executable_path).expect("open second log file handle");
        writeln!(second, "second entry").expect("write second entry");
    }

    assert_eq!(
        fs::read_to_string(&log_path).expect("read appended log entries"),
        "first entry\nsecond entry\n"
    );
    fs::remove_dir_all(temporary_directory).expect("remove temporary directory");
}

#[test]
fn relay_linkd_startup_writes_to_the_file_log_beside_its_executable() {
    let temporary_directory = temporary_test_directory();
    fs::create_dir_all(&temporary_directory).expect("create temporary directory");
    let built_backend = std::env::var_os("CARGO_BIN_EXE_relay-linkd")
        .map(PathBuf::from)
        .expect("Cargo should provide the relay-linkd test binary");
    let copied_backend = temporary_directory.join(
        built_backend
            .file_name()
            .expect("built backend should have a file name"),
    );
    fs::copy(&built_backend, &copied_backend).expect("copy backend into temporary directory");

    let output = Command::new(&copied_backend)
        .arg("--help")
        .env("RUST_LOG", "info")
        .output()
        .expect("run copied backend");
    assert!(
        output.status.success(),
        "backend help command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let log_path = temporary_directory.join("log").join("relay-linkd.log");
    let log_contents = fs::read_to_string(log_path).expect("read startup log file");
    assert!(
        log_contents.contains("file logging initialized"),
        "startup event was not written to the file log: {log_contents}"
    );
    fs::remove_dir_all(temporary_directory).expect("remove temporary directory");
}

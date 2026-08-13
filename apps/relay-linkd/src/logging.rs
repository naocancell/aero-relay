use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

const LOG_DIRECTORY: &str = "log";
const LOG_FILE_NAME: &str = "relay-linkd.log";

pub fn log_file_path(executable_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let executable_directory = executable_path.as_ref().parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "relay-linkd executable has no parent directory",
        )
    })?;
    Ok(executable_directory.join(LOG_DIRECTORY).join(LOG_FILE_NAME))
}

pub fn open_log_file(executable_path: impl AsRef<Path>) -> io::Result<File> {
    let log_file_path = log_file_path(executable_path)?;
    let log_directory = log_file_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "log file path has no parent directory",
        )
    })?;
    fs::create_dir_all(log_directory)?;
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file_path)
}

use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Local;
use parking_lot::Mutex;

static LOG_FILE: OnceLock<Mutex<Option<File>>> = OnceLock::new();

pub fn log_dir() -> PathBuf {
    std::env::var_os("APPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("Xun")
        .join("logs")
}

pub fn log_path() -> PathBuf {
    log_dir().join(format!("xun-{}.log", Local::now().format("%Y-%m-%d")))
}

pub fn init_session() {
    let path = log_path();
    let _ = sink();
    info(format!("session started, log file: {}", path.display()));
}

pub fn info(message: impl AsRef<str>) {
    write_line("INFO", message.as_ref());
}

pub fn warn(message: impl AsRef<str>) {
    write_line("WARN", message.as_ref());
}

pub fn error(message: impl AsRef<str>) {
    write_line("ERROR", message.as_ref());
}

fn sink() -> &'static Mutex<Option<File>> {
    LOG_FILE.get_or_init(|| {
        let path = log_path();
        if let Some(parent) = path.parent() {
            let _ = create_dir_all(parent);
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok();
        Mutex::new(file)
    })
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_millis())
        .unwrap_or(0)
}

fn write_line(level: &str, message: &str) {
    let line = format!(
        "[Xun][{level}][{}][{:?}] {message}",
        now_millis(),
        std::thread::current().id()
    );

    eprintln!("{line}");

    if let Some(file) = sink().lock().as_mut() {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

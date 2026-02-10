use std::fs::{File, OpenOptions, create_dir_all, read_dir, remove_file};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::{Local, NaiveDate};
use parking_lot::Mutex;

static LOG_FILE: OnceLock<Mutex<Option<File>>> = OnceLock::new();
static ACTIVE_LOG_PATH: OnceLock<PathBuf> = OnceLock::new();
static LOG_SCOPE: OnceLock<LogScope> = OnceLock::new();
const LOG_RETENTION_DAYS: i64 = 7;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogScope {
    Client,
    Server,
}

fn file_name_for_today() -> String {
    format!("xun-{}.log", Local::now().format("%Y-%m-%d"))
}

pub fn log_dir() -> PathBuf {
    preferred_log_dirs(configured_scope())
        .into_iter()
        .next()
        .unwrap_or_else(|| std::env::temp_dir().join("Xun").join("logs"))
}

pub fn log_path() -> PathBuf {
    active_log_path().unwrap_or_else(|| log_dir().join(file_name_for_today()))
}

pub fn active_log_path() -> Option<PathBuf> {
    ACTIVE_LOG_PATH.get().cloned()
}

pub fn init_session(scope: LogScope) {
    let _ = LOG_SCOPE.set(scope);
    let _ = sink();
    info(format!(
        "session started, log file: {}",
        log_path().display()
    ));
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
        let candidates = preferred_log_dirs(configured_scope())
            .into_iter()
            .map(|dir| dir.join(file_name_for_today()))
            .collect::<Vec<_>>();

        for path in candidates {
            if let Some(parent) = path.parent()
                && let Err(err) = create_dir_all(parent)
            {
                eprintln!(
                    "[Xun][WARN][logger] create_dir_all failed {}: {}",
                    parent.display(),
                    err
                );
                continue;
            }

            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    if let Some(parent) = path.parent() {
                        cleanup_old_logs(parent);
                    }
                    let _ = ACTIVE_LOG_PATH.set(path);
                    return Mutex::new(Some(file));
                }
                Err(err) => {
                    eprintln!(
                        "[Xun][WARN][logger] open log failed {}: {}",
                        path.display(),
                        err
                    );
                }
            }
        }

        Mutex::new(None)
    })
}

fn configured_scope() -> LogScope {
    LOG_SCOPE.get().copied().unwrap_or(LogScope::Client)
}

fn preferred_log_dirs(scope: LogScope) -> Vec<PathBuf> {
    let mut dirs = Vec::<PathBuf>::new();

    match scope {
        LogScope::Server => {
            if let Some(program_data) = std::env::var_os("PROGRAMDATA") {
                push_unique_path(
                    &mut dirs,
                    PathBuf::from(program_data).join("Xun").join("logs"),
                );
            }
            if let Some(app_data) = std::env::var_os("APPDATA") {
                push_unique_path(&mut dirs, PathBuf::from(app_data).join("Xun").join("logs"));
            }
        }
        LogScope::Client => {
            if let Some(app_data) = std::env::var_os("APPDATA") {
                push_unique_path(&mut dirs, PathBuf::from(app_data).join("Xun").join("logs"));
            }
        }
    }

    push_unique_path(&mut dirs, std::env::temp_dir().join("Xun").join("logs"));
    dirs
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

fn cleanup_old_logs(dir: &Path) {
    let today = Local::now().date_naive();
    let Ok(entries) = read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        let Some(date) = parse_log_date(file_name) else {
            continue;
        };

        if today.signed_duration_since(date).num_days() < LOG_RETENTION_DAYS {
            continue;
        }

        if let Err(err) = remove_file(&path) {
            eprintln!(
                "[Xun][WARN][logger] remove old log failed {}: {}",
                path.display(),
                err
            );
        }
    }
}

fn parse_log_date(file_name: &str) -> Option<NaiveDate> {
    let date_text = file_name.strip_prefix("xun-")?.strip_suffix(".log")?;
    NaiveDate::parse_from_str(date_text, "%Y-%m-%d").ok()
}

fn now_human_time() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn write_line(level: &str, message: &str) {
    let line = format!(
        "[Xun][{level}][{}][{:?}] {message}",
        now_human_time(),
        std::thread::current().id()
    );

    eprintln!("{line}");

    if let Some(file) = sink().lock().as_mut() {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
}

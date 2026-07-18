use std::{
    env,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process,
    str::FromStr,
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime},
};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};

static LOGGER: OnceLock<Logger> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

impl FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DEBUG" => Ok(Self::Debug),
            "INFO" => Ok(Self::Info),
            "WARN" | "WARNING" => Ok(Self::Warn),
            "ERROR" => Ok(Self::Error),
            other => bail!("LOG_LEVEL geçersiz: {other}. DEBUG, INFO, WARN veya ERROR olmalı"),
        }
    }
}

#[derive(Debug, Clone)]
struct LoggerConfig {
    directory: PathBuf,
    minimum_level: LogLevel,
    console_enabled: bool,
    json_enabled: bool,
    flush_each_write: bool,
    retention_days: u64,
}

impl LoggerConfig {
    fn from_env() -> Result<Self> {
        let directory = env::var("LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("logs"));

        let minimum_level = env::var("LOG_LEVEL")
            .unwrap_or_else(|_| "INFO".to_string())
            .parse::<LogLevel>()?;

        Ok(Self {
            directory,
            minimum_level,
            console_enabled: env_bool("LOG_CONSOLE", true)?,
            json_enabled: env_bool("LOG_JSON", true)?,
            flush_each_write: env_bool("LOG_FLUSH_EACH_WRITE", true)?,
            retention_days: env_parse("LOG_RETENTION_DAYS", 14_u64)?,
        })
    }
}

#[derive(Debug)]
struct LogFiles {
    date: String,
    text: File,
    json: Option<File>,
    errors: File,
    scoring_csv: File,
}

#[derive(Debug)]
struct LoggerState {
    files: LogFiles,
}

#[derive(Debug)]
struct Logger {
    config: LoggerConfig,
    session_id: String,
    state: Mutex<LoggerState>,
}

#[derive(Debug)]
pub struct ScoringLogRecord<'a> {
    pub order_id: &'a str,
    pub token_id: &'a str,
    pub market_slug: &'a str,
    pub check_number: u64,
    pub order_age_ms: u128,
    pub request_latency_ms: u128,
    pub state: &'a str,
    pub in_grace: bool,
    pub consecutive_false: u32,
    pub consecutive_errors: u32,
}

impl Logger {
    fn new(config: LoggerConfig) -> Result<Self> {
        fs::create_dir_all(&config.directory).with_context(|| {
            format!(
                "Log klasörü oluşturulamadı: {}",
                config.directory.display()
            )
        })?;

        cleanup_old_logs(&config.directory, config.retention_days)?;

        let date = Utc::now().format("%Y-%m-%d").to_string();
        let files = open_log_files(&config, &date)?;
        let session_id = format!(
            "{}-pid{}",
            Utc::now().format("%Y%m%dT%H%M%S%.3fZ"),
            process::id()
        );

        Ok(Self {
            config,
            session_id,
            state: Mutex::new(LoggerState { files }),
        })
    }

    fn enabled(&self, level: LogLevel) -> bool {
        level >= self.config.minimum_level
    }

    fn rotate_if_needed(&self, state: &mut LoggerState) -> Result<()> {
        let current_date = Utc::now().format("%Y-%m-%d").to_string();
        if state.files.date != current_date {
            state.files = open_log_files(&self.config, &current_date)?;
            cleanup_old_logs(&self.config.directory, self.config.retention_days)?;
        }
        Ok(())
    }

    fn write(
        &self,
        level: LogLevel,
        component: &str,
        event: &str,
        message: &str,
        fields: Value,
        source_file: &str,
        source_line: u32,
    ) -> Result<()> {
        if !self.enabled(level) {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let message = sanitize_message(message);
        let thread_name = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();

        if self.config.console_enabled {
            let line = format!(
                "[{timestamp}] [{}] [{component}:{event}] {message}",
                level.as_str()
            );
            if level >= LogLevel::Warn {
                eprintln!("{line}");
            } else {
                println!("{line}");
            }
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.rotate_if_needed(&mut state)?;

        let text_line = format!(
            "{timestamp}\t{}\t{}\t{}\t{}\t{}:{}\t{}\n",
            level.as_str(),
            self.session_id,
            component,
            event,
            source_file,
            source_line,
            message.replace('\n', "\\n")
        );
        state
            .files
            .text
            .write_all(text_line.as_bytes())
            .context("Metin log dosyasına yazılamadı")?;

        let json_value = json!({
            "timestamp": timestamp,
            "level": level.as_str(),
            "session_id": self.session_id,
            "pid": process::id(),
            "thread": thread_name,
            "component": component,
            "event": event,
            "message": message,
            "source": {
                "file": source_file,
                "line": source_line,
            },
            "fields": fields,
        });

        let serialized = serde_json::to_string(&json_value)
            .context("Log kaydı JSON biçimine dönüştürülemedi")?;

        if let Some(json_file) = state.files.json.as_mut() {
            json_file
                .write_all(serialized.as_bytes())
                .and_then(|_| json_file.write_all(b"\n"))
                .context("JSONL log dosyasına yazılamadı")?;
        }

        if level >= LogLevel::Warn {
            state
                .files
                .errors
                .write_all(serialized.as_bytes())
                .and_then(|_| state.files.errors.write_all(b"\n"))
                .context("Hata JSONL log dosyasına yazılamadı")?;
        }

        if self.config.flush_each_write {
            flush_files(&mut state.files)?;
        }

        Ok(())
    }

    fn write_scoring(&self, record: &ScoringLogRecord<'_>) -> Result<()> {
        let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.rotate_if_needed(&mut state)?;

        let row = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}\n",
            csv_escape(&timestamp),
            csv_escape(&self.session_id),
            csv_escape(record.order_id),
            csv_escape(record.token_id),
            csv_escape(record.market_slug),
            record.check_number,
            record.order_age_ms,
            record.request_latency_ms,
            csv_escape(record.state),
            record.in_grace,
            record.consecutive_false,
            record.consecutive_errors,
        );

        state
            .files
            .scoring_csv
            .write_all(row.as_bytes())
            .context("Scoring CSV log dosyasına yazılamadı")?;

        if self.config.flush_each_write {
            state
                .files
                .scoring_csv
                .flush()
                .context("Scoring CSV log dosyası flush edilemedi")?;
        }

        Ok(())
    }
}

pub fn init_from_env() -> Result<()> {
    if LOGGER.get().is_some() {
        return Ok(());
    }

    let logger = Logger::new(LoggerConfig::from_env()?)?;
    LOGGER
        .set(logger)
        .map_err(|_| anyhow::anyhow!("Logger aynı süreçte ikinci kez başlatılamaz"))?;

    install_panic_hook();

    log_event(
        LogLevel::Info,
        "logger",
        "logger_initialized",
        "Dosya ve konsol log sistemi başlatıldı",
        json!({
            "log_directory": log_directory().map(|path| path.display().to_string()),
            "session_id": session_id(),
        }),
        file!(),
        line!(),
    );

    Ok(())
}

pub fn session_id() -> Option<String> {
    LOGGER.get().map(|logger| logger.session_id.clone())
}

pub fn log_directory() -> Option<PathBuf> {
    LOGGER
        .get()
        .map(|logger| logger.config.directory.clone())
}

pub fn empty_fields() -> Value {
    Value::Null
}

pub fn log_args(
    level: LogLevel,
    component: &str,
    event: &str,
    args: fmt::Arguments<'_>,
    fields: Value,
    source_file: &str,
    source_line: u32,
) {
    log_event(
        level,
        component,
        event,
        args.to_string(),
        fields,
        source_file,
        source_line,
    );
}

pub fn log_event(
    level: LogLevel,
    component: &str,
    event: &str,
    message: impl Into<String>,
    fields: Value,
    source_file: &str,
    source_line: u32,
) {
    let message = message.into();

    if let Some(logger) = LOGGER.get() {
        if let Err(error) = logger.write(
            level,
            component,
            event,
            &message,
            fields,
            source_file,
            source_line,
        ) {
            eprintln!(
                "[LOGGER-FAILURE] log yazılamadı: {error:#}; original_level={}; message={message}",
                level.as_str()
            );
        }
    } else {
        let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if level >= LogLevel::Warn {
            eprintln!(
                "[{timestamp}] [{}] [{component}:{event}] {message}",
                level.as_str()
            );
        } else {
            println!(
                "[{timestamp}] [{}] [{component}:{event}] {message}",
                level.as_str()
            );
        }
    }
}

pub fn record_scoring(record: &ScoringLogRecord<'_>) {
    if let Some(logger) = LOGGER.get() {
        if let Err(error) = logger.write_scoring(record) {
            eprintln!("[LOGGER-FAILURE] scoring CSV yazılamadı: {error:#}");
        }
    }
}

pub fn flush() {
    let Some(logger) = LOGGER.get() else {
        return;
    };

    let mut state = logger
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Err(error) = flush_files(&mut state.files) {
        eprintln!("[LOGGER-FAILURE] log dosyaları flush edilemedi: {error:#}");
    }
}

fn install_panic_hook() {
    let previous_hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |panic_info| {
        let location = panic_info
            .location()
            .map(|location| format!("{}:{}", location.file(), location.line()))
            .unwrap_or_else(|| "bilinmeyen konum".to_string());

        let payload = if let Some(message) = panic_info.payload().downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = panic_info.payload().downcast_ref::<String>() {
            message.clone()
        } else {
            "String olmayan panic payload".to_string()
        };

        log_event(
            LogLevel::Error,
            "panic",
            "unhandled_panic",
            format!("Yakalanmamış panic: {payload}"),
            json!({ "location": location }),
            file!(),
            line!(),
        );
        flush();
        previous_hook(panic_info);
    }));
}

fn open_log_files(config: &LoggerConfig, date: &str) -> Result<LogFiles> {
    fs::create_dir_all(&config.directory).with_context(|| {
        format!(
            "Log klasörü oluşturulamadı: {}",
            config.directory.display()
        )
    })?;

    let text_path = config.directory.join(format!("polybot-{date}.log"));
    let json_path = config.directory.join(format!("polybot-{date}.jsonl"));
    let errors_path = config
        .directory
        .join(format!("polybot-errors-{date}.jsonl"));
    let scoring_path = config
        .directory
        .join(format!("polybot-scoring-{date}.csv"));

    let text = open_append(&text_path)?;
    let json = if config.json_enabled {
        Some(open_append(&json_path)?)
    } else {
        None
    };
    let errors = open_append(&errors_path)?;

    let scoring_exists_and_has_content = scoring_path
        .metadata()
        .map(|metadata| metadata.len() > 0)
        .unwrap_or(false);
    let mut scoring_csv = open_append(&scoring_path)?;
    if !scoring_exists_and_has_content {
        scoring_csv
            .write_all(
                b"timestamp,session_id,order_id,token_id,market_slug,check_number,order_age_ms,request_latency_ms,state,in_grace,consecutive_false,consecutive_errors\n",
            )
            .context("Scoring CSV başlığı yazılamadı")?;
        scoring_csv.flush().context("Scoring CSV başlığı flush edilemedi")?;
    }

    Ok(LogFiles {
        date: date.to_string(),
        text,
        json,
        errors,
        scoring_csv,
    })
}

fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Log dosyası açılamadı: {}", path.display()))
}

fn flush_files(files: &mut LogFiles) -> Result<()> {
    files.text.flush().context("Metin log flush edilemedi")?;
    if let Some(json) = files.json.as_mut() {
        json.flush().context("JSONL log flush edilemedi")?;
    }
    files.errors.flush().context("Hata logu flush edilemedi")?;
    files
        .scoring_csv
        .flush()
        .context("Scoring CSV flush edilemedi")?;
    Ok(())
}

fn cleanup_old_logs(directory: &Path, retention_days: u64) -> Result<()> {
    if retention_days == 0 {
        return Ok(());
    }

    let maximum_age = Duration::from_secs(retention_days.saturating_mul(24 * 60 * 60));
    let now = SystemTime::now();

    for entry in fs::read_dir(directory)
        .with_context(|| format!("Log klasörü okunamadı: {}", directory.display()))?
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                eprintln!("[LOGGER-WARN] Log klasörü girdisi okunamadı: {error}");
                continue;
            }
        };

        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        if !name.starts_with("polybot-") || !path.is_file() {
            continue;
        }

        let modified = match entry.metadata().and_then(|metadata| metadata.modified()) {
            Ok(modified) => modified,
            Err(error) => {
                eprintln!(
                    "[LOGGER-WARN] Log dosyası metadata okunamadı ({}): {error}",
                    path.display()
                );
                continue;
            }
        };

        let age = now.duration_since(modified).unwrap_or_default();
        if age > maximum_age {
            if let Err(error) = fs::remove_file(&path) {
                eprintln!(
                    "[LOGGER-WARN] Eski log dosyası silinemedi ({}): {error}",
                    path.display()
                );
            }
        }
    }

    Ok(())
}

fn sanitize_message(message: &str) -> String {
    // Kod private key değerini loglamıyor. Bu ek maskeleme, gelecekte yanlışlıkla
    // "POLYMARKET_PRIVATE_KEY=..." biçiminde bir mesaj üretilirse değeri gizler.
    redact_assignment(message, "POLYMARKET_PRIVATE_KEY")
}

fn redact_assignment(message: &str, key: &str) -> String {
    let marker = format!("{key}=");
    let Some(start) = message.find(&marker) else {
        return message.to_string();
    };

    let value_start = start + marker.len();
    let value_end = message[value_start..]
        .find(|character: char| character.is_whitespace() || character == ',' || character == ';')
        .map(|offset| value_start + offset)
        .unwrap_or(message.len());

    let mut redacted = String::with_capacity(message.len());
    redacted.push_str(&message[..value_start]);
    redacted.push_str("***REDACTED***");
    redacted.push_str(&message[value_end..]);
    redacted
}

fn csv_escape(value: &str) -> String {
    if value
        .chars()
        .any(|character| matches!(character, ',' | '"' | '\n' | '\r'))
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{name} true/false biçiminde olmalı"),
    }
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };

    let value = value.trim();
    if value.is_empty() {
        return Ok(default);
    }

    value
        .parse::<T>()
        .with_context(|| format!("{name} geçerli bir değer değil"))
}

#[macro_export]
macro_rules! log_debug {
    ($event:expr, $($arg:tt)*) => {{
        $crate::logger::log_args(
            $crate::logger::LogLevel::Debug,
            module_path!(),
            $event,
            format_args!($($arg)*),
            $crate::logger::empty_fields(),
            file!(),
            line!(),
        );
    }};
}

#[macro_export]
macro_rules! log_info {
    ($event:expr, $($arg:tt)*) => {{
        $crate::logger::log_args(
            $crate::logger::LogLevel::Info,
            module_path!(),
            $event,
            format_args!($($arg)*),
            $crate::logger::empty_fields(),
            file!(),
            line!(),
        );
    }};
}

#[macro_export]
macro_rules! log_warn {
    ($event:expr, $($arg:tt)*) => {{
        $crate::logger::log_args(
            $crate::logger::LogLevel::Warn,
            module_path!(),
            $event,
            format_args!($($arg)*),
            $crate::logger::empty_fields(),
            file!(),
            line!(),
        );
    }};
}

#[macro_export]
macro_rules! log_error {
    ($event:expr, $($arg:tt)*) => {{
        $crate::logger::log_args(
            $crate::logger::LogLevel::Error,
            module_path!(),
            $event,
            format_args!($($arg)*),
            $crate::logger::empty_fields(),
            file!(),
            line!(),
        );
    }};
}

use anyhow::{Result, anyhow};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::{fs, path::PathBuf};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_rolling_file::RollingFileAppenderBase;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::LevelFilter, fmt, layer::SubscriberExt};

static LOG_GUARDS: OnceCell<Vec<WorkerGuard>> = OnceCell::new();

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct Log {
    pub file: File,
    pub console: Console,
}

impl Default for Log {
    fn default() -> Self {
        Self { file: File::default(), console: Console::default() }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct File {
    pub level: Level,
    pub format: Format,
    pub dir: PathBuf,
    pub file: String,
    pub error_file: Option<String>,
    pub rotation_size: u64,
    pub rotation_days: usize,
}

impl Default for File {
    fn default() -> Self {
        Self {
            level: Level::Info,
            format: Format::Json,
            dir: PathBuf::from("./logs"),
            file: "iotmq.log".into(),
            error_file: None,
            rotation_size: 100,
            rotation_days: 30,
        }
    }
}

impl File {
    fn appender(&self, filename: &str) -> Result<RollingFileAppenderBase> {
        let filename = self.dir.join(filename).to_string_lossy().into_owned();
        let size = self.rotation_size * 1024 * 1024;
        let appender = RollingFileAppenderBase::builder()
            .filename(filename)
            .max_filecount(self.rotation_days)
            .condition_max_file_size(size)
            .build()
            .map_err(|e| anyhow!("{e}"))?;
        Ok(appender)
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct Console {
    pub enable: bool,
    pub level: Level,
    pub format: Format,
}

impl Default for Console {
    fn default() -> Self {
        Self { enable: false, level: Level::Warn, format: Format::Text }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    fn filter(&self) -> LevelFilter {
        match self {
            Level::Trace => LevelFilter::TRACE,
            Level::Debug => LevelFilter::DEBUG,
            Level::Info => LevelFilter::INFO,
            Level::Warn => LevelFilter::WARN,
            Level::Error => LevelFilter::ERROR,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    Text,
}

/// init log
pub fn init(log: &Log) -> Result<()> {
    fs::create_dir_all(&log.file.dir)?;

    // main log layer
    let main_appender = log.file.appender(&log.file.file)?;
    let (main_writer, main_guard) = main_appender.get_non_blocking_appender();
    let mut guards = vec![main_guard];
    let main_layer =
        fmt::layer().json().with_writer(main_writer).with_filter(log.file.level.filter());

    // error log layer
    let error_layer = if let Some(error_file) = &log.file.error_file {
        let error_appender = log.file.appender(error_file)?;
        let (error_writer, error_guard) = error_appender.get_non_blocking_appender();
        let error_layer =
            fmt::layer().json().with_writer(error_writer).with_filter(LevelFilter::ERROR);
        guards.push(error_guard);
        Some(error_layer)
    } else {
        None
    };

    // console log layer
    let console_layer = if log.console.enable {
        let console_layer = fmt::layer()
            .with_line_number(true)
            .with_ansi(true)
            .with_filter(log.console.level.filter());
        Some(console_layer)
    } else {
        None
    };

    tracing_subscriber::registry().with(main_layer).with(error_layer).with(console_layer).init();
    let _ = LOG_GUARDS.set(guards);
    Ok(())
}

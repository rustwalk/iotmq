use anyhow::{Result, anyhow};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::{fs, path::PathBuf};
use tracing::Subscriber;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_rolling_file::RollingFileAppenderBase;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::LevelFilter, fmt, layer::SubscriberExt, registry};

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

impl Log {
    /// init log
    pub fn init(log: &Log) -> Result<()> {
        fs::create_dir_all(&log.file.dir)?;

        let mut guards = Vec::new();

        // Create the main log layer
        let (main_writer, main_guard) =
            log.file.appender(&log.file.filename)?.get_non_blocking_appender();
        guards.push(main_guard);
        let main_layer = layer(log.file.format, main_writer, log.file.level.filter());

        // Create the error log layer
        let error_layer = if let Some(error_file) = &log.file.error_filename {
            let (error_writer, error_guard) =
                log.file.appender(error_file)?.get_non_blocking_appender();
            guards.push(error_guard);
            let error_layer = layer(log.file.format, error_writer, LevelFilter::ERROR);
            Some(error_layer)
        } else {
            None
        };

        // Create the console log layer
        let console_layer = if log.console.enable {
            let console_layer =
                layer(log.console.format, std::io::stdout, log.console.level.filter());
            Some(console_layer)
        } else {
            None
        };

        tracing_subscriber::registry()
            .with(main_layer)
            .with(error_layer)
            .with(console_layer)
            .try_init()?;
        let _ = LOG_GUARDS.set(guards);
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct File {
    pub level: Level,
    pub format: Format,
    pub dir: PathBuf,
    pub filename: String,
    pub error_filename: Option<String>,
    pub rotation_size: u64,
    pub rotation_days: usize,
}

impl Default for File {
    fn default() -> Self {
        Self {
            level: Level::Info,
            format: Format::Json,
            dir: PathBuf::from("./logs"),
            filename: "iotmq.log".into(),
            error_filename: None,
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

#[derive(Deserialize, Debug, Clone, Copy)]
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

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    Text,
}

fn layer<W, S>(format: Format, writer: W, filter: LevelFilter) -> Box<dyn Layer<S> + Send + Sync>
where
    W: for<'writer> fmt::MakeWriter<'writer> + Send + Sync + 'static,
    S: Subscriber + for<'a> registry::LookupSpan<'a>,
{
    match format {
        Format::Text => fmt::layer()
            .with_ansi(true)
            .with_line_number(true)
            .with_writer(writer)
            .with_filter(filter)
            .boxed(),
        Format::Json => {
            fmt::layer().json().flatten_event(true).with_writer(writer).with_filter(filter).boxed()
        }
    }
}

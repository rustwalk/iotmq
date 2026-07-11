use crate::Listener;
use crate::LogConfig;
use crate::WebConfig;
use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use config::{Environment, File};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::Path;
use std::{env, fs, path::PathBuf, sync::Arc};
use tracing::info;

const ENV_CONFIG: &str = "IOTMQ_CONFIG";
const CONFIG_FILE: &str = "iotmq.toml";
const CONFIG_DIR: &str = "./config";

#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: LogConfig,
    pub web: WebConfig,
    #[serde(rename = "listener", default = "Config::default_listeners")]
    pub listeners: Vec<Listener>,
}

impl Config {
    // Validate config
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    // Default listeners
    fn default_listeners() -> Vec<Listener> {
        vec![Listener::default()]
    }
}

pub struct ConfigManager {
    config: ArcSwap<Config>,
    path: PathBuf,
}

impl ConfigManager {
    /// Init config manager
    pub fn init(path: &Path) -> Result<Self> {
        let manager =
            Self { config: ArcSwap::new(Arc::new(Self::load(&path)?)), path: path.to_path_buf() };
        Ok(manager)
    }

    /// Read runtime config
    pub fn read(&self) -> Arc<Config> {
        self.config.load_full()
    }

    /// Reload configs
    pub fn reload(&self) -> Result<()> {
        let config = Self::load(&self.path)?;
        config.validate()?;
        self.config.store(Arc::new(config));
        info!("Reloaded config: {:?}", self.path);
        Ok(())
    }

    /// Get static config path
    pub fn static_config(config: Option<PathBuf>) -> Result<PathBuf> {
        // Command config
        if let Some(path) = config {
            if !path.is_file() {
                bail!("Config file does not exist: {}", path.display());
            }
            return Ok(path);
        }

        // Environment variable
        if let Ok(path) = env::var(ENV_CONFIG) {
            let path = PathBuf::from(path);
            if !path.is_file() {
                bail!("Config file does not exist: {}", path.display());
            }
            return Ok(path);
        }

        // CONFIG_DIR or /etc/iotmq
        let paths = [
            PathBuf::from(CONFIG_DIR).join(CONFIG_FILE),
            PathBuf::from("/etc/iotmq").join(CONFIG_FILE),
        ];
        for path in paths {
            if path.is_file() {
                return Ok(path);
            }
        }

        bail!("Config file does not exist: {}", CONFIG_FILE);
    }

    /// Get dynamic configs path
    fn dynamic_configs() -> Result<Vec<PathBuf>> {
        let dir = PathBuf::from(CONFIG_DIR);
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
            return Ok(vec![]);
        }

        let paths = fs::read_dir(dir)?
            .filter_map(|file| {
                let file = file.ok()?;
                let path = file.path();

                if !path.is_file() {
                    return None;
                }

                if path.file_name() == Some(OsStr::new(CONFIG_FILE)) {
                    return None;
                }

                Some(path)
            })
            .collect();
        Ok(paths)
    }

    /// Load configs
    fn load(static_config: &Path) -> Result<Config> {
        let mut builder = config::Config::builder();

        // static config
        builder = builder.add_source(File::from(static_config).required(true));

        // dynamic configs
        for path in Self::dynamic_configs()? {
            builder = builder.add_source(File::from(path).required(false));
        }

        // environment variable
        builder = builder.add_source(Environment::with_prefix("IOTMQ").separator("__"));

        builder.build()?.try_deserialize::<Config>().map_err(|e| e.into())
    }
}

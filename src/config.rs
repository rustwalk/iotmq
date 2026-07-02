use crate::Log;
use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use config::{Environment, File};
use serde::Deserialize;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::{env, fs, path::Path, path::PathBuf, sync::Arc};

const ENV_CONFIG: &str = "IOTMQ_CONFIG";
const CONFIG_FILE: &str = "iotmq.toml";
const CONFIG_DIR: &str = "./config";
const CONFIG_EXTS: &[&str] = &["toml", "yaml", "json", "ini"];

#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: Log,
    //pub http: Http,
    //pub mqtt: Mqtt,
    //#[serde(rename = "listener")]
    //pub listeners: HashMap<String, Listener>,
}

#[derive(Deserialize, Debug)]
pub struct Http {}

#[derive(Deserialize, Debug)]
pub struct Mqtt {}

#[derive(Deserialize, Debug)]
pub struct Listener {}

impl Config {
    // Validate config
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

pub struct ConfigManager {
    config: ArcSwap<Config>,
}

impl ConfigManager {
    /// Init config manager
    pub fn init() -> Result<Self> {
        let manager =
            Self { config: ArcSwap::new(Arc::new(Config { log: Log { ..Default::default() } })) };
        manager.reload()?;
        Ok(manager)
    }

    /// Read runtime config
    pub fn read(&self) -> Arc<Config> {
        self.config.load_full()
    }

    /// Reload configs
    pub fn reload(&self) -> Result<()> {
        let config = Self::load()?;
        config.validate()?;
        self.config.store(Arc::new(config));
        Ok(())
    }

    /// Get static config path
    fn static_config() -> Result<PathBuf> {
        if let Ok(path) = env::var(ENV_CONFIG) {
            let path = PathBuf::from(path);
            if !path.is_file() {
                bail!("Config file does not exist: {}", path.display());
            }
            return Ok(path);
        }

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

                let ext = path.extension()?.to_str()?.to_ascii_lowercase();
                if !CONFIG_EXTS.contains(&ext.as_str()) {
                    return None;
                }

                Some(path)
            })
            .collect();
        Ok(paths)
    }

    /// Load configs
    fn load() -> Result<Config> {
        let mut builder = config::Config::builder();

        // static config
        let static_config = Self::static_config()?;
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

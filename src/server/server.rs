use crate::{ConfigManager, Context, logger};
use anyhow::Result;
use tracing::info;

pub struct Server {}

impl Server {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run(&mut self) -> Result<()> {
        let config = ConfigManager::init()?;
        logger::init(&config.read().log)?;
        let ctx = Context::init(config);
        info!("server run");
        Ok(())
    }

    pub fn stop() {}
}

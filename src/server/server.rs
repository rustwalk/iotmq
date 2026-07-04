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
        let ctx = Context::init(config);
        logger::init(&ctx.config().log)?;
        info!("server run");
        Ok(())
    }
}

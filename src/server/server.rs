use crate::{ConfigManager, Context, logger};
use anyhow::Result;
use tracing::info;

pub struct Server;

impl Server {
    pub fn start() -> Result<()> {
        let config = ConfigManager::init()?;
        logger::init(&config.read().log)?;

        info!("server starting...");

        let ctx = Context::init(config);

        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(run())?;

        info!("server stopped");

        Ok(())
    }

    pub fn stop() {}
}

async fn run() -> Result<()> {
    Ok(())
}

use crate::server::web::WebServer;
use crate::{ConfigManager, Context, logger};
use anyhow::Result;
use std::path::PathBuf;
use tracing::{error, info};

pub struct Server;

impl Server {
    pub fn start(config: Option<PathBuf>) -> Result<()> {
        let config_path = ConfigManager::static_config(config)?;
        let config = ConfigManager::init(config_path)?;
        logger::init(&config.read().log)?;

        info!("server starting...");

        let ctx = Context::init(config);

        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let web_ctx = ctx.clone();
            let web_task = tokio::spawn(async move {
                if let Err(e) = WebServer::run(ctx).await {
                    error!("web server error: {}", e);
                }
            });

            let _ = tokio::join!(web_task);
        });

        info!("server stopped");

        Ok(())
    }

    pub fn stop() {}

    pub fn reload() {
        println!("Reloading...");
    }

    pub fn restart() {}
}

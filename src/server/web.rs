use crate::Context;
use anyhow::Result;
use tracing::info;

pub struct WebServer;

impl WebServer {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("Starting web server");
        Ok(())
    }
}

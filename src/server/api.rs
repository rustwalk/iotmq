use crate::Context;
use anyhow::Result;
use tracing::info;

pub struct ApiServer;

impl ApiServer {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("Starting api server");
        Ok(())
    }
}

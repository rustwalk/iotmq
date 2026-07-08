use crate::Context;
use anyhow::Result;
use tracing::info;

pub struct WebServer;

impl WebServer {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("Web server started");
        let mut rx = ctx.subscribe();
        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    break;
                }
            }
        }
        info!("Web server stopped");
        Ok(())
    }
}

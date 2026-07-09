use crate::Context;
use crate::context::Event;
use anyhow::Result;
use axum::Router;
use serde::Deserialize;
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;

const WEB_DIR: &str = "./web/dist";

#[derive(Debug, Deserialize)]
pub struct WebConfig {
    #[serde(default = "WebConfig::default_addr")]
    pub addr: String,
}

impl WebConfig {
    fn default_addr() -> String {
        "[::]:8080".into()
    }
}
pub struct WebServer;

impl WebServer {
    pub async fn run(ctx: Context) -> Result<()> {
        let index = format!("{}/{}", WEB_DIR, "index.html");
        let spa = ServeDir::new(WEB_DIR).not_found_service(ServeFile::new(index));
        let router = Router::new().fallback_service(spa);

        let config = ctx.config();
        let ln = TcpListener::bind(&config.web.addr).await?;
        info!("Web server started: {}", config.web.addr);
        axum::serve(ln, router)
            .with_graceful_shutdown(async move {
                let mut rx = ctx.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(Event::Reload) => (),
                        _ => break,
                    }
                }
            })
            .await?;

        info!("Web server stopped");
        Ok(())
    }
}

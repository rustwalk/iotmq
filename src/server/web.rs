use super::Context;
use anyhow::Result;
use axum::Router;
use serde::Deserialize;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;

const WEB_DIR: &str = "./web/dist";

#[derive(Debug, Deserialize)]
pub struct WebConfig {
    #[serde(default = "WebConfig::default_addr")]
    pub addr: SocketAddr,
}

impl WebConfig {
    fn default_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8080)
    }
}
pub struct WebServer;

impl WebServer {
    pub async fn run(ctx: Context) -> Result<()> {
        let mut rx = ctx.subscribe();
        let index = format!("{}/{}", WEB_DIR, "index.html");
        let spa = ServeDir::new(WEB_DIR).not_found_service(ServeFile::new(index));
        let router = Router::new().fallback_service(spa);

        let config = ctx.config();
        let ln = TcpListener::bind(&config.web.addr).await?;
        info!("Web server started: {}", config.web.addr);
        axum::serve(ln, router)
            .with_graceful_shutdown(async move {
                let _ = Context::shutdown(&mut rx).await;
            })
            .await?;

        info!("Web server stopped");
        Ok(())
    }
}

use crate::Context;
use anyhow::Result;
use serde::Deserialize;
use std::net::SocketAddr;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct Listener {
    pub protocol: Protocol,
    pub addr: SocketAddr,
    #[serde(default)]
    pub cert: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub proxy_protocol: bool,
    #[serde(default)]
    pub max_connections: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Tls,
    Ws,
    Wss,
}
pub struct Broker;

impl Broker {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("Starting mqtt server");
        Ok(())
    }
}

use crate::Context;
use crate::context::Event;
use anyhow::Result;
use serde::Deserialize;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{debug, error, info};

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Tls,
    Ws,
    Wss,
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let protocol = match self {
            Protocol::Tcp => "TCP",
            Protocol::Tls => "TLS",
            Protocol::Ws => "WS",
            Protocol::Wss => "WSS",
        };
        f.write_str(protocol)
    }
}
#[derive(Debug, Deserialize, Clone)]
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

impl Default for Listener {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            addr: SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 1883),
            cert: None,
            key: None,
            proxy_protocol: false,
            max_connections: 0,
        }
    }
}

impl Listener {
    /// Tcp accept
    async fn accept<H, F>(&self, ctx: Context, handler: H) -> Result<Event>
    where
        H: Fn(TcpStream) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let listener = TcpListener::bind(self.addr).await?;
        info!("MQTT Broker {} listening on {}", self.protocol, self.addr);

        let mut rx = ctx.subscribe();
        loop {
            tokio::select! {
                event = Context::shutdown(&mut rx) => return Ok(event),
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("MQTT Broker {} accepted new connection from {}", self.protocol, addr);
                            //let _ctx = ctx.clone();
                            let handler = handler.clone();
                            let protocol = self.protocol;
                            tokio::spawn(async move {
                                if let Err(e) = handler(stream).await{
                                    debug!("MQTT Broker {} connect error: {}", protocol,e);
                                }
                            });
                        }

                        Err(e) => {
                            match e.kind() {
                                ErrorKind::ConnectionAborted
                                | ErrorKind::Interrupted  => {
                                    debug!("MQTT Broker {} accept interrupted: {}", self.protocol, e);
                                    continue;
                                }
                                _ => return Err(e.into())
                            }
                        }
                    }
                }
            }
        }
    }

    /// Tcp listen
    async fn tcp(&self, ctx: Context) -> Result<Event> {
        self.accept(ctx, |stream| async move {
            let _stream = stream;
            Ok(())
        })
        .await
    }

    /// TLS listen
    async fn tls(&self, ctx: Context) -> Result<Event> {
        self.accept(ctx, |stream| async move {
            let _stream = stream;
            Ok(())
        })
        .await
    }

    /// Websocket listen
    async fn ws(&self, ctx: Context) -> Result<Event> {
        self.accept(ctx, |stream| async move {
            let _stream = stream;
            Ok(())
        })
        .await
    }

    /// Websocket TLS listen
    async fn wss(&self, ctx: Context) -> Result<Event> {
        self.accept(ctx, |stream| async move {
            let _stream = stream;
            Ok(())
        })
        .await
    }
}

pub struct Broker;

impl Broker {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("MQTT Broker Starting...");
        let listeners = ctx.config().listeners.clone();

        let mut tasks = JoinSet::new();
        for listener in listeners {
            let rx = ctx.subscribe();
            let ctx = ctx.clone();
            tasks.spawn(async move {
                (
                    match listener.protocol {
                        Protocol::Tcp => listener.tcp(ctx).await,
                        Protocol::Tls => listener.tls(ctx).await,
                        Protocol::Ws => listener.ws(ctx).await,
                        Protocol::Wss => listener.wss(ctx).await,
                    },
                    listener,
                )
            });
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((result, listener)) => match result {
                    Ok(event) => {
                        info!("MQTT Broker listener stop: {} {:?}", listener.addr, event);
                    }
                    Err(e) => {
                        error!(
                            "MQTT Broker {} listener error: {} {}",
                            listener.protocol, listener.addr, e
                        );
                        ctx.stop();
                        tasks.abort_all();
                        return Err(e);
                    }
                },
                Err(e) => {
                    error!("MQTT Broker listener task error: {}", e);
                    ctx.stop();
                    tasks.abort_all();
                    return Err(e.into());
                }
            }
        }

        info!("MQTT Broker Stopped");
        Ok(())
    }
}

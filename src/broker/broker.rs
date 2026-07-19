use super::Session;
use crate::Context;
use anyhow::{Context as _, Error, Result};
use async_tungstenite::tokio::accept_hdr_async;
use async_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use async_tungstenite::tungstenite::http::HeaderValue;
use serde::Deserialize;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::{debug, info};
use ws_stream_tungstenite::WsStream;

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
    async fn accept<H, F>(&self, ctx: Context, handler: H) -> Result<()>
    where
        H: Fn(TcpStream, SocketAddr) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let listener = TcpListener::bind(self.addr).await?;
        info!("MQTT Broker {} listening on {}", self.protocol, self.addr);

        let mut rx = ctx.subscribe();
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = Context::shutdown(&mut rx) => {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                    return Ok(());
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("MQTT Broker {} accepted new connection from {}", self.protocol, addr);
                            let handler = handler.clone();
                            let protocol = self.protocol;
                            connections.spawn(async move {
                                (handler(stream, addr).await, protocol, addr)
                            });
                        }

                        Err(e) => {
                            match e.kind() {
                                ErrorKind::ConnectionAborted
                                | ErrorKind::Interrupted  => {
                                    debug!("MQTT Broker {} accept interrupted on {}: {:#}", self.protocol, self.addr, e);
                                    continue;
                                }
                                _ => return Err(e).context(format!("{} accept failed on {}", self.protocol, self.addr)),
                            }
                        }
                    }
                }

                result = connections.join_next(), if !connections.is_empty() => {
                    match result {
                        Some(Ok((Ok(()), protocol, addr))) => {
                            debug!("MQTT Broker {} connection from {} closed",protocol, addr);
                        }
                        Some(Ok((Err(e), protocol, addr))) => {
                            debug!("MQTT Broker {} connection error from {}: {:#}",protocol, addr, e);
                        }
                        Some(Err(e)) if !e.is_cancelled() => {
                            debug!("MQTT Broker connection task failed: {}", e);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// TLS acceptor
    fn tls_acceptor(&self) -> Result<TlsAcceptor> {
        let key_path =
            self.key.as_ref().context(format!("{} listener requires `key`", self.protocol))?;
        let cert_path =
            self.cert.as_ref().context(format!("{} listener requires `cert`", self.protocol))?;

        let key = PrivateKeyDer::from_pem_file(key_path)
            .context(format!("Failed to open TLS private key: {}", key_path))?;
        let certs = CertificateDer::pem_file_iter(cert_path)
            .context(format!("Failed to open TLS certificate: {}", cert_path))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(format!("Failed to parse TLS certificate: {}", cert_path))?;

        let config = ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        Ok(acceptor)
    }

    /// Tcp listen
    async fn tcp(&self, ctx: Context) -> Result<()> {
        let move_ctx = ctx.clone();
        self.accept(ctx, move |stream, addr| {
            let ctx = move_ctx.clone();
            async move {
                let stream = Box::new(stream);
                let session = Session::connect(ctx, stream, addr).await?;
                session.run().await
            }
        })
        .await
    }

    /// TLS listen
    async fn tls(&self, ctx: Context) -> Result<()> {
        let acceptor = self.tls_acceptor()?;
        let move_ctx = ctx.clone();

        self.accept(ctx, move |stream, addr| {
            let acceptor = acceptor.clone();
            let ctx = move_ctx.clone();
            async move {
                let stream = acceptor.accept(stream).await?;
                let stream = Box::new(stream);
                let session = Session::connect(ctx, stream, addr).await?;
                session.run().await
            }
        })
        .await
    }

    /// Websocket listen
    async fn ws(&self, ctx: Context) -> Result<()> {
        let move_ctx = ctx.clone();

        self.accept(ctx, move |stream, addr| {
            let ctx = move_ctx.clone();
            async move {
                let stream = accept_hdr_async(stream, ws_callback).await?;
                let stream = Box::new(WsStream::new(stream));
                let session = Session::connect(ctx, stream, addr).await?;
                session.run().await
            }
        })
        .await
    }

    /// Websocket TLS listen
    async fn wss(&self, ctx: Context) -> Result<()> {
        let acceptor = self.tls_acceptor()?;
        let move_ctx = ctx.clone();

        self.accept(ctx, move |stream, addr| {
            let acceptor = acceptor.clone();
            let ctx = move_ctx.clone();
            async move {
                let stream = acceptor.accept(stream).await?;
                let stream = accept_hdr_async(stream, ws_callback).await?;
                let stream = Box::new(WsStream::new(stream));
                let session = Session::connect(ctx, stream, addr).await?;
                session.run().await
            }
        })
        .await
    }
}

/// WS callback
fn ws_callback(request: &Request, mut response: Response) -> Result<Response, ErrorResponse> {
    let protocol = request
        .headers()
        .get("Sec-WebSocket-Protocol")
        .ok_or(ErrorResponse::new(Some("Sec-WebSocket-Protocol header missing".into())))?;

    if protocol == "mqtt" {
        response.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_static("mqtt"));
    }

    Ok(response)
}

pub struct Broker;

impl Broker {
    pub async fn run(ctx: Context) -> Result<()> {
        info!("MQTT Broker Starting...");
        let listeners = ctx.config().listeners.clone();

        let mut tasks = JoinSet::new();
        for listener in listeners {
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

        let mut error = None;

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((result, listener)) => match result {
                    Ok(_) => {
                        info!(
                            "MQTT Broker {} listener stopped: {}",
                            listener.protocol, listener.addr
                        );
                    }
                    Err(e) => {
                        error = Some(e.context(format!(
                            "{} listener stopped: {}",
                            listener.protocol, listener.addr
                        )));
                        break;
                    }
                },
                Err(e) => {
                    error = Some(Error::new(e).context("Listener task failed"));
                    break;
                }
            }
        }

        if let Some(e) = error {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Err(e);
        }

        info!("MQTT Broker Stopped");
        Ok(())
    }
}

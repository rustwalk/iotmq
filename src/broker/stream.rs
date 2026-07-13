use super::Session;
use crate::Context;
use crate::mqtt::Codec;
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub trait IO: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IO for T {}

pub struct Stream {
    framed: Framed<Box<dyn IO>, Codec>,
    addr: SocketAddr,
}

impl Stream {
    pub async fn connect(ctx: Context, io: Box<dyn IO>, addr: SocketAddr) -> Result<Session> {
        let mut stream = Self { framed: Framed::new(io, Codec {}), addr };
        match stream.framed.next().await {
            Some(Ok(packet)) => {
                println!("Connected to {:?}", packet);
            }
            Some(Err(e)) => {
                println!("{}", e);
            }
            _ => {}
        }
        Ok(Session {})
    }
}

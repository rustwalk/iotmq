use super::Session;
use crate::Context;
use crate::mqtt::{Codec, ConnAck, Error, Packet, ReasonCode};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_util::codec::Framed;

pub trait IO: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IO for T {}

pub struct Stream {
    framed: Framed<Box<dyn IO>, Codec>,
    addr: SocketAddr,
}

impl Stream {
    /// MQTT connect
    pub async fn connect(ctx: Context, io: Box<dyn IO>, addr: SocketAddr) -> Result<Session> {
        let mut stream = Self { framed: Framed::new(io, Codec {}), addr };

        let (packet, _) = timeout(Duration::from_secs(10), stream.recv()).await??;
        let connect = match packet {
            Packet::Connect(connect) => connect,
            _ => return Err(Error::ProtocolError("First packet must be CONNECT".into()).into()),
        };

        let mut connack = ConnAck::default();
        connack.version = connect.protocol_version;
        stream.send(Packet::ConnAck(connack)).await?;

        Ok(Session::new(ctx, stream, connect))
    }

    /// MQTT send packet
    pub async fn send(&mut self, packet: Packet) -> Result<()> {
        self.framed.send(packet).await?;
        Ok(())
    }

    /// MQTT recv packet
    pub async fn recv(&mut self) -> Result<(Packet, u32)> {
        self.framed.next().await.ok_or(Error::ConnectionClosed)?.map_err(Into::into)
    }
}

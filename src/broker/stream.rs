use super::Session;
use crate::Context;
use crate::mqtt::{
    Codec, ConnAck, ConnAckProperties, Connect, ConnectProperties, Error, Packet, Version,
};
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
        let mut stream = Self { framed: Framed::new(io, Codec::default()), addr };

        let (packet, packet_size) = timeout(Duration::from_secs(10), stream.recv()).await??;
        let mut connect = match packet {
            Packet::Connect(connect) => connect,
            _ => return Err(Error::ProtocolError("First packet must be CONNECT".into()).into()),
        };

        // Set codec version
        stream.framed.codec_mut().version(connect.protocol_version);

        let max_packet_size = ctx.config().mqtt.max_packet_size();
        if packet_size > max_packet_size {
            let error = Error::PacketTooLarge;
            if connect.protocol_version == Version::V5 {
                stream.send_error(connect.protocol_version, &error).await?;
            }
            return Err(error.into());
        }

        let assigned = match resolve_client_id(&mut connect) {
            Ok(assigned) => assigned,
            Err(e) => {
                stream.send_error(connect.protocol_version, &e).await?;
                return Err(e.into());
            }
        };

        let properties = assigned.then(|| {
            let mut properties = ConnAckProperties::default();
            properties.assigned_client_identifier = Some(connect.client_id.clone());
            properties
        });
        stream.send_ok(connect.protocol_version, properties).await?;
        Ok(Session::new(ctx, stream, connect))
    }

    /// MQTT send packet
    pub async fn send(&mut self, packet: Packet) -> Result<(), Error> {
        self.framed.send(packet).await?;
        Ok(())
    }

    /// MQTT recv packet
    pub async fn recv(&mut self) -> Result<(Packet, u32), Error> {
        self.framed.next().await.ok_or(Error::ConnectionClosed)?.map_err(Into::into)
    }

    /// Send ConnAck
    pub async fn send_error(&mut self, version: Version, e: &Error) -> Result<(), Error> {
        let mut connack = ConnAck::default();
        connack.reason_code = e.into();
        self.send(Packet::ConnAck(connack)).await
    }

    /// Send Ok
    pub async fn send_ok(
        &mut self,
        version: Version,
        properties: Option<ConnAckProperties>,
    ) -> Result<(), Error> {
        let mut connack = ConnAck::default();
        connack.properties = properties;
        self.send(Packet::ConnAck(connack)).await
    }
}

fn resolve_client_id(connect: &mut Connect) -> Result<bool, Error> {
    if connect.client_id.is_empty() {
        connect.client_id = uuid::Uuid::new_v4().simple().to_string();
        return Ok(true);
    }
    Ok(false)
}

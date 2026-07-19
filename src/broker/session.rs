use crate::Context;
use crate::mqtt::*;
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_util::codec::Framed;

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
type IO = Box<dyn Stream>;

pub struct Session {
    ctx: Context,
    framed: Framed<IO, Codec>,
    rx: mpsc::Receiver<Packet>,
    keepalive: u16,
}

impl Session {
    /// MQTT connect
    pub async fn connect(ctx: Context, io: IO, addr: SocketAddr) -> Result<Session> {
        let mut framed = Framed::new(io, Codec::default());

        // Receive first packet
        let (packet, packet_size) = timeout(Duration::from_secs(10), framed.next())
            .await?
            .ok_or(Error::ConnectionClosed)??;

        // CONNECT packet
        let mut connect = match packet {
            Packet::Connect(connect) => connect,
            _ => {
                return Err(Error::ProtocolError("First packet must be CONNECT".to_string()).into());
            }
        };

        // max packet size
        let max_packet_size = ctx.config().mqtt.max_packet_size();
        if packet_size > max_packet_size {
            let error = Error::PacketTooLarge;
            if connect.protocol_version == Version::V5 {
                send_error(&mut framed, &error).await?;
            }
            return Err(error.into());
        }

        // client id
        let assigned = match resolve_client_id(&mut connect) {
            Ok(assigned) => assigned,
            Err(e) => {
                send_error(&mut framed, &e).await?;
                return Err(e.into());
            }
        };

        // ConnAck
        let properties = assigned.then(|| {
            let mut properties = ConnAckProperties::default();
            properties.assigned_client_identifier = Some(connect.client_id.clone());
            properties
        });
        send_ok(&mut framed, properties).await?;

        // new Session
        let (tx, rx) = mpsc::channel(128);
        Ok(Self { ctx, framed, rx, keepalive: connect.keepalive })
    }

    /// Send packet
    pub async fn send(&mut self, packet: Packet) -> Result<(), Error> {
        self.framed.send(packet).await
    }

    /// Receive packet
    pub async fn recv(&mut self) -> Result<(Packet, u32), Error> {
        self.framed.next().await.ok_or(Error::ConnectionClosed)?
    }

    /// Session run loop
    pub async fn run(mut self) -> Result<()> {
        let mut rx = self.ctx.subscribe();

        let keepalive = self.keepalive();
        let deadline = keepalive.map_or_else(Instant::now, |keepalive| Instant::now() + keepalive);
        let keepalive_timer = sleep_until(deadline);
        tokio::pin!(keepalive_timer);

        loop {
            tokio::select! {
                // Receive rx
                // packet = self.rx.recv() => {
                //     match packet {
                //         Some(packet) => self.stream.send(packet).await?,
                //         None => return Ok(()),
                //     }
                // }

                // Server shutdown
                _ = Context::shutdown(&mut rx) => {
                    self.server_shutdown().await?;
                    return Ok(());
                }

                // Receive Packet
                result = self.recv() => {
                    let (packet, packet_size) = match result {
                        Ok(packet) => packet,
                        Err(Error::ConnectionClosed) => {
                            self.connection_lost().await?;
                            return Ok(());
                        }
                        Err(e) => {
                            self.connection_error().await?;
                            return Err(e.into());
                        }
                    };

                    // Validate packet size
                    self.validate_packet_size(packet_size)?;

                    // Reset keepalive timer
                    if let Some(keepalive) = keepalive {
                        keepalive_timer.as_mut().reset(Instant::now() + keepalive);
                    }

                    // Packet handle
                    if self.handle_packet(packet).await? {
                        return Ok(());
                    }
                }

                // Keepalive timeout
                _ = &mut keepalive_timer, if keepalive.is_some() => {
                    self.keepalive_timeout().await?;
                }
            }
        }
    }

    /// keep alive
    fn keepalive(&self) -> Option<Duration> {
        let keepalive = u64::from(self.keepalive) * 1_500;
        (keepalive > 0).then(|| Duration::from_millis(keepalive))
    }

    /// Server shutdown
    async fn server_shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    ///
    async fn connection_lost(&mut self) -> Result<()> {
        Ok(())
    }

    ///
    async fn connection_error(&mut self) -> Result<()> {
        Ok(())
    }

    /// Keepalive timeout
    async fn keepalive_timeout(&mut self) -> Result<(), Error> {
        Err(Error::ProtocolError("Keepalive timeout".into()))
    }

    /// Validate packet size
    fn validate_packet_size(&self, packet_size: u32) -> Result<(), Error> {
        if packet_size > self.ctx.config().mqtt.max_packet_size() {
            return Err(Error::PacketTooLarge);
        }
        Ok(())
    }

    /// Handle Packet
    async fn handle_packet(&mut self, packet: Packet) -> Result<bool, Error> {
        match packet {
            Packet::PingReq => self.send(Packet::PingResp).await?,
            Packet::Connect(_) => {
                return Err(Error::ProtocolError("CONNECT packet received more than once".into()));
            }
            Packet::Disconnect(disconnect) => {
                self.handle_disconnect(disconnect).await?;
                return Ok(true);
            }
            Packet::Publish(publish) => self.handle_publish(publish).await?,
            _ => {}
        }

        Ok(false)
    }

    /// Handle Disconnect
    async fn handle_disconnect(&mut self, disconnect: Disconnect) -> Result<(), Error> {
        println!("{:?}", disconnect);
        Ok(())
    }

    /// Handle Publish
    async fn handle_publish(&mut self, publish: Publish) -> Result<(), Error> {
        println!("{:?}", publish);
        Ok(())
    }
}

fn resolve_client_id(connect: &mut Connect) -> Result<bool, Error> {
    if connect.client_id.is_empty() {
        connect.client_id = uuid::Uuid::new_v4().simple().to_string();
        return Ok(true);
    }
    Ok(false)
}

/// Send ConnAck Error
async fn send_error(framed: &mut Framed<IO, Codec>, e: &Error) -> Result<(), Error> {
    let mut connack = ConnAck::default();
    connack.reason_code = e.into();
    framed.send(Packet::ConnAck(connack)).await
}

/// Send ConnAck Ok
async fn send_ok(
    framed: &mut Framed<IO, Codec>,
    properties: Option<ConnAckProperties>,
) -> Result<(), Error> {
    let mut connack = ConnAck::default();
    connack.properties = properties;
    framed.send(Packet::ConnAck(connack)).await
}

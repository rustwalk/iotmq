use super::*;
use crate::{Context, mqtt::*};
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
    expiry_interval: u32,
}

impl Session {
    /// MQTT connect
    pub async fn connect(ctx: Context, io: IO, addr: SocketAddr) -> Result<Self> {
        // Config
        let config = ctx.config();
        let mqtt = &config.mqtt;

        // Framed
        let mut framed = Framed::new(io, Codec::new(mqtt.max_packet_size));

        // Connection timeout
        let (packet, _) = timeout(Duration::from_secs(10), framed.next())
            .await?
            .ok_or(Error::ConnectionClosed)??;

        // First packet
        let mut connect = match packet {
            Packet::Connect(connect) => connect,
            _ => {
                return Err(Error::ProtocolError("First packet must be CONNECT".to_string()).into());
            }
        };

        // Resolve client id
        let assigned = match Self::resolve_client_id(&mut connect, mqtt.max_client_id_len) {
            Ok(assigned) => assigned,
            Err(error) => {
                Self::ack_error(&mut framed, &error).await?;
                return Err(error.into());
            }
        };

        // Session expiry interval
        let connect_expiry_interval =
            connect.properties.as_ref().and_then(|p| p.session_expiry_interval).unwrap_or(0);
        let expiry_interval = if mqtt.session_expiry_interval > 0 {
            connect_expiry_interval.min(mqtt.session_expiry_interval)
        } else {
            connect_expiry_interval
        };

        // Send ConnAck
        let properties = if connect.protocol_version == Version::V5 {
            Some(ConnAckProperties {
                assigned_client_identifier: assigned.then(|| connect.client_id.clone()),
                max_packet_size: (mqtt.max_packet_size > 0).then_some(mqtt.max_packet_size),
                topic_alias_max: (mqtt.max_topic_alias > 0).then_some(mqtt.max_topic_alias),
                receive_maximum: (mqtt.max_receive > 0).then_some(mqtt.max_receive),
                maximum_qos: (mqtt.max_qos < 2).then_some(mqtt.max_qos),
                retain_available: (!mqtt.retain_available).then_some(0),
                session_expiry_interval: (expiry_interval != connect_expiry_interval)
                    .then_some(expiry_interval),
                ..Default::default()
            })
        } else {
            None
        };
        Self::ack_ok(&mut framed, false, properties).await?;

        // new Client
        let (tx, rx) = mpsc::channel::<Packet>(128);
        let client = Client::new(connect.client_id.clone(), addr, connect.protocol_version, tx);
        ctx.clients.insert(connect.client_id, client);

        Ok(Self { ctx, framed, rx, keepalive: connect.keepalive, expiry_interval })
    }

    /// Send ConnAck Ok
    async fn ack_ok(
        framed: &mut Framed<IO, Codec>,
        session_present: bool,
        properties: Option<ConnAckProperties>,
    ) -> Result<(), Error> {
        let connack = ConnAck { session_present, reason_code: ReasonCode::Success, properties };
        framed.send(Packet::ConnAck(connack)).await
    }

    /// Send ConnAck Error
    async fn ack_error(framed: &mut Framed<IO, Codec>, e: &Error) -> Result<(), Error> {
        let mut connack = ConnAck::default();
        connack.reason_code = e.into();
        framed.send(Packet::ConnAck(connack)).await
    }

    pub fn resolve_client_id(
        connect: &mut Connect,
        max_client_id_len: usize,
    ) -> Result<bool, Error> {
        let assigned = connect.client_id.is_empty();

        if max_client_id_len > 0 && connect.client_id.len() > max_client_id_len {
            return Err(Error::ClientIdentifierNotValid);
        }

        if assigned {
            if !connect.clean_start {
                return Err(Error::ClientIdentifierNotValid);
            }

            if connect.protocol_version == Version::V31 {
                return Err(Error::ClientIdentifierNotValid);
            }

            connect.client_id = uuid::Uuid::new_v4().simple().to_string();
        }

        Ok(assigned)
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
                packet = self.rx.recv() => {
                    match packet {
                        Some(packet) => self.framed.send(packet).await?,
                        None => return Ok(()),
                    }
                }

                // Server shutdown
                _ = Context::shutdown(&mut rx) => {
                    self.server_shutdown().await?;
                    return Ok(());
                }

                // Receive Packet
                result = self.framed.next() => {
                    let (packet, packet_size) = match result {
                        Some(Ok(packet)) => packet,
                        None | Some(Err(Error::ConnectionClosed)) => {
                            self.connection_lost().await?;
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            self.connection_error().await?;
                            return Err(e.into());
                        }
                    };

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

    /// Handle Packet
    async fn handle_packet(&mut self, packet: Packet) -> Result<bool, Error> {
        match packet {
            Packet::PingReq => self.framed.send(Packet::PingResp).await?,
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

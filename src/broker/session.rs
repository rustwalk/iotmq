use super::Stream;
use crate::Context;
use crate::mqtt::{Connect, Error, Packet};
use anyhow::Result;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};
use tracing::debug;

pub struct Session {
    pub ctx: Context,
    pub stream: Stream,
    pub connect: Connect,
    //pub client_id: String,
}

impl Session {
    pub fn new(ctx: Context, stream: Stream, connect: Connect) -> Self {
        Self { ctx, stream, connect }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut rx = self.ctx.subscribe();

        let keepalive = self.keepalive();
        let deadline = keepalive.map_or_else(Instant::now, |keepalive| Instant::now() + keepalive);
        let keepalive_timer = sleep_until(deadline);
        tokio::pin!(keepalive_timer);

        loop {
            tokio::select! {
                // Server shutdown
                _ = Context::shutdown(&mut rx) => {
                    self.server_shutdown().await?;
                    return Ok(());
                }

                // Receive Packet
                result = self.stream.recv() => {
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
                    self.handle_packet(packet).await?;
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
        let keepalive = u64::from(self.connect.keepalive) * 1_500;
        (keepalive > 0).then(|| Duration::from_millis(keepalive))
    }

    /// Clean up when the broker is stopping or restarting.
    async fn server_shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    ///
    #[allow(dead_code)]
    async fn client_disconnect(&mut self) -> Result<()> {
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

    ///
    #[allow(dead_code)]
    async fn session_taken_over(&mut self) -> Result<()> {
        Ok(())
    }

    /// Handel Packet
    async fn handle_packet(&mut self, packet: Packet) -> Result<(), Error> {
        println!("{:?}", packet);
        match packet {
            Packet::PingReq => self.stream.send(Packet::PingResp).await,
            Packet::Connect(_) => {
                Err(Error::ProtocolError("CONNECT packet received more than once".into()))
            }
            _ => Ok(()),
        }
    }
}

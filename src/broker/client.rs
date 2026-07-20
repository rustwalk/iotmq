use crate::mqtt::*;
use std::net::SocketAddr;
use std::time::SystemTime;
use tokio::sync::mpsc;

pub struct Client {
    pub id: String,
    pub addr: SocketAddr,
    pub version: Version,
    pub connected_at: SystemTime,

    tx: mpsc::Sender<Packet>,
}

impl Client {
    pub fn new(id: String, addr: SocketAddr, version: Version, tx: mpsc::Sender<Packet>) -> Self {
        Self { id, addr, version, connected_at: SystemTime::now(), tx }
    }

    pub async fn send(&self, packet: Packet) -> Result<(), mpsc::error::SendError<Packet>> {
        self.tx.send(packet).await
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

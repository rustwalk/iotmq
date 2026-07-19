use crate::mqtt::{Packet, Version};
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

impl Client {}

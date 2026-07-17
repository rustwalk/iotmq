use super::PacketType;
use tokio_util::bytes::{BufMut, BytesMut};

/// PingReq Packet
pub struct PingReq;

impl PingReq {
    pub fn encode(write: &mut BytesMut) {
        write.put_u8((PacketType::PingReq as u8) << 4);
        write.put_u8(0x00);
    }
}

/// PingResp Packet
pub struct PingResp;

impl PingResp {
    pub fn encode(write: &mut BytesMut) {
        write.put_u8((PacketType::PingResp as u8) << 4);
        write.put_u8(0x00);
    }
}

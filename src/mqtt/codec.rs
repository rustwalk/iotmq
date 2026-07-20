use super::*;
use crate::mqtt::publish::Publish;
use anyhow::Result;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

const MAX_PACKET_SIZE: u32 = 268_435_460;
pub struct Codec {
    version: Version,
    max_packet_size: u32,
}

impl Codec {
    pub fn new(max_packet_size: u32) -> Self {
        let max_packet_size = if max_packet_size == 0 {
            MAX_PACKET_SIZE
        } else {
            max_packet_size.min(MAX_PACKET_SIZE)
        };
        Self { version: Version::default(), max_packet_size }
    }
}

/// Codec Decode
impl Decoder for Codec {
    type Item = (Packet, u32);
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Check length
        if src.len() < 2 {
            return Ok(None);
        }

        // Decode remaining length
        let bytes = src.as_ref();
        let packet_type = bytes[0] >> 4;
        let flags = bytes[0] & 0x0F;
        let (bytes, packet_length) = match decode_length(&bytes[1..])? {
            Some((length, bytes)) => {
                let packet_length = 1 + bytes + length;
                if src.len() < packet_length {
                    src.reserve(packet_length - src.len());
                    return Ok(None);
                }
                src.advance(bytes + 1);
                let bytes = src.split_to(length).freeze();
                (bytes, packet_length as u32)
            }
            None => return Ok(None),
        };

        // Validate packet size
        if packet_length > self.max_packet_size {
            return Err(Error::PacketTooLarge);
        }

        // Decode packet
        let packet_type = PacketType::try_from(packet_type).map_err(|_| Error::MalformedPacket)?;
        let packet = match packet_type {
            PacketType::Connect => {
                let connect = Connect::decode(bytes)?;
                self.version = connect.protocol_version;
                Packet::Connect(connect)
            }
            PacketType::Disconnect => Packet::Disconnect(Disconnect::decode(self.version, bytes)?),
            PacketType::Publish => Packet::Publish(Publish::decode(self.version, bytes, flags)?),
            PacketType::PingReq => Packet::PingReq,
            PacketType::PingResp => Packet::PingResp,
            _ => {
                return Err(Error::ProtocolError(format!(
                    "Packet {packet_type:?} Decoder is not implemented"
                )));
            }
        };

        Ok(Some((packet, packet_length)))
    }
}

/// Codec Encode
impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::ConnAck(connack) => connack.encode(self.version, dst)?,
            //Packet::Disconnect(disconnect) => disconnect.encode(dst)?,
            Packet::Publish(publish) => publish.encode(self.version, dst)?,
            Packet::PingReq => PingReq::encode(dst),
            Packet::PingResp => PingResp::encode(dst),
            _ => return Err(Error::ProtocolError("Packet Encoder is not implemented".into())),
        }
        Ok(())
    }
}

use super::*;
use anyhow::Result;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct Codec;

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
        let (bytes, length) = match decode_length(&bytes[1..])? {
            Some((length, bytes)) => {
                let packet_length = 1 + bytes + length;
                if src.len() < packet_length {
                    src.reserve(packet_length);
                    return Ok(None);
                }
                src.advance(bytes + 1);
                let bytes = src.split_to(length).freeze();
                (bytes, packet_length as u32)
            }
            None => return Ok(None),
        };

        // Decode packet
        let packet_type = PacketType::try_from(packet_type).map_err(|_| Error::MalformedPacket)?;
        let packet = match packet_type {
            PacketType::Connect => Packet::Connect(Connect::decode(bytes)?),
            _ => return Err(Error::ProtocolError("Packet Decoder is not implemented".into())),
        };

        Ok(Some((packet, length)))
    }
}

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::ConnAck(connack) => connack.encode(dst)?,
            _ => return Err(Error::ProtocolError("Packet Encoder is not implemented".into())),
        }
        Ok(())
    }
}

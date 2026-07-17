use super::*;
use anyhow::Result;
use tokio_util::bytes::{Buf, Bytes};

/// DISCONNECT Packet
#[derive(Debug, Default)]
pub struct Disconnect {
    pub reason_code: ReasonCode,
    pub properties: Option<DisconnectProperties>,
}

impl Disconnect {
    /// Decode Disconnect Packet
    pub fn decode(mut src: Bytes) -> Result<Self, Error> {
        let mut disconnect = Self::default();
        disconnect.reason_code =
            ReasonCode::try_from(src.get_u8()).map_err(|_| Error::MalformedPacket)?;
        disconnect.properties = DisconnectProperties::decode(&mut src)?;
        Ok(disconnect)
    }
}

/// DISCONNECT Properties
#[derive(Debug, Default)]
pub struct DisconnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub user_property: Vec<(String, String)>,
}

impl DisconnectProperties {
    /// Decode Disconnect Properties
    pub fn decode(src: &mut Bytes) -> Result<Option<Self>, Error> {
        let mut properties = Self::default();

        let (length, length_bytes) = decode_length(src.as_ref())?.ok_or(Error::MalformedPacket)?;
        src.advance(length_bytes);

        if length == 0 {
            return Ok(None);
        }

        let mut src = src.split_to(length);

        loop {
            if !src.has_remaining() {
                return Ok(Some(properties));
            }

            let id = src.get_u8();
            let property = Property::try_from(id).map_err(|_| Error::MalformedPacket)?;
            match property {
                Property::SessionExpiryInterval => {
                    if src.remaining() < 4 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.session_expiry_interval = Some(src.get_u32());
                }

                Property::ServerReference => {
                    properties.server_reference = Some(decode_string(&mut src)?);
                }

                Property::ReasonString => {
                    properties.reason_string = Some(decode_string(&mut src)?);
                }

                Property::UserProperty => {
                    let k = decode_string(&mut src)?;
                    let v = decode_string(&mut src)?;
                    properties.user_property.push((k, v));
                }
                property => {
                    return Err(Error::ProtocolError(format!(
                        "{property:?} is not allowed in DISCONNECT"
                    )));
                }
            }
        }
    }
}

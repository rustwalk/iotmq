use super::*;
use anyhow::Result;
use tokio_util::bytes::{BufMut, BytesMut};

/// ConnAck Packet
#[derive(Debug, Default)]
pub struct ConnAck {
    pub version: Version,
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: Option<ConnAckProperties>,
}

impl ConnAck {
    /// Encode ConnAck Packet
    pub fn encode(self, dst: &mut BytesMut) -> Result<(), Error> {
        dst.put_u8((PacketType::ConnAck as u8) << 4);

        match self.version {
            Version::V5 => {
                let property_length = self.properties.as_ref().map_or(0, ConnAckProperties::len);
                let length = 2 + length_bytes(property_length) + property_length;
                encode_length(dst, length)?;
                dst.put_u8(self.session_present as u8);
                dst.put_u8(self.reason_code as u8);
                encode_length(dst, property_length)?;
                if let Some(property) = self.properties {
                    property.encode(dst)?;
                }
            }
            _ => {
                dst.put_u8(2);
                dst.put_u8(self.session_present as u8);
                dst.put_u8(self.reason_code.to_v3()?);
            }
        }

        Ok(())
    }
}

/// ConnAck Properties
#[derive(Debug, Default)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub auth_method: Option<String>,
    pub auth_data: Option<Vec<u8>>,
    pub response_info: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_max: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub user_property: Vec<(String, String)>,
    pub max_packet_size: Option<u32>,
    pub wildcard_sub_available: Option<u8>,
    pub sub_identifier_available: Option<u8>,
    pub shared_sub_available: Option<u8>,
}

impl ConnAckProperties {
    /// Encode ConnAck Properties
    pub fn encode(self, dst: &mut BytesMut) -> Result<(), Error> {
        if let Some(session_expiry_interval) = self.session_expiry_interval {
            dst.put_u8(Property::SessionExpiryInterval as u8);
            dst.put_u32(session_expiry_interval);
        }

        if let Some(assigned_client_identifier) = self.assigned_client_identifier {
            dst.put_u8(Property::AssignedClientIdentifier as u8);
            encode_string(dst, &assigned_client_identifier)?;
        }

        if let Some(server_keep_alive) = self.server_keep_alive {
            dst.put_u8(Property::ServerKeepAlive as u8);
            dst.put_u16(server_keep_alive);
        }

        if let Some(auth_method) = self.auth_method {
            dst.put_u8(Property::AuthMethod as u8);
            encode_string(dst, &auth_method)?;
        }

        if let Some(auth_data) = self.auth_data {
            dst.put_u8(Property::AuthData as u8);
            dst.put_u16(auth_data.len() as u16);
            dst.extend_from_slice(&auth_data);
        }

        if let Some(response_info) = self.response_info {
            dst.put_u8(Property::ResponseInfo as u8);
            encode_string(dst, &response_info)?;
        }

        if let Some(server_reference) = self.server_reference {
            dst.put_u8(Property::ServerReference as u8);
            encode_string(dst, &server_reference)?;
        }

        if let Some(reason_string) = self.reason_string {
            dst.put_u8(Property::ReasonString as u8);
            encode_string(dst, &reason_string)?;
        }

        if let Some(receive_maximum) = self.receive_maximum {
            dst.put_u8(Property::ReceiveMaximum as u8);
            dst.put_u16(receive_maximum);
        }

        if let Some(topic_alias_max) = self.topic_alias_max {
            dst.put_u8(Property::TopicAliasMaximum as u8);
            dst.put_u16(topic_alias_max);
        }

        if let Some(maximum_qos) = self.maximum_qos {
            dst.put_u8(Property::MaximumQoS as u8);
            dst.put_u8(maximum_qos);
        }

        if let Some(retain_available) = self.retain_available {
            dst.put_u8(Property::RetainAvailable as u8);
            dst.put_u8(retain_available);
        }

        for (k, v) in self.user_property.iter() {
            dst.put_u8(Property::UserProperty as u8);
            encode_string(dst, k)?;
            encode_string(dst, v)?;
        }

        if let Some(max_packet_size) = self.max_packet_size {
            dst.put_u8(Property::MaxPacketSize as u8);
            dst.put_u32(max_packet_size);
        }

        if let Some(wildcard_sub_available) = self.wildcard_sub_available {
            dst.put_u8(Property::WildcardSubAvailable as u8);
            dst.put_u8(wildcard_sub_available);
        }

        if let Some(sub_identifier_available) = self.sub_identifier_available {
            dst.put_u8(Property::SubIdentifierAvailable as u8);
            dst.put_u8(sub_identifier_available);
        }

        if let Some(shared_sub_available) = self.shared_sub_available {
            dst.put_u8(Property::SharedSubAvailable as u8);
            dst.put_u8(shared_sub_available);
        }

        Ok(())
    }

    pub fn len(&self) -> usize {
        let mut len = 0;

        if self.session_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if let Some(ref assigned_client_identifier) = self.assigned_client_identifier {
            len += 1 + 2 + assigned_client_identifier.len();
        }

        if self.server_keep_alive.is_some() {
            len += 1 + 2;
        }

        if let Some(ref auth_method) = self.auth_method {
            len += 1 + 2 + auth_method.len();
        }

        if let Some(ref auth_data) = self.auth_data {
            len += 1 + 2 + auth_data.len();
        }

        if let Some(ref response_info) = self.response_info {
            len += 1 + 2 + response_info.len();
        }

        if let Some(ref server_reference) = self.server_reference {
            len += 1 + 2 + server_reference.len();
        }

        if let Some(ref reason_string) = self.reason_string {
            len += 1 + 2 + reason_string.len();
        }

        if self.receive_maximum.is_some() {
            len += 1 + 2;
        }

        if self.topic_alias_max.is_some() {
            len += 1 + 2;
        }

        if self.maximum_qos.is_some() {
            len += 1 + 1;
        }

        if self.retain_available.is_some() {
            len += 1 + 1;
        }

        for (k, v) in self.user_property.iter() {
            len += 1 + 2 + k.len() + 2 + v.len();
        }

        if self.max_packet_size.is_some() {
            len += 1 + 4;
        }

        if self.wildcard_sub_available.is_some() {
            len += 1 + 1;
        }

        if self.sub_identifier_available.is_some() {
            len += 1 + 1;
        }

        if self.shared_sub_available.is_some() {
            len += 1 + 1;
        }

        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(connack: ConnAck) -> Result<BytesMut, Error> {
        let mut dst = BytesMut::new();
        connack.encode(&mut dst)?;
        Ok(dst)
    }

    #[test]
    fn encode_v31_success() {
        let dst = encode(ConnAck {
            version: Version::V31,
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: None,
        })
        .unwrap();

        assert_eq!(&dst[..], &[0x20, 0x02, 0x00, 0x00]);
    }

    #[test]
    fn encode_v311_failure_reason_code() {
        let dst = encode(ConnAck {
            version: Version::V311,
            session_present: false,
            reason_code: ReasonCode::NotAuthorized,
            properties: None,
        })
        .unwrap();

        assert_eq!(&dst[..], &[0x20, 0x02, 0x00, 0x05]);
    }

    #[test]
    fn encode_v3_does_not_include_v5_properties() {
        let dst = encode(ConnAck {
            version: Version::V311,
            session_present: true,
            reason_code: ReasonCode::Success,
            properties: Some(ConnAckProperties {
                server_keep_alive: Some(30),
                ..Default::default()
            }),
        })
        .unwrap();

        assert_eq!(&dst[..], &[0x20, 0x02, 0x01, 0x00]);
    }

    #[test]
    fn encode_v3_rejects_unrepresentable_reason_code() {
        let result = encode(ConnAck {
            version: Version::V311,
            session_present: false,
            reason_code: ReasonCode::ServerBusy,
            properties: None,
        });

        assert!(matches!(result, Err(Error::ProtocolError(_))));
    }

    #[test]
    fn encode_v5_without_properties() {
        let dst = encode(ConnAck {
            version: Version::V5,
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: None,
        })
        .unwrap();

        assert_eq!(&dst[..], &[0x20, 0x03, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn encode_v5_with_properties() {
        let dst = encode(ConnAck {
            version: Version::V5,
            session_present: true,
            reason_code: ReasonCode::Success,
            properties: Some(ConnAckProperties {
                server_keep_alive: Some(30),
                assigned_client_identifier: Some("client-1".into()),
                user_property: vec![("key".into(), "value".into())],
                ..Default::default()
            }),
        })
        .unwrap();

        assert_eq!(dst[0], 0x20);
        assert_eq!(dst[1] as usize, dst.len() - 2);
        assert_eq!(dst[2], 0x01);
        assert_eq!(dst[3], 0x00);
        assert_eq!(dst[4] as usize, dst.len() - 5);
        assert_eq!(
            &dst[5..],
            &[
                0x12, 0x00, 0x08, b'c', b'l', b'i', b'e', b'n', b't', b'-', b'1', 0x13, 0x00, 0x1e,
                0x26, 0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b'v', b'a', b'l', b'u', b'e',
            ]
        );
    }
}

use super::*;
use anyhow::Result;
use tokio_util::bytes::{Buf, BufMut, Bytes, BytesMut};

/// Publish Packet
#[derive(Debug, Default, Clone)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic_name: String,
    pub packet_id: Option<u16>,
    pub properties: Option<PublishProperties>,
    pub payload: Vec<u8>,
}

impl Publish {
    /// Decode Publish Packet
    pub fn decode(version: Version, mut src: Bytes, flags: u8) -> Result<Self, Error> {
        let mut publish = Self::default();

        // Flags
        publish.dup = flags >> 3 > 0;
        publish.qos = QoS::try_from((flags >> 1) & 0x03).map_err(|_| Error::MalformedPacket)?;
        publish.retain = flags & 0x01 > 0;

        // Topic Name
        publish.topic_name = decode_string(&mut src)?;

        // Packet ID
        if publish.qos > QoS::AtMostOnce {
            if src.remaining() < 2 {
                return Err(Error::MalformedPacket);
            }
            publish.packet_id = Some(src.get_u16());
        }

        // Properties
        if version == Version::V5 {
            publish.properties = PublishProperties::decode(&mut src)?;
        }

        // Payload
        publish.payload = src.to_vec();

        Ok(publish)
    }

    /// Encode Publish Packet
    pub fn encode(self, version: Version, dst: &mut BytesMut) -> Result<(), Error> {
        if self.qos == QoS::AtMostOnce && self.packet_id.is_some() {
            return Err(Error::ProtocolError(
                "QoS 0 PUBLISH cannot contain Packet Identifier".into(),
            ));
        }
        if self.qos > QoS::AtMostOnce {
            if let Some(packet_id) = self.packet_id {
                if packet_id == 0 {
                    return Err(Error::ProtocolError(
                        "QoS 1/2 PUBLISH Packet Identifier not equal zero".into(),
                    ));
                }
            } else {
                return Err(Error::ProtocolError(
                    "QoS 1/2 PUBLISH must contain Packet Identifier".into(),
                ));
            }
        }

        // Byte1
        let mut packet_type = (PacketType::Publish as u8) << 4;
        if self.dup {
            packet_type |= 1 << 3;
        }
        packet_type |= (self.qos as u8) << 1;
        if self.retain {
            packet_type |= 1;
        }
        dst.put_u8(packet_type);

        // Remaining Length
        let (length, property_length) = self.len(version);
        encode_length(dst, length)?;

        // Topic Name
        encode_string(dst, &self.topic_name)?;

        // Packet ID
        if self.qos > QoS::AtMostOnce
            && let Some(packet_id) = self.packet_id
        {
            dst.put_u16(packet_id);
        }

        // Properties
        if version == Version::V5 {
            encode_length(dst, property_length)?;
            if let Some(properties) = self.properties {
                properties.encode(dst)?;
            }
        }

        // Payload
        dst.extend_from_slice(&self.payload);

        Ok(())
    }

    /// Publish Length
    pub fn len(&self, version: Version) -> (usize, usize) {
        let mut length = 2 + self.topic_name.len();

        if self.qos > QoS::AtMostOnce {
            length += 2;
        }

        let mut property_length = 0;
        if version == Version::V5 {
            property_length = self.properties.as_ref().map_or(0, PublishProperties::len);
            length += length_bytes(property_length) + property_length;
        }

        length += self.payload.len();

        (length, property_length)
    }
}

/// Publish Properties
#[derive(Debug, Default, Clone)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscription_identifier: Vec<u32>,
    pub topic_alias: Option<u16>,
    pub user_property: Vec<(String, String)>,
}

impl PublishProperties {
    /// Decode Publish Properties
    pub fn decode(src: &mut Bytes) -> Result<Option<Self>, Error> {
        let mut properties = Self::default();

        let (length, length_bytes) = decode_length(src.as_ref())?.ok_or(Error::MalformedPacket)?;
        src.advance(length_bytes);

        if length == 0 {
            return Ok(None);
        }

        if src.len() < length {
            return Err(Error::MalformedPacket);
        }
        let mut src = src.split_to(length);

        loop {
            if !src.has_remaining() {
                return Ok(Some(properties));
            }

            let id = src.get_u8();
            let property = Property::try_from(id).map_err(|_| Error::MalformedPacket)?;
            match property {
                Property::PayloadFormatIndicator => {
                    if src.remaining() < 1 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.payload_format_indicator = Some(src.get_u8());
                }

                Property::MessageExpiryInterval => {
                    if src.remaining() < 4 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.message_expiry_interval = Some(src.get_u32());
                }

                Property::ContentType => {
                    properties.content_type = Some(decode_string(&mut src)?);
                }

                Property::ResponseTopic => {
                    properties.response_topic = Some(decode_string(&mut src)?);
                }
                Property::CorrelationData => {
                    if src.remaining() < 2 {
                        return Err(Error::MalformedPacket);
                    }
                    let length = src.get_u16() as usize;
                    let src = src.split_to(length);
                    properties.correlation_data = Some(src.to_vec())
                }

                Property::SubIdentifier => {
                    let (length, length_bytes) =
                        decode_length(src.as_ref())?.ok_or(Error::MalformedPacket)?;
                    src.advance(length_bytes);
                    properties.subscription_identifier.push(length as u32);
                }

                Property::TopicAlias => {
                    if src.remaining() < 2 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.topic_alias = Some(src.get_u16());
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

    /// Encode Publish Properties
    pub fn encode(self, dst: &mut BytesMut) -> Result<(), Error> {
        if let Some(payload_format_indicator) = self.payload_format_indicator {
            if payload_format_indicator > 1 {
                return Err(Error::ProtocolError("Payload Format Indicator must be 0 or 1".into()));
            }
            dst.put_u8(Property::PayloadFormatIndicator as u8);
            dst.put_u8(payload_format_indicator);
        }

        if let Some(message_expiry_interval) = self.message_expiry_interval {
            dst.put_u8(Property::MessageExpiryInterval as u8);
            dst.put_u32(message_expiry_interval);
        }

        if let Some(content_type) = self.content_type {
            dst.put_u8(Property::ContentType as u8);
            encode_string(dst, &content_type)?;
        }

        if let Some(response_topic) = self.response_topic {
            dst.put_u8(Property::ResponseTopic as u8);
            encode_string(dst, &response_topic)?;
        }

        if let Some(correlation_data) = self.correlation_data {
            if correlation_data.len() > u16::MAX as usize {
                return Err(Error::ProtocolError("Correlation Data exceeds 65535 bytes".into()));
            }
            dst.put_u8(Property::CorrelationData as u8);
            dst.put_u16(correlation_data.len() as u16);
            dst.extend_from_slice(&correlation_data);
        }

        for identifier in self.subscription_identifier {
            if identifier == 0 || identifier > 268_435_455 {
                return Err(Error::ProtocolError("Invalid Subscription Identifier".into()));
            }
            dst.put_u8(Property::SubIdentifier as u8);
            encode_length(dst, identifier as usize)?;
        }

        if let Some(topic_alias) = self.topic_alias {
            if topic_alias == 0 {
                return Err(Error::ProtocolError("Topic Alias cannot be zero".into()));
            }
            dst.put_u8(Property::TopicAlias as u8);
            dst.put_u16(topic_alias);
        }

        for (k, v) in self.user_property {
            dst.put_u8(Property::UserProperty as u8);
            encode_string(dst, &k)?;
            encode_string(dst, &v)?;
        }

        Ok(())
    }

    /// Publish Properties Length
    pub fn len(&self) -> usize {
        let mut len = 0;

        if self.payload_format_indicator.is_some() {
            len += 1 + 1;
        }

        if self.message_expiry_interval.is_some() {
            len += 1 + 4;
        }

        if let Some(ref content_type) = self.content_type {
            len += 1 + 2 + content_type.len();
        }

        if let Some(ref response_topic) = self.response_topic {
            len += 1 + 2 + response_topic.len();
        }

        if let Some(ref correlation_data) = self.correlation_data {
            len += 1 + 2 + correlation_data.len();
        }

        for identifier in self.subscription_identifier.iter() {
            len += 1 + length_bytes(*identifier as usize);
        }

        if self.topic_alias.is_some() {
            len += 1 + 2;
        }

        for (k, v) in self.user_property.iter() {
            len += 1 + 2 + k.len() + 2 + v.len();
        }

        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mqtt_string(dst: &mut Vec<u8>, value: &str) {
        dst.extend_from_slice(&(value.len() as u16).to_be_bytes());
        dst.extend_from_slice(value.as_bytes());
    }

    fn decode_encoded_publish(version: Version, encoded: &BytesMut) -> Publish {
        let flags = encoded[0] & 0x0f;
        let (remaining_length, length_bytes) = decode_length(&encoded[1..]).unwrap().unwrap();
        assert_eq!(encoded.len(), 1 + length_bytes + remaining_length);

        let body = Bytes::copy_from_slice(&encoded[1 + length_bytes..]);
        Publish::decode(version, body, flags).unwrap()
    }

    #[test]
    fn encode_v311_qos0_exact_bytes() {
        let publish = Publish {
            retain: true,
            topic_name: "a/b".into(),
            payload: vec![0x00, 0xff],
            ..Default::default()
        };
        let mut dst = BytesMut::new();

        publish.encode(Version::V311, &mut dst).unwrap();

        assert_eq!(&dst[..], &[0x31, 0x07, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0xff]);
    }

    #[test]
    fn encode_v311_qos1_exact_bytes() {
        let publish = Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            topic_name: "status".into(),
            packet_id: Some(0x1234),
            payload: b"ok".to_vec(),
            ..Default::default()
        };
        let mut dst = BytesMut::new();

        publish.encode(Version::V311, &mut dst).unwrap();

        assert_eq!(
            &dst[..],
            &[0x3a, 0x0c, 0x00, 0x06, b's', b't', b'a', b't', b'u', b's', 0x12, 0x34, b'o', b'k',]
        );
    }

    #[test]
    fn encode_v5_properties_round_trip() {
        let publish = Publish {
            qos: QoS::ExactlyOnce,
            retain: true,
            topic_name: "request/device".into(),
            packet_id: Some(7),
            properties: Some(PublishProperties {
                payload_format_indicator: Some(1),
                message_expiry_interval: Some(60),
                content_type: Some("application/json".into()),
                response_topic: Some("response/device".into()),
                correlation_data: Some(vec![0xaa, 0xbb, 0xcc]),
                subscription_identifier: vec![321],
                topic_alias: Some(9),
                user_property: vec![("source".into(), "test".into())],
            }),
            payload: vec![0x7b, 0x00, 0xff, 0x7d],
            ..Default::default()
        };
        let mut dst = BytesMut::new();

        publish.encode(Version::V5, &mut dst).unwrap();
        let decoded = decode_encoded_publish(Version::V5, &dst);

        assert_eq!(decoded.qos, QoS::ExactlyOnce);
        assert!(decoded.retain);
        assert_eq!(decoded.topic_name, "request/device");
        assert_eq!(decoded.packet_id, Some(7));
        assert_eq!(decoded.payload, [0x7b, 0x00, 0xff, 0x7d]);

        let properties = decoded.properties.unwrap();
        assert_eq!(properties.payload_format_indicator, Some(1));
        assert_eq!(properties.message_expiry_interval, Some(60));
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.response_topic.as_deref(), Some("response/device"));
        assert_eq!(properties.correlation_data.as_deref(), Some(&[0xaa, 0xbb, 0xcc][..]));
        assert_eq!(properties.subscription_identifier, vec![321]);
        assert_eq!(properties.topic_alias, Some(9));
        assert_eq!(properties.user_property, vec![("source".into(), "test".into())]);
    }

    #[test]
    fn encode_rejects_invalid_packet_identifier() {
        for (qos, packet_id) in [
            (QoS::AtMostOnce, Some(1)),
            (QoS::AtLeastOnce, None),
            (QoS::AtLeastOnce, Some(0)),
            (QoS::ExactlyOnce, None),
        ] {
            let publish =
                Publish { qos, topic_name: "topic".into(), packet_id, ..Default::default() };

            assert!(matches!(
                publish.encode(Version::V5, &mut BytesMut::new()),
                Err(Error::ProtocolError(_))
            ));
        }
    }

    #[test]
    fn decode_v311_qos0_with_binary_payload() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "sensor/temperature");
        src.extend_from_slice(&[0x00, 0xff, 0x10, 0x80]);

        let publish = Publish::decode(
            Version::V311,
            Bytes::from(src),
            0x01, // QoS 0, RETAIN
        )
        .unwrap();

        assert!(!publish.dup);
        assert_eq!(publish.qos, QoS::AtMostOnce);
        assert!(publish.retain);
        assert_eq!(publish.topic_name, "sensor/temperature");
        assert_eq!(publish.packet_id, None);
        assert!(publish.properties.is_none());
        assert_eq!(publish.payload, [0x00, 0xff, 0x10, 0x80]);
    }

    #[test]
    fn decode_v311_qos1_with_packet_id() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "sensor/status");
        src.extend_from_slice(&0x1234_u16.to_be_bytes());
        src.extend_from_slice(b"online");

        let publish = Publish::decode(
            Version::V311,
            Bytes::from(src),
            0x0a, // DUP, QoS 1
        )
        .unwrap();

        assert!(publish.dup);
        assert_eq!(publish.qos, QoS::AtLeastOnce);
        assert!(!publish.retain);
        assert_eq!(publish.packet_id, Some(0x1234));
        assert_eq!(publish.payload, b"online");
    }

    #[test]
    fn decode_v5_qos2_with_properties() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "request/device");
        src.extend_from_slice(&7_u16.to_be_bytes());

        let mut properties = Vec::new();
        properties.extend_from_slice(&[Property::PayloadFormatIndicator as u8, 1]);
        properties.extend_from_slice(&[
            Property::MessageExpiryInterval as u8,
            0x00,
            0x00,
            0x00,
            0x3c,
        ]);
        properties.push(Property::ContentType as u8);
        mqtt_string(&mut properties, "application/json");
        properties.push(Property::ResponseTopic as u8);
        mqtt_string(&mut properties, "response/device");
        properties.extend_from_slice(&[
            Property::CorrelationData as u8,
            0x00,
            0x03,
            0xaa,
            0xbb,
            0xcc,
        ]);
        properties.extend_from_slice(&[
            Property::SubIdentifier as u8,
            0xc1,
            0x02, // Variable Byte Integer 321
        ]);
        properties.extend_from_slice(&[Property::TopicAlias as u8, 0x00, 0x09]);
        properties.push(Property::UserProperty as u8);
        mqtt_string(&mut properties, "source");
        mqtt_string(&mut properties, "test");

        assert!(properties.len() < 128);
        src.push(properties.len() as u8);
        src.extend_from_slice(&properties);
        src.extend_from_slice(&[0x7b, 0x00, 0xff, 0x7d]);

        let publish = Publish::decode(
            Version::V5,
            Bytes::from(src),
            0x05, // QoS 2, RETAIN
        )
        .unwrap();

        assert_eq!(publish.qos, QoS::ExactlyOnce);
        assert!(publish.retain);
        assert_eq!(publish.packet_id, Some(7));
        assert_eq!(publish.payload, [0x7b, 0x00, 0xff, 0x7d]);

        let properties = publish.properties.unwrap();
        assert_eq!(properties.payload_format_indicator, Some(1));
        assert_eq!(properties.message_expiry_interval, Some(60));
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.response_topic.as_deref(), Some("response/device"));
        assert_eq!(properties.correlation_data.as_deref(), Some(&[0xaa, 0xbb, 0xcc][..]));
        assert_eq!(properties.subscription_identifier, vec![321]);
        assert_eq!(properties.topic_alias, Some(9));
        assert_eq!(properties.user_property, vec![("source".into(), "test".into())]);
    }

    #[test]
    fn reject_invalid_qos_bits() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "topic");

        assert!(matches!(
            Publish::decode(Version::V5, Bytes::from(src), 0x06),
            Err(Error::MalformedPacket)
        ));
    }

    #[test]
    fn reject_truncated_packet_id() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "topic");
        src.push(0x12);

        assert!(matches!(
            Publish::decode(Version::V311, Bytes::from(src), 0x02),
            Err(Error::MalformedPacket)
        ));
    }

    #[test]
    fn reject_property_length_larger_than_packet() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "topic");
        src.extend_from_slice(&[
            0x05, // Properties Length is 5, but only two bytes follow
            Property::TopicAlias as u8,
            0x00,
        ]);

        assert!(matches!(
            Publish::decode(Version::V5, Bytes::from(src), 0x00),
            Err(Error::MalformedPacket)
        ));
    }
}

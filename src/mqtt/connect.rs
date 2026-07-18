use super::*;
use anyhow::Result;
use tokio_util::bytes::{Buf, Bytes};

/// CONNECT Packet
#[derive(Debug, Default)]
pub struct Connect {
    pub protocol_name: String,
    pub protocol_version: Version,
    pub username_flag: bool,
    pub password_flag: bool,
    pub will_retain: bool,
    pub will_qos: QoS,
    pub will_flag: bool,
    pub clean_start: bool,
    pub keepalive: u16,
    pub properties: Option<ConnectProperties>,
    pub client_id: String,
    pub will_properties: Option<WillProperties>,
    pub will_topic: String,
    pub will_payload: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Connect {
    /// Decode Connect Packet
    pub fn decode(mut src: Bytes) -> Result<Self, Error> {
        let mut connect = Self::default();

        // Protocol
        let protocol_name = decode_string(&mut src)?;
        if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
            return Err(Error::ProtocolError(format!("protocol_name = {protocol_name}")));
        }
        connect.protocol_name = protocol_name;
        let version = src.get_u8();
        connect.protocol_version =
            Version::try_from(version).map_err(|_| Error::UnsupportedProtocolVersion(version))?;

        // Connect Flags
        let connect_flags = src.get_u8();
        connect.username_flag = connect_flags & 0x80 > 0;
        connect.password_flag = connect_flags & 0x40 > 0;
        connect.will_retain = connect_flags & 0x20 > 0;
        let qos = (connect_flags & 0x18) >> 3;
        connect.will_qos =
            QoS::try_from(qos).map_err(|_| Error::ProtocolError(format!("QoS = {qos}")))?;
        connect.will_flag = connect_flags & 0x04 > 0;
        connect.clean_start = connect_flags & 0x02 > 0;

        // Keep Alive
        connect.keepalive = src.get_u16();

        // Properties
        if connect.protocol_version == Version::V5 {
            connect.properties = ConnectProperties::decode(&mut src)?;
        }

        // Client ID
        connect.client_id = decode_string(&mut src)?;

        // Will
        if connect.will_flag {
            connect.will_properties = WillProperties::decode(&mut src)?;
            connect.will_topic = decode_string(&mut src)?;
            connect.will_payload = decode_string(&mut src)?;
        }

        // User Name
        if connect.username_flag {
            connect.username = Some(decode_string(&mut src)?);
        }

        // Password
        if connect.password_flag {
            connect.password = Some(decode_string(&mut src)?);
        }

        Ok(connect)
    }
}

/// Connect Properties
#[derive(Debug, Default)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_max: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user_property: Vec<(String, String)>,
    pub auth_method: Option<String>,
    pub auth_data: Option<Vec<u8>>,
}

impl ConnectProperties {
    /// Decode Connect Connect Properties
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
                Property::SessionExpiryInterval => {
                    if src.remaining() < 4 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.session_expiry_interval = Some(src.get_u32());
                }

                Property::ReceiveMaximum => {
                    if src.remaining() < 2 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.receive_max = Some(src.get_u16());
                }

                Property::MaxPacketSize => {
                    if src.remaining() < 4 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.max_packet_size = Some(src.get_u32());
                }

                Property::TopicAliasMaximum => {
                    if src.remaining() < 2 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.topic_alias_max = Some(src.get_u16());
                }

                Property::RequestResponseInfo => {
                    if src.remaining() < 1 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.request_response_info = Some(src.get_u8());
                }

                Property::RequestProblemInfo => {
                    if src.remaining() < 1 {
                        return Err(Error::MalformedPacket);
                    }
                    properties.request_problem_info = Some(src.get_u8());
                }

                Property::UserProperty => {
                    let k = decode_string(&mut src)?;
                    let v = decode_string(&mut src)?;
                    properties.user_property.push((k, v));
                }

                Property::AuthMethod => {
                    properties.auth_method = Some(decode_string(&mut src)?);
                }

                Property::AuthData => {
                    if src.remaining() < 2 {
                        return Err(Error::MalformedPacket);
                    }
                    let length = src.get_u16() as usize;
                    let src = src.split_to(length);
                    properties.auth_data = Some(src.to_vec());
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

/// Will Properties
#[derive(Debug, Default)]
pub struct WillProperties {
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub will_delay_interval: Option<u32>,
    pub message_expiry_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub user_property: Vec<(String, String)>,
}

impl WillProperties {
    /// Decode Connect Will Properties
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
                Property::ContentType => {
                    properties.content_type = Some(decode_string(&mut src)?);
                }

                Property::ResponseTopic => {
                    properties.response_topic = Some(decode_string(&mut src)?);
                }

                Property::CorrelationData => {
                    let len = src.get_u16() as usize;
                    let read = src.split_to(len);
                    properties.correlation_data = Some(read.to_vec())
                }

                Property::WillDelayInterval => {
                    properties.will_delay_interval = Some(src.get_u32());
                }

                Property::MessageExpiryInterval => {
                    properties.message_expiry_interval = Some(src.get_u32());
                }

                Property::PayloadFormatIndicator => {
                    properties.payload_format_indicator = Some(src.get_u8());
                }

                Property::UserProperty => {
                    let k = decode_string(&mut src)?;
                    let v = decode_string(&mut src)?;
                    properties.user_property.push((k, v));
                }
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mqtt_string(dst: &mut Vec<u8>, value: &str) {
        dst.extend_from_slice(&(value.len() as u16).to_be_bytes());
        dst.extend_from_slice(value.as_bytes());
    }

    fn connect_header(dst: &mut Vec<u8>, version: u8, flags: u8, keepalive: u16) {
        mqtt_string(dst, "MQTT");
        dst.push(version);
        dst.push(flags);
        dst.extend_from_slice(&keepalive.to_be_bytes());
    }

    #[test]
    fn decode_v311_with_username_and_password() {
        let mut src = Vec::new();
        connect_header(&mut src, Version::V311 as u8, 0xc2, 60);
        mqtt_string(&mut src, "client-1");
        mqtt_string(&mut src, "user");
        mqtt_string(&mut src, "password");

        let connect = Connect::decode(Bytes::from(src)).unwrap();

        assert_eq!(connect.protocol_name, "MQTT");
        assert_eq!(connect.protocol_version, Version::V311);
        assert!(connect.clean_start);
        assert!(connect.username_flag);
        assert!(connect.password_flag);
        assert!(!connect.will_flag);
        assert_eq!(connect.keepalive, 60);
        assert_eq!(connect.client_id, "client-1");
        assert_eq!(connect.username.as_deref(), Some("user"));
        assert_eq!(connect.password.as_deref(), Some("password"));
        assert!(connect.properties.is_none());
    }

    #[test]
    fn decode_v5_with_connect_and_will_properties() {
        let mut src = Vec::new();
        // Username, password, Will Retain, Will QoS 1, Will Flag, Clean Start.
        connect_header(&mut src, Version::V5 as u8, 0xee, 30);

        let mut properties = Vec::new();
        properties.extend_from_slice(&[Property::SessionExpiryInterval as u8, 0, 0, 0, 10]);
        properties.extend_from_slice(&[Property::ReceiveMaximum as u8, 0, 20]);
        properties.push(Property::UserProperty as u8);
        mqtt_string(&mut properties, "key");
        mqtt_string(&mut properties, "value");
        properties.push(Property::AuthMethod as u8);
        mqtt_string(&mut properties, "token");
        properties.extend_from_slice(&[Property::AuthData as u8, 0, 2, 0xaa, 0xbb]);
        assert!(properties.len() < 128);
        src.push(properties.len() as u8);
        src.extend_from_slice(&properties);

        mqtt_string(&mut src, "client-v5");

        let mut will_properties = Vec::new();
        will_properties.extend_from_slice(&[Property::PayloadFormatIndicator as u8, 1]);
        will_properties.push(Property::ContentType as u8);
        mqtt_string(&mut will_properties, "text/plain");
        will_properties.push(Property::UserProperty as u8);
        mqtt_string(&mut will_properties, "k");
        mqtt_string(&mut will_properties, "v");
        assert!(will_properties.len() < 128);
        src.push(will_properties.len() as u8);
        src.extend_from_slice(&will_properties);
        mqtt_string(&mut src, "status/client-v5");
        mqtt_string(&mut src, "offline");
        mqtt_string(&mut src, "alice");
        mqtt_string(&mut src, "secret");

        let connect = Connect::decode(Bytes::from(src)).unwrap();

        assert_eq!(connect.protocol_version, Version::V5);
        assert!(connect.clean_start);
        assert!(connect.will_flag);
        assert!(connect.will_retain);
        assert!(matches!(connect.will_qos, QoS::AtLeastOnce));
        assert_eq!(connect.client_id, "client-v5");
        assert_eq!(connect.will_topic, "status/client-v5");
        assert_eq!(connect.will_payload, "offline");
        assert_eq!(connect.username.as_deref(), Some("alice"));
        assert_eq!(connect.password.as_deref(), Some("secret"));

        let properties = connect.properties.unwrap();
        assert_eq!(properties.session_expiry_interval, Some(10));
        assert_eq!(properties.receive_max, Some(20));
        assert_eq!(properties.user_property, vec![("key".into(), "value".into())]);
        assert_eq!(properties.auth_method.as_deref(), Some("token"));
        assert_eq!(properties.auth_data.as_deref(), Some(&[0xaa, 0xbb][..]));

        let will_properties = connect.will_properties.unwrap();
        assert_eq!(will_properties.payload_format_indicator, Some(1));
        assert_eq!(will_properties.content_type.as_deref(), Some("text/plain"));
        assert_eq!(will_properties.user_property, vec![("k".into(), "v".into())]);
    }

    #[test]
    fn reject_invalid_protocol_name() {
        let mut src = Vec::new();
        mqtt_string(&mut src, "HTTP");
        src.extend_from_slice(&[Version::V311 as u8, 0x02, 0x00, 0x3c]);
        mqtt_string(&mut src, "client");

        assert!(matches!(Connect::decode(Bytes::from(src)), Err(Error::ProtocolError(_))));
    }

    #[test]
    fn reject_unsupported_protocol_version() {
        let mut src = Vec::new();
        connect_header(&mut src, 6, 0x02, 60);
        mqtt_string(&mut src, "client");

        assert!(matches!(
            Connect::decode(Bytes::from(src)),
            Err(Error::UnsupportedProtocolVersion(6))
        ));
    }

    #[test]
    fn reject_truncated_protocol_name() {
        let src = Bytes::from_static(&[0x00, 0x04, b'M', b'Q']);

        assert!(matches!(Connect::decode(src), Err(Error::MalformedPacket)));
    }
}

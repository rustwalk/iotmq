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
    /// Decode Connect Packet
    pub fn decode(src: &mut Bytes) -> Result<Option<Self>, Error> {
        let mut properties = Self::default();

        let bytes = src.as_ref();
        let (length, length_bytes) = match decode_length(bytes)? {
            Some(length) => length,
            None => return Err(Error::MalformedPacket),
        };
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
                    properties.session_expiry_interval = Some(src.get_u32());
                }

                Property::ReceiveMaximum => {
                    properties.receive_max = Some(src.get_u16());
                }

                Property::MaxPacketSize => {
                    properties.max_packet_size = Some(src.get_u32());
                }

                Property::TopicAliasMaximum => {
                    properties.topic_alias_max = Some(src.get_u16());
                }

                Property::RequestResponseInfo => {
                    properties.request_response_info = Some(src.get_u8());
                }

                Property::RequestProblemInfo => {
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
                    let length = src.get_u16() as usize;
                    let src = src.split_to(length);
                    properties.auth_data = Some(src.to_vec());
                }
                _ => unreachable!(),
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
    /// Decode Connect Packet
    pub fn decode(src: &mut Bytes) -> Result<Option<Self>, Error> {
        let mut properties = Self::default();

        let bytes = src.as_ref();
        let (length, length_len) = match decode_length(bytes)? {
            Some(len) => len,
            None => return Err(Error::MalformedPacket),
        };
        src.advance(length_len);

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

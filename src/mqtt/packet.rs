use super::*;
use anyhow::Result;
use num_enum::TryFromPrimitive;
use tokio_util::bytes::{Buf, BufMut, Bytes, BytesMut};

/// MQTT Packet
#[derive(Debug)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    // Publish(Publish),
    // PubAck(PubAck),
    // PubRec(PubRec),
    // PubRel(PubRel),
    // PubComp(PubComp),
    // Subscribe(Subscribe),
    // SubAck(SubAck),
    // Unsubscribe(Unsubscribe),
    // UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    // Auth(Auth)
}

/// MQTT Error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Malformed Packet")]
    MalformedPacket,

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u8),

    #[error("Packet too large")]
    PacketTooLarge,

    #[error("MQTT connection closed by peer")]
    ConnectionClosed,

    #[error("Client identifier not valid")]
    ClientIdentifierNotValid,

    #[error("Bad username or password")]
    BadUserNameOrPassword,

    #[error("Not authorized")]
    NotAuthorized,

    #[error("Server unavailable")]
    ServerUnavailable,
}

/// MQTT QoS
#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce,
    ExactlyOnce,
}

impl Default for QoS {
    fn default() -> Self {
        Self::AtMostOnce
    }
}

/// MQTT Version
#[derive(Debug, TryFromPrimitive, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Version {
    V31 = 3,
    V311,
    V5,
}

impl Version {
    fn as_str(self) -> &'static str {
        match self {
            Self::V31 => "3.1",
            Self::V311 => "3.1.1",
            Self::V5 => "5.0",
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::V5
    }
}

/// MQTT Packet Type
#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PacketType {
    Reserved = 0,
    Connect,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

/// MQTT Reason Code
#[derive(Debug, TryFromPrimitive, Clone, Copy)]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0x00,
    GrantedQoS1 = 0x01,
    GrantedQoS2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NotMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIDInUse = 0x91,
    PacketIDNotFound = 0x92,
    RecvMaxExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdminAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaxConnectTime = 0xA0,
    SubIDNotSupported = 0xA1,
    WildcardSubNotSupported = 0xA2,
}

impl Default for ReasonCode {
    fn default() -> Self {
        Self::Success
    }
}

impl ReasonCode {
    pub fn to_v3(self) -> Result<u8, Error> {
        match self {
            ReasonCode::Success => Ok(0x00),
            ReasonCode::UnsupportedProtocolVersion => Ok(0x01),
            ReasonCode::ClientIdentifierNotValid => Ok(0x02),
            ReasonCode::ServerUnavailable => Ok(0x03),
            ReasonCode::BadUserNameOrPassword => Ok(0x04),
            ReasonCode::NotAuthorized => Ok(0x05),
            _ => Err(Error::ProtocolError("Unknown v3 reason code".into())),
        }
    }
}

impl From<&Error> for ReasonCode {
    fn from(e: &Error) -> Self {
        match e {
            Error::MalformedPacket => Self::MalformedPacket,
            Error::ProtocolError(_) => Self::ProtocolError,
            Error::UnsupportedProtocolVersion(_) => Self::UnsupportedProtocolVersion,
            Error::PacketTooLarge => ReasonCode::PacketTooLarge,
            Error::ClientIdentifierNotValid => ReasonCode::ClientIdentifierNotValid,
            Error::BadUserNameOrPassword => ReasonCode::BadUserNameOrPassword,
            Error::NotAuthorized => ReasonCode::NotAuthorized,
            Error::ServerUnavailable => ReasonCode::ServerUnavailable,
            _ => ReasonCode::UnspecifiedError,
        }
    }
}

/// MQTT Property
#[derive(Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum Property {
    PayloadFormatIndicator = 0x01,
    MessageExpiryInterval = 0x02,
    ContentType = 0x03,
    ResponseTopic = 0x08,
    CorrelationData = 0x09,
    SubIdentifier = 0x0B,
    SessionExpiryInterval = 0x11,
    AssignedClientIdentifier = 0x12,
    ServerKeepAlive = 0x13,
    AuthMethod = 0x15,
    AuthData = 0x16,
    RequestProblemInfo = 0x17,
    WillDelayInterval = 0x18,
    RequestResponseInfo = 0x19,
    ResponseInfo = 0x1A,
    ServerReference = 0x1C,
    ReasonString = 0x1F,
    ReceiveMaximum = 0x21,
    TopicAliasMaximum = 0x22,
    TopicAlias = 0x23,
    MaximumQoS = 0x24,
    RetainAvailable = 0x25,
    UserProperty = 0x26,
    MaxPacketSize = 0x27,
    WildcardSubAvailable = 0x28,
    SubIdentifierAvailable = 0x29,
    SharedSubAvailable = 0x2A,
}

/// Decode length
pub fn decode_length(src: &[u8]) -> Result<Option<(usize, usize)>, Error> {
    let mut length = 0;

    for (i, &byte) in src.iter().take(4).enumerate() {
        length |= usize::from(byte & 0x7f) << (i * 7);

        if byte & 0x80 == 0 {
            return Ok(Some((length, i + 1)));
        }

        if i == 3 {
            return Err(Error::MalformedPacket);
        }
    }

    Ok(None)
}

/// Encode length
pub fn encode_length(dst: &mut BytesMut, mut length: usize) -> Result<(), Error> {
    if length > 268_435_455 {
        return Err(Error::PacketTooLarge);
    }

    while length >= 128 {
        dst.put_u8((length as u8 & 0x7f) | 0x80);
        length >>= 7;
    }

    dst.put_u8(length as u8);

    Ok(())
}

/// Length bytes number
pub fn length_bytes(mut length: usize) -> usize {
    let mut length_bytes = 1;
    while length >= 128 {
        length >>= 7;
        length_bytes += 1;
    }
    length_bytes
}

/// Decode string
pub fn decode_string(src: &mut Bytes) -> Result<String, Error> {
    if src.len() < 2 {
        return Err(Error::MalformedPacket);
    }

    let length = src.get_u16() as usize;

    if src.len() < length {
        return Err(Error::MalformedPacket);
    }

    let bytes = src.split_to(length);
    let string = String::from_utf8(bytes.to_vec()).map_err(|_| Error::MalformedPacket)?;
    Ok(string)
}

/// Encode string
pub fn encode_string(dst: &mut BytesMut, string: &str) -> Result<(), Error> {
    let length = string.len();

    if length > 65535 {
        return Err(Error::ProtocolError("MQTT string exceeds 65535 bytes".into()));
    }

    dst.put_u16(length as u16);
    dst.extend_from_slice(string.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_len() {
        let mut dst = BytesMut::new();
        assert!(matches!(encode_length(&mut dst, 268_435_456), Err(Error::PacketTooLarge)));

        let mut dst = BytesMut::new();
        assert!(encode_length(&mut dst, 127).is_ok());
        assert_eq!(dst[..], [0x7F]);

        let mut dst = BytesMut::new();
        assert!(encode_length(&mut dst, 16_383).is_ok());
        assert_eq!(dst[..], [0xFF, 0x7F]);

        let mut dst = BytesMut::new();
        assert!(encode_length(&mut dst, 2_097_151).is_ok());
        assert_eq!(dst[..], [0xFF, 0xFF, 0x7F]);

        let mut dst = BytesMut::new();
        assert!(encode_length(&mut dst, 268_435_455).is_ok());
        assert_eq!(dst[..], [0xFF, 0xFF, 0xFF, 0x7F]);
    }

    #[test]
    fn test_decode_len() {
        let src = &[0xFF];
        assert!(matches!(decode_length(src), Ok(None)));

        let src = &[0x7F];
        assert!(matches!(decode_length(src), Ok(Some((127, 1)))));

        let src = &[0xFF, 0x7F];
        assert!(matches!(decode_length(src), Ok(Some((16_383, 2)))));

        let src = &[0xFF, 0xFF, 0x7F];
        assert!(matches!(decode_length(src), Ok(Some((2_097_151, 3)))));

        let src = &[0xFF, 0xFF, 0xFF, 0x7F];
        assert!(matches!(decode_length(src), Ok(Some((268_435_455, 4)))));

        let src = &[0xFF, 0xFF, 0xFF, 0xFF];
        assert!(matches!(decode_length(src), Err(Error::MalformedPacket)));
    }
}

use std::io;

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

pub const MAX_REMOTE_FRAME_LENGTH: usize = 4 * 1024 * 1024;
const JSON_FRAME_MARKER: u8 = 0x00;
const DATA_FRAME_MARKER: u8 = 0x01;
const AIRCRAFT_DATA_KIND: u8 = 0x01;
const CONTROL_DATA_KIND: u8 = 0x02;
const DATA_FRAME_HEADER_LENGTH: usize = 6;

// Every remote frame keeps the existing 4-byte big-endian length prefix. The
// body is either `0x00 + UTF-8 JSON` for control/status messages or
// `0x01 + kind + relay_id_len:u16 + link_id_len:u16 + ids + raw payload` for
// aircraft/control data. The latter deliberately never serializes payload as
// a JSON array of numbers.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyTarget {
    RelayWeb,
    LLink,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum RemoteMessage {
    RelayHello {
        relay_id: String,
    },
    HelloAck {
        server_id: String,
    },
    LinkAnnounce {
        relay_id: String,
        link_id: String,
        kind: String,
        #[serde(default)]
        priority: u32,
    },
    Heartbeat {
        relay_id: String,
    },
    ActivePath {
        relay_id: String,
        link_id: Option<String>,
    },
    LinkStatus {
        relay_id: String,
        link_id: String,
        adapter_ready: bool,
        last_tx_at: Option<u64>,
        tx_bytes: u64,
        tx_rate_bytes_per_second: f64,
        last_error: Option<String>,
    },
    AircraftData {
        relay_id: String,
        link_id: String,
        payload: Vec<u8>,
    },
    ControlData {
        relay_id: String,
        link_id: String,
        payload: Vec<u8>,
    },
    HttpProxyRequest {
        request_id: u64,
        target: ProxyTarget,
        method: String,
        uri: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    HttpProxyResponse {
        request_id: u64,
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    TcpProxyOpen {
        stream_id: u64,
    },
    TcpProxyData {
        stream_id: u64,
        data: Vec<u8>,
    },
    TcpProxyClose {
        stream_id: u64,
    },
    Rejected {
        reason: String,
    },
}

pub struct RemoteCodec {
    inner: LengthDelimitedCodec,
}

impl RemoteCodec {
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .length_field_length(4)
                .length_field_offset(0)
                .num_skip(4)
                .max_frame_length(MAX_REMOTE_FRAME_LENGTH)
                .big_endian()
                .new_codec(),
        }
    }
}

impl Default for RemoteCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RemoteCodec {
    type Item = RemoteMessage;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(frame) = self.inner.decode(source)? else {
            return Ok(None);
        };
        if frame.is_empty() {
            return Err(invalid_frame("remote frame is empty"));
        }

        match frame[0] {
            JSON_FRAME_MARKER => decode_json_message(&frame[1..]).map(Some),
            DATA_FRAME_MARKER => decode_data_message(&frame).map(Some),
            // Accept frames from the previous plain-TCP JSON protocol while
            // both endpoints are being upgraded. New data frames are binary.
            b'{' => decode_json_message(&frame).map(Some),
            marker => Err(invalid_frame(format!(
                "unknown remote frame marker 0x{marker:02X}"
            ))),
        }
    }
}

impl Encoder<RemoteMessage> for RemoteCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: RemoteMessage,
        destination: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let payload = match item {
            RemoteMessage::AircraftData {
                relay_id,
                link_id,
                payload,
            } => encode_data_message(AIRCRAFT_DATA_KIND, &relay_id, &link_id, &payload)?,
            RemoteMessage::ControlData {
                relay_id,
                link_id,
                payload,
            } => encode_data_message(CONTROL_DATA_KIND, &relay_id, &link_id, &payload)?,
            item => {
                let json = serde_json::to_vec(&item)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                let mut frame = BytesMut::with_capacity(json.len() + 1);
                frame.extend_from_slice(&[JSON_FRAME_MARKER]);
                frame.extend_from_slice(&json);
                frame
            }
        };
        self.inner.encode(payload.freeze(), destination)
    }
}

fn encode_data_message(
    kind: u8,
    relay_id: &str,
    link_id: &str,
    payload: &[u8],
) -> Result<BytesMut, io::Error> {
    let relay_id = relay_id.as_bytes();
    let link_id = link_id.as_bytes();
    if relay_id.len() > u16::MAX as usize || link_id.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "remote data message identifier is too long",
        ));
    }

    let mut frame = BytesMut::with_capacity(
        DATA_FRAME_HEADER_LENGTH + relay_id.len() + link_id.len() + payload.len(),
    );
    frame.extend_from_slice(&[DATA_FRAME_MARKER, kind]);
    frame.extend_from_slice(&(relay_id.len() as u16).to_be_bytes());
    frame.extend_from_slice(&(link_id.len() as u16).to_be_bytes());
    frame.extend_from_slice(relay_id);
    frame.extend_from_slice(link_id);
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn decode_json_message(frame: &[u8]) -> Result<RemoteMessage, io::Error> {
    serde_json::from_slice(frame).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn decode_data_message(frame: &[u8]) -> Result<RemoteMessage, io::Error> {
    if frame.len() < DATA_FRAME_HEADER_LENGTH {
        return Err(invalid_frame("remote data frame header is truncated"));
    }

    let kind = frame[1];
    let relay_id_length = u16::from_be_bytes([frame[2], frame[3]]) as usize;
    let link_id_length = u16::from_be_bytes([frame[4], frame[5]]) as usize;
    let relay_id_start = DATA_FRAME_HEADER_LENGTH;
    let link_id_start = relay_id_start + relay_id_length;
    let payload_start = link_id_start + link_id_length;
    if payload_start > frame.len() {
        return Err(invalid_frame("remote data frame fields are truncated"));
    }

    let relay_id = std::str::from_utf8(&frame[relay_id_start..link_id_start])
        .map_err(|error| invalid_frame(format!("relay id is not UTF-8: {error}")))?
        .to_owned();
    let link_id = std::str::from_utf8(&frame[link_id_start..payload_start])
        .map_err(|error| invalid_frame(format!("link id is not UTF-8: {error}")))?
        .to_owned();
    let payload = frame[payload_start..].to_vec();

    match kind {
        AIRCRAFT_DATA_KIND => Ok(RemoteMessage::AircraftData {
            relay_id,
            link_id,
            payload,
        }),
        CONTROL_DATA_KIND => Ok(RemoteMessage::ControlData {
            relay_id,
            link_id,
            payload,
        }),
        kind => Err(invalid_frame(format!(
            "unknown remote data message kind 0x{kind:02X}"
        ))),
    }
}

fn invalid_frame(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_frame_contains_payload_as_raw_bytes() {
        let payload = vec![0x00, 0xEB, 0x90, 0xFF, b'{', b'}'];
        let mut codec = RemoteCodec::new();
        let mut encoded = BytesMut::new();

        codec
            .encode(
                RemoteMessage::AircraftData {
                    relay_id: "relay-a".into(),
                    link_id: "serial-1".into(),
                    payload: payload.clone(),
                },
                &mut encoded,
            )
            .expect("aircraft data should encode");

        assert!(
            encoded
                .windows(payload.len())
                .any(|window| window == payload.as_slice()),
            "encoded frame must contain the payload bytes without JSON number encoding"
        );

        let frame_length = u32::from_be_bytes(
            encoded[..std::mem::size_of::<u32>()]
                .try_into()
                .expect("length prefix"),
        ) as usize;
        assert_eq!(
            frame_length,
            DATA_FRAME_HEADER_LENGTH + "relay-a".len() + "serial-1".len() + payload.len()
        );

        let decoded = codec
            .decode(&mut encoded)
            .expect("aircraft data should decode")
            .expect("aircraft frame");
        assert_eq!(
            decoded,
            RemoteMessage::AircraftData {
                relay_id: "relay-a".into(),
                link_id: "serial-1".into(),
                payload,
            }
        );
        assert!(encoded.is_empty());
    }

    #[test]
    fn control_data_frame_round_trips_binary_payload() {
        let expected = RemoteMessage::ControlData {
            relay_id: "relay-a".into(),
            link_id: "udp-1".into(),
            payload: vec![0x00, 0x01, 0xFE, 0xFF, b'[', b']'],
        };
        let mut codec = RemoteCodec::new();
        let mut encoded = BytesMut::new();

        codec
            .encode(expected.clone(), &mut encoded)
            .expect("control data should encode");

        assert_eq!(
            codec
                .decode(&mut encoded)
                .expect("control data should decode")
                .expect("control frame"),
            expected
        );
        assert!(encoded.is_empty());
    }
}

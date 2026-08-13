use relay_core::model::PathId;
use std::fmt;

pub const FRAME_LENGTH: usize = 32;
pub const FRAME_HEADER: [u8; 2] = [0xEB, 0x90];
pub const HEARTBEAT_TYPE: u8 = 0xFE;
pub const NO_AIRCRAFT_ID: u8 = 0xFF;
pub const HEARTBEAT_STATUS_NO_ACTIVE_PATH: u8 = 0x00;
pub const HEARTBEAT_STATUS_ACTIVE_PATH: u8 = 0x01;
const HEARTBEAT_METADATA_START: usize = 6;
const HEARTBEAT_METADATA_LENGTH: usize = 24;
const HEARTBEAT_ID_LENGTH_LIMIT: usize = HEARTBEAT_METADATA_LENGTH - 2;

#[derive(Debug, Clone, Default)]
pub struct AircraftFrameAssembler {
    source_path: Option<PathId>,
    buffer: Vec<u8>,
}

impl AircraftFrameAssembler {
    pub fn push(&mut self, source_path: &PathId, data: &[u8]) -> Vec<[u8; FRAME_LENGTH]> {
        if self.source_path.as_ref() != Some(source_path) {
            self.source_path = Some(source_path.clone());
            self.buffer.clear();
        }
        self.buffer.extend_from_slice(data);

        let mut frames = Vec::new();
        loop {
            let Some(header_position) = find_header(&self.buffer) else {
                retain_possible_header(&mut self.buffer);
                break;
            };
            if header_position > 0 {
                self.buffer.drain(..header_position);
            }
            if self.buffer.len() < FRAME_LENGTH {
                break;
            }

            let mut frame = [0u8; FRAME_LENGTH];
            frame.copy_from_slice(&self.buffer[..FRAME_LENGTH]);
            if is_valid_frame(&frame) {
                self.buffer.drain(..FRAME_LENGTH);
                frames.push(frame);
            } else {
                self.buffer.drain(..1);
            }
        }
        frames
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatFrameError {
    PathMetadataTooLong { bytes: usize, limit: usize },
}

impl fmt::Display for HeartbeatFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathMetadataTooLong { bytes, limit } => write!(
                formatter,
                "heartbeat path metadata is {bytes} bytes but the limit is {limit}"
            ),
        }
    }
}

impl std::error::Error for HeartbeatFrameError {}

pub fn crc_bytes(data: &[u8]) -> [u8; 2] {
    let mut crc = 0u16;
    for &byte in data {
        crc ^= byte as u16;
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x8408
            } else {
                crc >> 1
            };
        }
    }
    [crc as u8, (crc >> 8) as u8]
}

pub fn is_valid_frame(frame: &[u8]) -> bool {
    frame.len() == FRAME_LENGTH
        && frame[..2] == FRAME_HEADER
        && crc_bytes(&frame[2..30]) == [frame[30], frame[31]]
}

pub fn build_heartbeat_frame(
    sequence: u8,
    active_path: Option<&PathId>,
) -> Result<[u8; FRAME_LENGTH], HeartbeatFrameError> {
    let mut frame = [0u8; FRAME_LENGTH];
    frame[..2].copy_from_slice(&FRAME_HEADER);
    frame[2] = HEARTBEAT_TYPE;
    frame[3] = sequence;
    frame[4] = NO_AIRCRAFT_ID;
    frame[5] = if active_path.is_some() {
        HEARTBEAT_STATUS_ACTIVE_PATH
    } else {
        HEARTBEAT_STATUS_NO_ACTIVE_PATH
    };

    if let Some(path) = active_path {
        let relay_id = path.relay_id.as_bytes();
        let link_id = path.link_id.as_bytes();
        let metadata_bytes = relay_id.len() + link_id.len();
        if metadata_bytes > HEARTBEAT_ID_LENGTH_LIMIT {
            return Err(HeartbeatFrameError::PathMetadataTooLong {
                bytes: metadata_bytes,
                limit: HEARTBEAT_ID_LENGTH_LIMIT,
            });
        }
        let mut offset = HEARTBEAT_METADATA_START;
        frame[offset] = relay_id.len() as u8;
        offset += 1;
        frame[offset..offset + relay_id.len()].copy_from_slice(relay_id);
        offset += relay_id.len();
        frame[offset] = link_id.len() as u8;
        offset += 1;
        frame[offset..offset + link_id.len()].copy_from_slice(link_id);
    } else {
        frame[HEARTBEAT_METADATA_START] = 0;
        frame[HEARTBEAT_METADATA_START + 1] = 0;
    }

    let [c0, c1] = crc_bytes(&frame[2..30]);
    frame[30] = c0;
    frame[31] = c1;
    Ok(frame)
}

fn find_header(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(FRAME_HEADER.len())
        .position(|candidate| candidate == FRAME_HEADER)
}

fn retain_possible_header(buffer: &mut Vec<u8>) {
    if buffer.last().copied() == Some(FRAME_HEADER[0]) {
        let last = buffer.pop().expect("buffer has a last byte");
        buffer.clear();
        buffer.push(last);
    } else {
        buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::model::PathId;

    fn valid_frame(seed: u8) -> [u8; FRAME_LENGTH] {
        let mut frame = [0u8; FRAME_LENGTH];
        frame[0] = FRAME_HEADER[0];
        frame[1] = FRAME_HEADER[1];
        for (index, byte) in frame[2..30].iter_mut().enumerate() {
            *byte = seed.wrapping_add(index as u8);
        }
        let [c0, c1] = crc_bytes(&frame[2..30]);
        frame[30] = c0;
        frame[31] = c1;
        frame
    }

    #[test]
    fn crc_matches_981c_reference_table() {
        assert_eq!(crc_bytes(&[0x01]), [0x89, 0x11]);
    }

    #[test]
    fn assembler_reassembles_only_valid_32_byte_frames() {
        let path = PathId::new("relay-a", "serial-radio-1");
        let frame = valid_frame(0x10);
        let mut assembler = AircraftFrameAssembler::default();

        assert!(assembler.push(&path, &frame[..13]).is_empty());
        assert_eq!(assembler.push(&path, &frame[13..]), vec![frame]);
    }

    #[test]
    fn assembler_discards_bad_crc_and_resynchronizes() {
        let path = PathId::new("relay-a", "serial-radio-1");
        let mut invalid = valid_frame(0x20);
        invalid[30] ^= 0xFF;
        let valid = valid_frame(0x40);
        let mut input = invalid.to_vec();
        input.extend_from_slice(&valid);
        let mut assembler = AircraftFrameAssembler::default();

        assert_eq!(assembler.push(&path, &input), vec![valid]);
    }

    #[test]
    fn heartbeat_uses_eb90_type_and_path_metadata() {
        let path = PathId::new("relay-a", "serial-radio-1");
        let frame = build_heartbeat_frame(7, Some(&path)).expect("path should fit");

        assert!(is_valid_frame(&frame));
        assert_eq!(&frame[..2], &FRAME_HEADER);
        assert_eq!(frame[2], HEARTBEAT_TYPE);
        assert_eq!(frame[3], 7);
        assert_eq!(frame[4], NO_AIRCRAFT_ID);
        assert_eq!(frame[5], HEARTBEAT_STATUS_ACTIVE_PATH);
        assert_eq!(frame[6], 7);
        assert_eq!(&frame[7..14], b"relay-a");
        assert_eq!(frame[14], 14);
        assert_eq!(&frame[15..29], b"serial-radio-1");
    }

    #[test]
    fn heartbeat_without_active_path_is_still_crc_valid() {
        let frame = build_heartbeat_frame(8, None).expect("empty path should fit");

        assert!(is_valid_frame(&frame));
        assert_eq!(frame[2], HEARTBEAT_TYPE);
        assert_eq!(frame[5], HEARTBEAT_STATUS_NO_ACTIVE_PATH);
        assert_eq!(frame[6], 0);
        assert_eq!(frame[7], 0);
    }
}

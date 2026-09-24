use crate::ground_station_protocol::crc_bytes;

pub const TURNTABLE_FRAME_LENGTH: usize = 32;
pub const TURNTABLE_HEADER: [u8; 2] = [0xEB, 0x90];
pub const TURNTABLE_DEST_ADDR: u8 = 0x02;
pub const TURNTABLE_SRC_ADDR: u8 = 0x01;
pub const TURNTABLE_FRAME_ID: u8 = 0xC2;

/// 按照 16 位累加和（低字节在前）计算校验码
pub fn checksum16_bytes(data: &[u8]) -> [u8; 2] {
    let mut sum = 0u32;
    for &byte in data {
        sum += byte as u32;
    }
    let checksum = (sum & 0xFFFF) as u16;
    [(checksum & 0xFF) as u8, ((checksum >> 8) & 0xFF) as u8]
}

/// 校验是否为符合协议规范的 32 字节伺服转台控制帧
pub fn is_valid_turntable_frame(frame: &[u8]) -> bool {
    if frame.len() != TURNTABLE_FRAME_LENGTH
        || frame[0] != TURNTABLE_HEADER[0]
        || frame[1] != TURNTABLE_HEADER[1]
        || frame[2] != TURNTABLE_DEST_ADDR
        || frame[3] != TURNTABLE_SRC_ADDR
        || frame[4] != TURNTABLE_FRAME_ID
    {
        return false;
    }

    let actual = [frame[30], frame[31]];
    // 包含 EB 90 的累加和（前 30 字节：frame[0..30]），同时兼容跳过 EB 90（frame[2..30]）与标准 CRC16
    checksum16_bytes(&frame[0..30]) == actual
        || checksum16_bytes(&frame[2..30]) == actual
        || crc_bytes(&frame[2..30]) == actual
}

/// 伺服转台控制帧组包器，处理 TCP 分片与粘包
#[derive(Debug, Clone, Default)]
pub struct TurntableFrameAssembler {
    buffer: Vec<u8>,
}

impl TurntableFrameAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// 处理输入的控制数据：
    /// 摘出符合转台协议的 32 字节控制帧（返回到 turntable_frames），
    /// 将其余非转台控制数据作为透传数据（返回到 passthrough）。
    pub fn process(&mut self, data: &[u8]) -> (Vec<[u8; TURNTABLE_FRAME_LENGTH]>, Vec<u8>) {
        self.buffer.extend_from_slice(data);
        let mut turntable_frames = Vec::new();
        let mut passthrough = Vec::new();

        loop {
            if self.buffer.is_empty() {
                break;
            }

            let Some(header_position) = find_header(&self.buffer) else {
                if self.buffer.last().copied() == Some(TURNTABLE_HEADER[0]) {
                    let non_candidate_len = self.buffer.len() - 1;
                    if non_candidate_len > 0 {
                        passthrough.extend(self.buffer.drain(..non_candidate_len));
                    }
                } else {
                    passthrough.extend(self.buffer.drain(..));
                }
                break;
            };

            if header_position > 0 {
                passthrough.extend(self.buffer.drain(..header_position));
            }

            // 检查前导标识 (至少需要 5 字节判断是否为转台控制帧)
            if self.buffer.len() < 5 {
                let mut matches_prefix = true;
                if self.buffer.len() >= 3 && self.buffer[2] != TURNTABLE_DEST_ADDR {
                    matches_prefix = false;
                }
                if self.buffer.len() >= 4 && self.buffer[3] != TURNTABLE_SRC_ADDR {
                    matches_prefix = false;
                }
                if matches_prefix {
                    break;
                } else {
                    passthrough.push(self.buffer.remove(0));
                    continue;
                }
            }

            if self.buffer[2] != TURNTABLE_DEST_ADDR
                || self.buffer[3] != TURNTABLE_SRC_ADDR
                || self.buffer[4] != TURNTABLE_FRAME_ID
            {
                // 不是转台控制帧，移出 1 字节至透传数据，继续检索下一个候选
                passthrough.push(self.buffer.remove(0));
                continue;
            }

            // 长度不足 32 字节，等待后续数据
            if self.buffer.len() < TURNTABLE_FRAME_LENGTH {
                break;
            }

            let mut frame = [0u8; TURNTABLE_FRAME_LENGTH];
            frame.copy_from_slice(&self.buffer[..TURNTABLE_FRAME_LENGTH]);
            if is_valid_turntable_frame(&frame) {
                self.buffer.drain(..TURNTABLE_FRAME_LENGTH);
                turntable_frames.push(frame);
            } else {
                // CRC/校验和失败，移出 1 字节至透传数据
                passthrough.push(self.buffer.remove(0));
            }
        }

        (turntable_frames, passthrough)
    }

    pub fn push(&mut self, data: &[u8]) -> Vec<[u8; TURNTABLE_FRAME_LENGTH]> {
        self.process(data).0
    }
}

fn find_header(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(TURNTABLE_HEADER.len())
        .position(|candidate| candidate == TURNTABLE_HEADER)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_turntable_frame(payload_byte: u8) -> [u8; TURNTABLE_FRAME_LENGTH] {
        let mut frame = [0u8; TURNTABLE_FRAME_LENGTH];
        frame[0] = TURNTABLE_HEADER[0];
        frame[1] = TURNTABLE_HEADER[1];
        frame[2] = TURNTABLE_DEST_ADDR;
        frame[3] = TURNTABLE_SRC_ADDR;
        frame[4] = TURNTABLE_FRAME_ID;
        for byte in &mut frame[5..30] {
            *byte = payload_byte;
        }
        let [c0, c1] = crc_bytes(&frame[2..30]);
        frame[30] = c0;
        frame[31] = c1;
        frame
    }

    fn build_test_turntable_frame_checksum16(payload_byte: u8) -> [u8; TURNTABLE_FRAME_LENGTH] {
        let mut frame = [0u8; TURNTABLE_FRAME_LENGTH];
        frame[0] = TURNTABLE_HEADER[0];
        frame[1] = TURNTABLE_HEADER[1];
        frame[2] = TURNTABLE_DEST_ADDR;
        frame[3] = TURNTABLE_SRC_ADDR;
        frame[4] = TURNTABLE_FRAME_ID;
        for byte in &mut frame[5..30] {
            *byte = payload_byte;
        }
        let [c0, c1] = checksum16_bytes(&frame[0..30]);
        frame[30] = c0;
        frame[31] = c1;
        frame
    }

    #[test]
    fn test_valid_turntable_frame_validation_checksum16() {
        let frame = build_test_turntable_frame_checksum16(0x5A);
        assert!(is_valid_turntable_frame(&frame));

        // 验证包含 EB 90 的累加和计算：
        // 0xEB + 0x90 + 0x02 + 0x01 + 0xC2 + 25 * 0x5A = 235 + 144 + 2 + 1 + 194 + 2250 = 2826 = 0x0B0A
        // 小端序存储为 [0x0A, 0x0B]
        assert_eq!([frame[30], frame[31]], [0x0A, 0x0B]);
    }

    #[test]
    fn test_valid_turntable_frame_validation() {
        let frame = build_test_turntable_frame(0x5A);
        assert!(is_valid_turntable_frame(&frame));
    }

    #[test]
    fn test_turntable_frame_invalid_crc() {
        let mut frame = build_test_turntable_frame(0x5A);
        frame[30] ^= 0xFF;
        assert!(!is_valid_turntable_frame(&frame));
    }

    #[test]
    fn test_turntable_frame_invalid_id() {
        let mut frame = build_test_turntable_frame(0x5A);
        frame[4] = 0xC1; // 错误识别码
        let [c0, c1] = crc_bytes(&frame[2..30]);
        frame[30] = c0;
        frame[31] = c1;
        assert!(!is_valid_turntable_frame(&frame));
    }

    #[test]
    fn test_assembler_single_frame() {
        let mut assembler = TurntableFrameAssembler::new();
        let frame = build_test_turntable_frame(0x11);
        let result = assembler.push(&frame);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], frame);
    }

    #[test]
    fn test_assembler_split_chunks() {
        let mut assembler = TurntableFrameAssembler::new();
        let frame = build_test_turntable_frame(0x22);
        assert!(assembler.push(&frame[..10]).is_empty());
        assert!(assembler.push(&frame[10..20]).is_empty());
        let result = assembler.push(&frame[20..]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], frame);
    }

    #[test]
    fn test_assembler_ignores_other_eb90_frames() {
        let mut assembler = TurntableFrameAssembler::new();
        // 构造一个心跳帧或飞机帧 (目的地址非 0x02)
        let mut other_frame = [0u8; 32];
        other_frame[0] = 0xEB;
        other_frame[1] = 0x90;
        other_frame[2] = 0xFE; // 心跳类型
        let target_frame = build_test_turntable_frame(0x33);

        let mut data = other_frame.to_vec();
        data.extend_from_slice(&target_frame);

        let result = assembler.push(&data);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], target_frame);
    }

    #[test]
    fn test_assembler_handles_garbage_prefix() {
        let mut assembler = TurntableFrameAssembler::new();
        let garbage = vec![0x12, 0x34, 0x56, 0xEB, 0x00, 0x78];
        let frame = build_test_turntable_frame(0x44);
        let mut data = garbage;
        data.extend_from_slice(&frame);

        let result = assembler.push(&data);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], frame);
    }

    #[test]
    fn test_assembler_extracts_turntable_frame_and_passes_through_other_data() {
        let mut assembler = TurntableFrameAssembler::new();
        let aircraft_data1 = vec![0x01, 0x02, 0x03, 0x04];
        let turntable_frame = build_test_turntable_frame_checksum16(0x77);
        let aircraft_data2 = vec![0x05, 0x06, 0x07];

        let mut mixed = aircraft_data1.clone();
        mixed.extend_from_slice(&turntable_frame);
        mixed.extend_from_slice(&aircraft_data2);

        let (frames, passthrough) = assembler.process(&mixed);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], turntable_frame);

        let mut expected_passthrough = aircraft_data1;
        expected_passthrough.extend_from_slice(&aircraft_data2);
        assert_eq!(passthrough, expected_passthrough);
    }

    #[test]
    fn test_assembler_pure_turntable_frame_has_empty_passthrough() {
        let mut assembler = TurntableFrameAssembler::new();
        let turntable_frame = build_test_turntable_frame_checksum16(0x88);

        let (frames, passthrough) = assembler.process(&turntable_frame);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], turntable_frame);
        assert!(passthrough.is_empty());
    }

    #[test]
    fn test_assembler_pure_aircraft_data_passes_through_completely() {
        let mut assembler = TurntableFrameAssembler::new();
        let aircraft_data = vec![0xFD, 0x09, 0x00, 0x00, 0x1A, 0x01, 0x01];

        let (frames, passthrough) = assembler.process(&aircraft_data);
        assert!(frames.is_empty());
        assert_eq!(passthrough, aircraft_data);
    }
}

use std::sync::{Arc, Mutex};
use std::time::Duration;

use relay_core::config::IoControllerConfig;
use relay_core::model::IoControllerSnapshot;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use tracing::{info, warn};

pub fn calculate_crc16(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &byte in data {
        crc ^= byte as u16;
        for _ in 0..8 {
            if (crc & 0x0001) != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

pub fn append_crc16(payload: &[u8]) -> Vec<u8> {
    let crc = calculate_crc16(payload);
    let mut buf = payload.to_vec();
    buf.push((crc & 0xFF) as u8);
    buf.push(((crc >> 8) & 0xFF) as u8);
    buf
}

pub fn verify_crc16(data: &[u8]) -> bool {
    if data.len() < 3 {
        return false;
    }
    let len = data.len();
    let payload = &data[..len - 2];
    let expected = calculate_crc16(payload);
    let actual = (data[len - 2] as u16) | ((data[len - 1] as u16) << 8);
    expected == actual
}

pub enum IoCommand {
    SetChannel {
        channel: u8,
        state: bool,
        response: oneshot::Sender<Result<(), String>>,
    },
    SetAll {
        state: bool,
        response: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Clone)]
pub struct IoControllerHandle {
    command_tx: mpsc::Sender<IoCommand>,
    snapshot: Arc<Mutex<IoControllerSnapshot>>,
}

impl IoControllerHandle {
    pub async fn set_channel(&self, channel: u8, state: bool) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(IoCommand::SetChannel {
                channel,
                state,
                response: tx,
            })
            .await
            .map_err(|_| "IO 控制器后台任务已停止".to_string())?;
        rx.await.map_err(|_| "等待 IO 控制响应超时".to_string())?
    }

    pub async fn set_all(&self, state: bool) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(IoCommand::SetAll {
                state,
                response: tx,
            })
            .await
            .map_err(|_| "IO 控制器后台任务已停止".to_string())?;
        rx.await.map_err(|_| "等待 IO 控制响应超时".to_string())?
    }

    pub fn snapshot(&self) -> IoControllerSnapshot {
        self.snapshot
            .lock()
            .expect("io snapshot mutex poisoned")
            .clone()
    }
}

pub fn start_io_controller(
    config: Option<IoControllerConfig>,
) -> (IoControllerHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<IoCommand>(16);
    let initial_snapshot = IoControllerSnapshot {
        enabled: config.is_some(),
        connected: false,
        channels: vec![false; 16],
        last_update_at: None,
        last_error: None,
    };
    let snapshot_arc = Arc::new(Mutex::new(initial_snapshot));

    let handle = IoControllerHandle {
        command_tx: cmd_tx,
        snapshot: snapshot_arc.clone(),
    };

    let join_handle = tokio::spawn(async move {
        let Some(cfg) = config else {
            return;
        };
        run_worker(cfg, cmd_rx, snapshot_arc).await;
    });

    (handle, join_handle)
}

fn now_epoch_ms_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_default()
}

async fn run_worker(
    config: IoControllerConfig,
    mut cmd_rx: mpsc::Receiver<IoCommand>,
    snapshot: Arc<Mutex<IoControllerSnapshot>>,
) {
    info!(
        port = %config.port,
        baud_rate = config.baud_rate,
        slave_id = config.slave_id,
        "启动 IO 控制器 (Modbus-RTU) 驱动任务"
    );

    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(200));

    loop {
        info!(port = %config.port, "尝试连接 IO 控制器串口...");
        let port_result = tokio_serial::new(&config.port, config.baud_rate)
            .timeout(Duration::from_millis(500))
            .open_native_async();

        let mut port = match port_result {
            Ok(p) => {
                info!(port = %config.port, "IO 控制器串口打开成功");
                p
            }
            Err(e) => {
                let err_msg = format!("无法打开串口 {}: {}", config.port, e);
                warn!("{}", err_msg);
                {
                    let mut s = snapshot.lock().expect("io snapshot mutex poisoned");
                    s.connected = false;
                    s.last_error = Some(err_msg);
                }
                tokio::select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            IoCommand::SetChannel { response, .. } | IoCommand::SetAll { response, .. } => {
                                let _ = response.send(Err("串口未连接".to_string()));
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                }
                continue;
            }
        };

        // 通信主循环
        let mut poll_timer = tokio::time::interval(poll_interval);
        poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        'inner: loop {
            tokio::select! {
                _ = poll_timer.tick() => {
                    if let Err(e) = poll_status(&mut port, &config, &snapshot).await {
                        warn!(port = %config.port, error = %e, "IO 控制器轮询状态失败，准备重连");
                        {
                            let mut s = snapshot.lock().expect("io snapshot mutex poisoned");
                            s.connected = false;
                            s.last_error = Some(e);
                        }
                        break 'inner;
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        IoCommand::SetChannel { channel, state, response } => {
                            let res = handle_set_channel(&mut port, &config, channel, state, &snapshot).await;
                            let _ = response.send(res);
                        }
                        IoCommand::SetAll { state, response } => {
                            let res = handle_set_all(&mut port, &config, state, &snapshot).await;
                            let _ = response.send(res);
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn flush_read(port: &mut SerialStream) {
    let mut discard = [0u8; 256];
    while let Ok(n) = tokio::time::timeout(Duration::from_millis(20), port.read(&mut discard)).await {
        if n.unwrap_or(0) == 0 {
            break;
        }
    }
}

async fn poll_status(
    port: &mut SerialStream,
    config: &IoControllerConfig,
    snapshot: &Arc<Mutex<IoControllerSnapshot>>,
) -> Result<(), String> {
    flush_read(port).await;

    // 读 16 路线圈：从机地址, 功能码01, 起始地址0000, 数量0010
    let cmd = append_crc16(&[config.slave_id, 0x01, 0x00, 0x00, 0x00, 0x10]);
    port.write_all(&cmd)
        .await
        .map_err(|e| format!("发送读指令失败: {e}"))?;
    port.flush().await.map_err(|e| format!("刷新串口失败: {e}"))?;

    // 应答长度：从机1(1B) + 功能码(1B) + 字节数(1B, 0x02) + 数据(2B) + CRC(2B) = 7 字节
    let mut resp = [0u8; 7];
    tokio::time::timeout(Duration::from_millis(400), port.read_exact(&mut resp))
        .await
        .map_err(|_| "读取应答超时".to_string())?
        .map_err(|e| format!("读取应答失败: {e}"))?;

    if !verify_crc16(&resp) {
        return Err("应答 CRC 校验错误".to_string());
    }

    if resp[0] != config.slave_id || resp[1] != 0x01 || resp[2] != 0x02 {
        return Err(format!("应答报文格式不匹配: {:02X?}", resp));
    }

    let b0 = resp[3];
    let b1 = resp[4];
    let mut channels = vec![false; 16];
    for i in 0..8 {
        channels[i] = (b0 & (1 << i)) != 0;
    }
    for i in 0..8 {
        channels[8 + i] = (b1 & (1 << i)) != 0;
    }

    {
        let mut s = snapshot.lock().expect("io snapshot mutex poisoned");
        s.connected = true;
        s.channels = channels;
        s.last_update_at = Some(now_epoch_ms_string());
        s.last_error = None;
    }

    Ok(())
}

async fn handle_set_channel(
    port: &mut SerialStream,
    config: &IoControllerConfig,
    channel: u8,
    state: bool,
    snapshot: &Arc<Mutex<IoControllerSnapshot>>,
) -> Result<(), String> {
    if !(1..=16).contains(&channel) {
        return Err(format!("通道号无效 (1~16): {}", channel));
    }

    flush_read(port).await;

    let coil_addr = (channel - 1) as u8;
    let val_hi = if state { 0xFF } else { 0x00 };
    // 写单个线圈 (0x05): 从机(1B), 功能码05(1B), 地址高(00), 地址低(coil_addr), 值高(val_hi), 值低(00) + CRC
    let cmd = append_crc16(&[config.slave_id, 0x05, 0x00, coil_addr, val_hi, 0x00]);

    port.write_all(&cmd)
        .await
        .map_err(|e| format!("发送控制指令失败: {e}"))?;
    port.flush().await.map_err(|e| format!("刷新串口失败: {e}"))?;

    let mut resp = [0u8; 8];
    tokio::time::timeout(Duration::from_millis(400), port.read_exact(&mut resp))
        .await
        .map_err(|_| "控制响应超时".to_string())?
        .map_err(|e| format!("读取控制响应失败: {e}"))?;

    if !verify_crc16(&resp) {
        return Err("控制响应 CRC 校验错误".to_string());
    }

    // 立即执行一次状态查询以更新快照
    let _ = poll_status(port, config, snapshot).await;

    Ok(())
}

async fn handle_set_all(
    port: &mut SerialStream,
    config: &IoControllerConfig,
    state: bool,
    snapshot: &Arc<Mutex<IoControllerSnapshot>>,
) -> Result<(), String> {
    flush_read(port).await;

    let val = if state { 0xFF } else { 0x00 };
    // 写多个线圈 (0x0F): 从机(1B), 功能码0F(1B), 起始0000(2B), 数量0010(2B), 字节数02(1B), 数据(2B) + CRC(2B)
    let cmd = append_crc16(&[
        config.slave_id,
        0x0F,
        0x00,
        0x00,
        0x00,
        0x10,
        0x02,
        val,
        val,
    ]);

    port.write_all(&cmd)
        .await
        .map_err(|e| format!("发送批量控制指令失败: {e}"))?;
    port.flush().await.map_err(|e| format!("刷新串口失败: {e}"))?;

    let mut resp = [0u8; 8];
    tokio::time::timeout(Duration::from_millis(400), port.read_exact(&mut resp))
        .await
        .map_err(|_| "批量控制响应超时".to_string())?
        .map_err(|e| format!("读取批量响应失败: {e}"))?;

    if !verify_crc16(&resp) {
        return Err("批量控制响应 CRC 校验错误".to_string());
    }

    // 立即执行一次状态查询以更新快照
    let _ = poll_status(port, config, snapshot).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_modbus_crc16() {
        // 01 01 00 00 00 10 -> CRC 应该是 3D C6 (C6 为高字节，3D 为低字节)
        let cmd = append_crc16(&[0x01, 0x01, 0x00, 0x00, 0x00, 0x10]);
        assert_eq!(cmd, vec![0x01, 0x01, 0x00, 0x00, 0x00, 0x10, 0x3D, 0xC6]);
        assert!(verify_crc16(&cmd));
    }
}

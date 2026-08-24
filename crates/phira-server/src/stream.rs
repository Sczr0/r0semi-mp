//! 协议帧层（§6.1）——原版 `phira-mp-common` 的 `Stream` 移植（Apache-2.0，TeamFlos）。
//!
//! 职责（不含心跳判定——那是 core 会话层的生命周期逻辑，阶段 2 接线）：
//! - 版本握手：客户端先发 1 字节版本号（当前 v1），服务端读取；服务端模式写版本
//! - 帧格式：`ULEB128 长度 + 载荷`，载荷以 `u8` 命令 tag 开头（§6.1）
//! - 包上限 2 MiB；长度字段超过 32 bit 拒绝（防攻击，§6.1）
//! - 有界发送队列（1024）+ 后台发送任务（写失败仅记录，不阻塞业务）
//! - 接收任务逐帧解码并交给 handler；解码失败断开（原版语义）
//!
//! 热路径（§6.5-17）：`send` 只入队；编码由发送任务统一做，一次编码共享缓冲。

use std::{
    future::Future,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Result, bail};
use phira_api::{BinaryData, decode_packet, encode_packet};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{error, trace, warn};

/// 协议版本号（§6.1：客户端发 1 字节，当前 v1）。
pub const PROTOCOL_VERSION: u8 = 1;

/// 单包载荷上限（§6.1：协议上限 2 MiB；鉴权后放开到此值）。
pub const MAX_PACKET_SIZE: u32 = 2 * 1024 * 1024;

/// 鉴权前帧上限（§10.4 红线：握手 + token ≤32B 之外无合法大帧，堵死未鉴权 2MiB 帧攻击）。
pub const PRE_AUTH_MAX_PACKET: u32 = 4 * 1024;

/// 双向帧流：`S` = 发送载荷类型（服务端侧 = `ServerCommand`），`R` = 接收载荷类型。
///
/// `new` 建立握手 + 启动 send/recv 两个后台任务；`drop` 中止两者（原版语义：
/// 会话结束即断开）。
pub struct Stream<S, R> {
    /// 协商后的版本号（服务端模式 = 客户端发来的版本）。
    version: u8,

    /// 发送队列发送端（clone 到 handler，供其主动回包）。
    send_tx: Arc<mpsc::Sender<S>>,

    /// `Option`：`await_closed` 取出后置空；Drop 时 abort 仍在的 handle。
    send_task_handle: Option<JoinHandle<()>>,
    recv_task_handle: Option<JoinHandle<Result<()>>>,

    _marker: PhantomData<(S, R)>,
}

impl<S, R> Stream<S, R>
where
    S: BinaryData + std::fmt::Debug + Send + Sync + 'static,
    R: BinaryData + std::fmt::Debug + Send + 'static,
{
    /// 建立帧流。
    ///
    /// - `version: None` = **服务端模式**：读客户端发来的 1 字节版本；
    ///   `Some(v)` = **客户端模式**：写版本。
    /// - `handler` 每收到一帧调用一次（拿到发送端 + 载荷），异步处理。
    /// - `packet_limit`：当前帧上限（§10.4：鉴权前 ~4KiB）；recv 任务每次读长度后
    ///   取最新值——会话层鉴权通过后 `store(MAX_PACKET_SIZE)` 即放开。
    ///
    /// # Errors
    ///
    /// 握手读写失败 / 启动任务失败。
    ///
    /// # Panics
    ///
    /// 编码载荷超过 2 MiB（`u32::try_from` 失败，理论上不发生——帧上限在接收侧）。
    pub async fn new<F>(
        version: Option<u8>,
        stream: TcpStream,
        mut handler: Box<dyn FnMut(Arc<mpsc::Sender<S>>, R) -> F + Send + Sync>,
        packet_limit: Arc<AtomicU32>,
    ) -> Result<Self>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        stream.set_nodelay(true)?;
        let (mut read, mut write) = stream.into_split();
        let version = if let Some(version) = version {
            write.write_u8(version).await?;
            version
        } else {
            read.read_u8().await?
        };

        let (send_tx, mut send_rx) = mpsc::channel(1024);
        let send_tx = Arc::new(send_tx);
        let send_task_handle = tokio::spawn({
            async move {
                let mut buffer = Vec::new();
                let mut len_buf = [0u8; 5];
                while let Some(payload) = send_rx.recv().await {
                    buffer.clear();
                    encode_packet(&payload, &mut buffer);
                    trace!("sending {} bytes ({payload:?}): {buffer:?}", buffer.len());

                    // ULEB128 长度前缀（§6.1）：载荷 ≤ 2 MiB → 最多 3 字节，缓冲 5 够用
                    let mut x = u32::try_from(buffer.len()).expect("payload ≤ 2MiB fits u32");
                    let mut n = 0;
                    loop {
                        len_buf[n] = (x & 0x7f) as u8;
                        n += 1;
                        x >>= 7;
                        if x == 0 {
                            break;
                        }
                        len_buf[n - 1] |= 0x80;
                    }

                    if let Err(err) = async {
                        write.write_all(&len_buf[..n]).await?;
                        write.write_all(&buffer).await?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await
                    {
                        error!("failed to send: {err:?}");
                    }
                }
            }
        });

        let recv_task_handle = tokio::spawn({
            let send_tx = Arc::clone(&send_tx);
            async move {
                let mut buffer = Vec::new();
                loop {
                    // ULEB128 长度：>32 bit 拒绝（防攻击，§6.1）
                    let mut len = 0u32;
                    let mut pos = 0;
                    loop {
                        let byte = read.read_u8().await?;
                        len |= u32::from(byte & 0x7f) << pos;
                        pos += 7;
                        if byte & 0x80 == 0 {
                            break;
                        }
                        if pos > 32 {
                            bail!("invalid length");
                        }
                    }
                    if len > packet_limit.load(Ordering::SeqCst) {
                        bail!(
                            "data packet too large (limit {})",
                            packet_limit.load(Ordering::SeqCst)
                        );
                    }
                    let len = len as usize;

                    buffer.resize(len, 0);
                    read.read_exact(&mut buffer).await?;
                    trace!("received {} bytes: {buffer:?}", buffer.len());

                    let payload: R = match decode_packet(&buffer) {
                        Ok(val) => val,
                        Err(err) => {
                            warn!("invalid packet: {err:?} {buffer:?}");
                            break;
                        }
                    };
                    trace!("decodes to {payload:?}");
                    handler(Arc::clone(&send_tx), payload).await;
                }
                Ok(())
            }
        });

        Ok(Self {
            version,
            send_tx,
            send_task_handle: Some(send_task_handle),
            recv_task_handle: Some(recv_task_handle),
            _marker: PhantomData,
        })
    }

    /// 协商后的协议版本。
    #[must_use]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// 异步入队一帧（仅入队，编码/发送由后台任务完成；队列满则等待）。
    ///
    /// # Errors
    ///
    /// 对端已断开 / 发送任务已退出 → `mpsc` 发送失败。
    #[allow(dead_code)] // 阶段 2 会话层（事件编码投递 §6.6 表 2）接入后使用
    pub async fn send(&self, payload: S) -> Result<()> {
        self.send_tx.send(payload).await?;
        Ok(())
    }

    /// 阻塞入队一帧（非 async 上下文用，如 Drop 前的最后通知）。
    ///
    /// # Errors
    ///
    /// 对端已断开 → `mpsc` 发送失败。
    #[allow(dead_code)] // 阶段 2 会话层接入后使用
    pub fn blocking_send(&self, payload: S) -> Result<()> {
        self.send_tx.blocking_send(payload)?;
        Ok(())
    }

    /// 等待接收任务结束（连接断开 / 解码失败 / 对端关闭）；期间保持发送任务存活。
    ///
    /// 会话层（阶段 2）在此之后收尾：归还用户、拆房间任务等。
    pub async fn await_closed(mut self) {
        if let Some(handle) = self.recv_task_handle.take() {
            let _ = handle.await;
        }
    }
}

impl<S, R> Drop for Stream<S, R> {
    fn drop(&mut self) {
        if let Some(handle) = self.send_task_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.recv_task_handle.take() {
            handle.abort();
        }
    }
}

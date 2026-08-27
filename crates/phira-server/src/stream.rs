//! 协议帧层（§6.1）——原版 `phira-mp-common` 的 `Stream` 移植（Apache-2.0，TeamFlos）。
//!
//! 职责（不含心跳判定——那是 core 会话层的生命周期逻辑，阶段 2 接线）：
//! - 版本握手：客户端先发 1 字节版本号（当前 v1），服务端读取；服务端模式写版本
//! - 帧格式：`ULEB128 长度 + 载荷`，载荷以 `u8` 命令 tag 开头（§6.1）
//! - 包上限 2 MiB；长度字段超过 32 bit 拒绝（防攻击，§6.1）
//! - 有界发送队列（1024）+ 后台发送任务（写失败仅记录，不阻塞业务）
//! - 接收任务逐帧解码并交给 handler；解码失败断开（原版语义）
//!
//! 热路径（§6.5-17）：`send` 只入队；编码由发送任务统一做。热路径（Touches/Judges）
//! 经 `Outbound::Encoded` 共享编码结果（ISSUE-0003 方案 2：SessionSink 的 EncodeCache
//! 编码一次、多 monitor 复用）；`Outbound` 实现 `BinaryData`——Encoded 分支直写缓存字节，
//! Stream 的通用编码路径不变。

use std::{
    future::Future,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use phira_api::{
    BinaryData, BinaryReader, BinaryWriter, ProtoResult, ServerCommand, decode_packet,
    encode_packet,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
};
use tracing::{error, trace, warn};

/// 记账释放守卫（安全锁 A）：写任务**任何结束路径**（自然结束 / abort / panic）都释放
/// 剩余 send 队列记账——`abort` 取消 future 时闭包局部变量 drop，guard 生效。
/// 与写任务的实时 `fetch_sub` 幂等（剩余 = 未消费部分，swap 后为 0）。
struct MemoryReleaser(Arc<AtomicUsize>);

impl Drop for MemoryReleaser {
    fn drop(&mut self) {
        let remaining = self.0.swap(0, std::sync::atomic::Ordering::SeqCst);
        if remaining > 0 {
            crate::server::release_memory(remaining);
        }
    }
}

/// 出站消息（ISSUE-0003 方案 2：热路径编码一次共享）。
#[derive(Debug)]
pub enum Outbound {
    /// 未编码命令：写任务负责编码（低频 / 每连接独立：响应、心跳、鉴权、领域事件）。
    Command(ServerCommand),
    /// 已编码载荷（**不含** ULEB128 长度前缀）：热路径共享编码结果，写任务直写。
    Encoded(Arc<Vec<u8>>),
}

impl BinaryData for Outbound {
    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            // 普通命令：走 ServerCommand 的标准协议编码
            Outbound::Command(cmd) => cmd.write_binary(w),
            // 热路径已编码载荷：直写缓存字节（一次编码、多接收者复用，零序列化）
            Outbound::Encoded(bytes) => w.write_raw(bytes),
        }
    }

    fn read_binary(_r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        unreachable!("Outbound 仅服务端发送（服务端模式），不接收")
    }
}

/// 协议版本号（§6.1：客户端发 1 字节，当前 v1）。
pub const PROTOCOL_VERSION: u8 = 1;

/// 单包载荷上限（§6.1：协议上限 2 MiB；鉴权后放开到此值）。
pub const MAX_PACKET_SIZE: u32 = 2 * 1024 * 1024;

/// 鉴权前帧上限（§10.4 红线：握手 + token ≤32B 之外无合法大帧，堵死未鉴权 2MiB 帧攻击）。
pub const PRE_AUTH_MAX_PACKET: u32 = 4 * 1024;

/// 握手超时（§10.4：peek/读首字节 ≤5s——半开连接（connect 后不发版本）不占资源）。
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 写批处理上限（docs/performance-cpu.md §6）：一次 `recv_many` 至多攒这么多帧
/// 合并为一次 `write_all`——Windows IOCP 下每次写 = 一次完成端口唤醒，1500 连接
/// 高频小包场景直接减系统调用量。低流量时 `recv_many` 至少等一帧即返回，延迟不增。
pub const WRITE_BATCH_MAX: usize = 64;

/// 读侧在途记账守卫（安全锁 A 账外区域补洞，docs/performance-cpu.md §6）：
/// payload 读取+解码+分发期间的字节占用计入全局账，Drop 时释放——
/// 任何退出路径（正常 / decode 失败断连 / abort / panic）账目必然平衡。
pub struct ReadCharge(usize);

impl Drop for ReadCharge {
    fn drop(&mut self) {
        crate::server::release_memory(self.0);
    }
}

/// 双向帧流：发送载荷固定为 [`Outbound`]（服务端侧——命令或共享编码帧），
/// `R` = 接收载荷类型（`ClientCommand`）。
///
/// `new` 建立握手 + 启动 send/recv 两个后台任务；`drop` 中止两者（原版语义：
/// 会话结束即断开）。
pub struct Stream<R> {
    /// 协商后的版本号（服务端模式 = 客户端发来的版本）。
    version: u8,

    /// 发送队列发送端（clone 到 handler，供其主动回包）。
    send_tx: Arc<mpsc::Sender<Outbound>>,

    /// `Option`：`await_closed` 取出后置空；Drop 时 abort 仍在的 handle。
    send_task_handle: Option<JoinHandle<()>>,
    recv_task_handle: Option<JoinHandle<Result<()>>>,

    _marker: PhantomData<R>,
}

impl<R> Stream<R>
where
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
    #[allow(clippy::too_many_lines)] // 帧流全生命周期（握手/收发任务/记账守卫）单一函数完整呈现
    pub async fn new<F>(
        version: Option<u8>,
        stream: TcpStream,
        mut handler: Box<dyn FnMut(Arc<mpsc::Sender<Outbound>>, R) -> F + Send + Sync>,
        packet_limit: Arc<AtomicU32>,
        queue_bytes: Arc<AtomicUsize>, // 安全锁 A：本连接 send 队列记账（写任务消费后释放）
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
            // §10.4：半开连接防护——等首字节最多 HANDSHAKE_TIMEOUT，超时即断开
            tokio::time::timeout(HANDSHAKE_TIMEOUT, read.read_u8())
                .await
                .map_err(|_| anyhow::anyhow!("handshake timeout"))??
        };

        let (send_tx, mut send_rx) = mpsc::channel(1024);
        let send_tx = Arc::new(send_tx);
        let send_task_handle = tokio::spawn({
            async move {
                // 安全锁 A：Drop guard——任务结束（含 abort）释放剩余记账（无泄漏）
                let _releaser = MemoryReleaser(Arc::clone(&queue_bytes));
                // 写批处理（实测淬取，docs/performance-cpu.md §6）：Windows IOCP 每次 write
                // 一次完成端口唤醒——1500 连接高频小包下合并帧一次写，系统调用量直接减半量级。
                // `recv_many` 队列空时至少等一帧即返回（低流量下延迟不增）；
                // 批内帧共享一处 output——每帧编出后拼接。
                let mut batch: Vec<Outbound> = Vec::with_capacity(crate::stream::WRITE_BATCH_MAX);
                let mut frame = Vec::new();
                let mut out = Vec::new();
                let mut len_buf = [0u8; 5];
                loop {
                    batch.clear();
                    frame.clear();
                    out.clear();
                    let got = send_rx
                        .recv_many(&mut batch, crate::stream::WRITE_BATCH_MAX)
                        .await;
                    if got == 0 {
                        break; // 通道关闭 = 连接收尾
                    }
                    for payload in batch.drain(..) {
                        frame.clear();
                        match &payload {
                            Outbound::Command(cmd) => {
                                encode_packet(cmd, &mut frame);
                                trace!("sending {} bytes ({cmd:?})", frame.len());
                            }
                            // 热路径共享编码帧（ISSUE-0003 方案 2）：直写缓存字节，零序列化
                            Outbound::Encoded(bytes) => {
                                // 安全锁 A：消费后释放记账（queue_bytes 减 + 全局在途字节减）
                                queue_bytes.fetch_sub(bytes.len(), Ordering::SeqCst);
                                crate::server::release_memory(bytes.len());
                                frame.extend_from_slice(bytes);
                            }
                        }

                        // ULEB128 长度前缀（§6.1）：载荷 ≤ 2 MiB → 最多 3 字节，缓冲 5 够用
                        let mut x = u32::try_from(frame.len()).expect("payload ≤ 2MiB fits u32");
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
                        out.extend_from_slice(&len_buf[..n]);
                        out.extend_from_slice(&frame);
                    }

                    if let Err(err) = write.write_all(&out).await {
                        error!("failed to send: {err:?}");
                    }
                }
                // 安全锁 A：写任务结束（send_rx 关闭 = 连接收尾）——释放剩余记账，
                // 保证"投递 charge 总数 == 释放总数"，连接关闭账目必然平衡（无泄漏）
                let remaining = queue_bytes.swap(0, Ordering::SeqCst);
                if remaining > 0 {
                    crate::server::release_memory(remaining);
                }
            }
        });

        let recv_task_handle = tokio::spawn({
            let send_tx = Arc::clone(&send_tx);
            async move {
                // 读侧合读（实测淬取，docs/performance-cpu.md §6）：一次 `read` 取 4KiB
                // 到一个 pending 缓冲 + 游标消费——小帧多帧/次，长度前缀不再逐字节 `read_u8`
                // （每字节一次 syscall），payload 优先消费 pending、不足才 `read_exact` 补齐。
                // 防垃圾（§6）：pending 上界 = 读缓冲尺寸（固定 4KiB，不随输入增长）；
                // 长度拒收（>32bit / nlen>5 / >packet_limit）与解码失败断连均不变。
                //
                // 读侧在途记账（安全锁 A 账外区域补洞）：payload 读取+解码+分发期间占用的
                // 字节入全局账目——声明 2MiB 帧的恶意洪水跨连接被全局 64MiB 闸住（超限断连
                // fail closed）；`ReadCharge` Drop guard 兜底保证任何退出路径记账平衡。
                let mut buf = [0u8; 4096];
                let mut pending: Vec<u8> = Vec::with_capacity(4096);
                let mut cursor = 0usize;
                let mut payload = Vec::new();
                loop {
                    // —— ULEB128 长度前缀（≤5 字节；pending 优先，不足一次读补齐）——
                    let mut len: u64 = 0;
                    let mut pos = 0;
                    let mut nlen = 0usize;
                    loop {
                        if cursor >= pending.len() {
                            pending.clear();
                            cursor = 0;
                            let n = read.read(&mut buf).await?;
                            if n == 0 {
                                bail!("connection closed mid-frame"); // 半帧挂断（同 read_u8 EOF 语义）
                            }
                            pending.extend_from_slice(&buf[..n]);
                        }
                        let byte = pending[cursor];
                        cursor += 1;
                        nlen += 1;
                        len |= u64::from(byte & 0x7f) << pos;
                        pos += 7;
                        if byte & 0x80 == 0 {
                            break;
                        }
                        // >32 bit（pos 超 32）/ 前缀超 5 字节 → 拒绝（§6.1 防攻击）
                        if pos > 32 || nlen > 5 {
                            bail!("invalid length");
                        }
                    }
                    if len > u64::from(packet_limit.load(Ordering::SeqCst)) {
                        bail!(
                            "data packet too large (limit {})",
                            packet_limit.load(Ordering::SeqCst)
                        );
                    }
                    // 值域已知：len ≤ packet_limit（u32 上限）→ usize/64 位平台无损
                    let len = usize::try_from(len).expect("len ≤ packet_limit (u32) fits usize");

                    // —— payload：pending 剩余优先，不足 read_exact 补齐；记账覆盖整段生命 ——
                    if !crate::server::charge_memory(len) {
                        // 全局在途超限（声明大帧洪水）：fail closed 断连——读侧不能丢新（无重发），
                        // 断连是对攻击者最小成本的处置
                        bail!("read-side memory guard exceeded ({len})");
                    }
                    let _charge = crate::stream::ReadCharge(len);
                    payload.clear();
                    let take = (len - payload.len()).min(pending.len() - cursor);
                    payload.extend_from_slice(&pending[cursor..cursor + take]);
                    cursor += take;
                    if payload.len() < len {
                        payload.resize(len, 0);
                        // pending 已消费 take 字节，余下 len - take 从 socket 补齐
                        read.read_exact(&mut payload[take..]).await?;
                    }
                    trace!("received {} bytes", payload.len());

                    let decoded: R = match decode_packet(&payload) {
                        Ok(val) => val,
                        Err(err) => {
                            warn!("invalid packet: {err:?} {payload:?}");
                            break;
                        }
                    };
                    trace!("decodes to {decoded:?}");
                    handler(Arc::clone(&send_tx), decoded).await;
                    // `_charge` Drop：分发完成后释放读侧在途账（任何退出路径平衡）
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
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// 异步入队一帧（仅入队，编码/发送由后台任务完成；队列满则等待）。
    ///
    /// # Errors
    ///
    /// 对端已断开 / 发送任务已退出 → `mpsc` 发送失败。
    #[allow(dead_code)] // 阶段 2 会话层（事件编码投递 §6.6 表 2）接入后使用
    pub async fn send(&self, payload: Outbound) -> Result<()> {
        self.send_tx.send(payload).await?;
        Ok(())
    }

    /// 阻塞入队一帧（非 async 上下文用，如 Drop 前的最后通知）。
    ///
    /// # Errors
    ///
    /// 对端已断开 → `mpsc` 发送失败。
    #[allow(dead_code)] // 阶段 2 会话层接入后使用
    pub fn blocking_send(&self, payload: Outbound) -> Result<()> {
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

impl<R> Drop for Stream<R> {
    fn drop(&mut self) {
        if let Some(handle) = self.send_task_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.recv_task_handle.take() {
            handle.abort();
        }
    }
}

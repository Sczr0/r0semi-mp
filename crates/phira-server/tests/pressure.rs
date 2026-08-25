#![allow(clippy::needless_continue, clippy::cast_precision_loss)]
//! 高压灌流压力测试（1000Mbps 量级，手动运行 `cargo test -p phira-server --test pressure -- --ignored`）。
//!
//! 威胁模型：CDN/防火墙全炸，攻击者海量连接 × 持续灌随机垃圾（服务端 decode 失败
//! 会断开连接——攻击者自动重连，形成"连接建立/断开风暴"）。
//!
//! 测量：
//! - 吞吐（客户端发送字节总量 / 时间——本地回环接近服务端真实接收）
//! - 连接尝试数（重连风暴速率）
//! - 内存守卫峰值（`in_flight_bytes`——必须 ≤ 64MiB 硬上限）
//! - **无 panic**（JoinHandle 检查，同 fuzz_frames）

use std::sync::Arc;
use std::time::{Duration, Instant};

use phira_api::{
    AuthError, AuthHandler, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    RoomResponse, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection, in_flight_bytes};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

struct NoopFactory;

impl RoomFactory for NoopFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(NoopActor)
    }
}

struct NoopActor;

#[async_trait::async_trait]
impl phira_api::RoomActor for NoopActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        _cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        (None, Vec::new())
    }
}

struct NoopAuth;

#[async_trait::async_trait]
impl AuthHandler for NoopAuth {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Err(AuthError::Business {
            code: phira_api::AuthErrorCode::InvalidToken,
            msg: "no".to_owned(),
        })
    }
}

fn test_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(NoopFactory);
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
    );
    let (task, registry, fact_tx) = LifecycleTask::new(bus.clone(), Duration::from_secs(10));
    tokio::spawn(task.run());
    let sink = Arc::new(SessionSink::new());
    bus.attach_sink(Arc::clone(&sink) as Arc<dyn phira_core::EventSink>);
    Arc::new(ConnContext {
        bus,
        auth: Arc::new(NoopAuth),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
    })
}

/// 确定性伪随机填充（xorshift，不引入 rand 依赖）。
fn fill_random(buf: &mut [u8], seed: u64) {
    let mut x = seed;
    for chunk in buf.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let v = x.to_le_bytes();
        let n = chunk.len();
        chunk.copy_from_slice(&v[..n]);
    }
}

/// 单个灌流 worker：循环连接 → 握手 → 猛灌随机字节 → 被断/超时后重连。
/// 返回 (发送总字节, 连接尝试次数)。
async fn flood_worker(
    addr: std::net::SocketAddr,
    seed: u64,
    duration: Duration,
    stats: Arc<Mutex<(u64, u64)>>,
) {
    let deadline = Instant::now() + duration;
    let mut buf = vec![0u8; 256 * 1024];
    let mut attempts = 0u64;
    let mut sent = 0u64;
    let mut s = seed;
    while Instant::now() < deadline {
        attempts += 1;
        match TcpStream::connect(addr).await {
            Ok(mut client) => {
                if client.write_all(&[PROTOCOL_VERSION]).await.is_err() {
                    continue;
                }
                // 猛灌：大块随机字节（连发，不断重连）
                while Instant::now() < deadline {
                    s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    fill_random(&mut buf, s);
                    match client.write_all(&buf).await {
                        Ok(()) => sent += buf.len() as u64,
                        Err(_) => break, // 服务端断开 → 重连
                    }
                }
                let _ = &mut s;
                let _ = &s;
            }
            Err(_) => continue,
        }
    }
    let mut st = stats.lock().await;
    st.0 += sent;
    st.1 += attempts;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "压力测试：手动运行（~10s，高带宽）——cargo test -p phira-server --test pressure -- --ignored"]
async fn flood_1000mbps_equivalent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handles: Arc<Mutex<Vec<tokio::task::JoinHandle<anyhow::Result<()>>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let handles2 = Arc::clone(&handles);
    let ctx = test_ctx();
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            let handles = Arc::clone(&handles2);
            let jh = tokio::spawn(async move { handle_connection(stream, addr, ctx).await });
            handles.lock().await.push(jh);
        }
    });

    // 并发灌流：32 个 worker × 持续 8 秒（本地回环，单 worker 可发 ~40MB/s）
    let duration = Duration::from_secs(8);
    let stats = Arc::new(Mutex::new((0u64, 0u64)));
    let mut workers = Vec::new();
    for i in 0..32 {
        let stats = Arc::clone(&stats);
        workers.push(tokio::spawn(flood_worker(
            addr,
            0xABCD_0000 + i,
            duration,
            stats,
        )));
    }
    // 监控：内存守卫峰值
    let monitor = tokio::spawn(async move {
        let end = Instant::now() + duration;
        let mut peak = 0usize;
        while Instant::now() < end {
            let v = in_flight_bytes();
            if v > peak {
                peak = v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        peak
    });

    for w in workers {
        w.await.unwrap();
    }
    let (sent_bytes, attempts) = *stats.lock().await;
    let mem_peak = monitor.await.unwrap();

    // 等所有连接收尾，检查 panic
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut jhs = handles.lock().await;
    let mut panics = Vec::new();
    for jh in jhs.drain(..) {
        if let Err(e) = jh.await {
            panics.push(e.to_string());
        }
    }

    let mbps = f64::from(u32::try_from(sent_bytes).unwrap_or(u32::MAX))
        / 1_000_000.0
        / duration.as_secs_f64()
        * 8.0;
    println!(
        "== 压力结果 ==\n总发送: {:.2} MB ({mbps:.0} Mbps 等效)\n连接尝试: {attempts}\n内存守卫峰值: {:.1} MiB\npanic: {}",
        f64::from(u32::try_from(sent_bytes).unwrap_or(u32::MAX)) / 1_000_000.0,
        f64::from(u32::try_from(mem_peak).unwrap_or(u32::MAX)) / 1_048_576.0,
        panics.len()
    );

    assert!(panics.is_empty(), "高压灌流下服务端 panic: {panics:?}");
    assert!(
        mem_peak <= 64 * 1024 * 1024,
        "内存守卫峰值 {:.1} MiB 超硬上限 64 MiB",
        f64::from(u32::try_from(mem_peak).unwrap_or(u32::MAX)) / 1_048_576.0
    );
    assert!(mbps >= 100.0, "本地回环吞吐异常偏低: {mbps:.0} Mbps");
}

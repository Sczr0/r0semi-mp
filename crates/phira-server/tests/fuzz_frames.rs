//! 帧层模糊测试（文档 §9 模糊层——网络入口威胁模型）。
//!
//! **场景**：黑客脚本往端口无脑灌垃圾字节流。验证服务端在随机/畸形帧流下
//! **进程不 panic**（连接被断/忽略是正常防御，panic = 服务器下线）。
//!
//! 真实 TCP 驱动 `handle_connection`，**JoinHandle 外层 Err 检测 panic**——
//! 区分"业务断开（内层 Err，正常）"与"task panic（外层 JoinError，失败）"。
//!
//! 灌入形态：
//! 1. 纯随机字节（握手后当帧解析）
//! 2. 半帧（ULEB128 长度声明大、只发部分——read_exact 挂起后客户端关闭）
//! 3. 超长帧（长度 > 帧上限 → 应断开）
//! 4. ULEB128 不终止（0x80... 连发 → 超 32 bit 应拒绝）

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    RoomResponse, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// 确定性伪随机（xorshift64——不引入 rand 依赖，可复现）。
struct Lcg(u64);

impl Lcg {
    const fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&v[..n]);
        }
    }
}

// —— 测试上下文（复制 frames.rs 模式：NoopFactory + 拒绝鉴权，帧层无需鉴权成功）——

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
        proxy_protocol: false,
    })
}

/// 起服务器，返回 (监听地址, 所有连接的 JoinHandle——用于 panic 检测)。
async fn spawn_server() -> (
    std::net::SocketAddr,
    Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<anyhow::Result<()>>>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handles: Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<anyhow::Result<()>>>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
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
    (addr, handles)
}

/// 灌一轮垃圾并等待连接结束；返回本连接的 JoinHandle 是否 panic。
async fn flood_one(addr: std::net::SocketAddr, seed: u64, len: usize, half_frame: bool) {
    let mut client = TcpStream::connect(addr).await.unwrap();
    // 握手：版本字节（任何字节服务端接受，§6.1）
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    let mut rng = Lcg(seed);

    if half_frame {
        // 半帧：ULEB128 声明巨大长度（0xFF 连发 = 高位继续位），只发 1 字节载荷
        // 服务端应挂起 read_exact 等剩余 → 客户端关闭 → Err 断开（不 panic）
        client
            .write_all(&[0xff, 0xff, 0xff, 0xff, 0xff])
            .await
            .unwrap();
        client.write_all(&[0x00]).await.unwrap();
    } else if len > 8 * 1024 * 1024 {
        // 超长帧：长度前缀声明超大 → 帧上限拒绝（4KiB 鉴权前）
        client
            .write_all(&[0xff, 0xff, 0xff, 0xff, 0x0f])
            .await
            .unwrap();
    } else {
        // 纯随机 / 随机长度随机字节
        let mut buf = vec![0u8; len];
        rng.fill(&mut buf);
        client.write_all(&buf).await.unwrap();
    }

    // 给服务端处理时间；随后关闭（半帧挂起依赖客户端关闭才断）
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(client);
}

#[tokio::test]
async fn random_flood_never_panics_server() {
    let (addr, handles) = spawn_server().await;

    // 多轮：不同种子 × 不同长度（含空、1 字节、帧大小、大随机）
    let mut seeds: Vec<u64> = (0..40).collect();
    let lens: Vec<usize> = vec![0, 1, 2, 16, 255, 1024, 4095, 4096, 4097, 1 << 16, 1 << 20];
    for (idx, seed) in seeds.drain(..).enumerate() {
        let len = lens[idx % lens.len()];
        flood_one(addr, seed, len, false).await;
    }
    // 半帧 + 超长帧专项
    flood_one(addr, 0xDEAD, 0, true).await;
    flood_one(addr, 0xBEEF, 9 * 1024 * 1024, false).await;

    // 等所有连接结束，检查 panic（外层 JoinError = task panic，失败）
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut jhs = handles.lock().await;
    let mut panics = Vec::new();
    for jh in jhs.drain(..) {
        match jh.await {
            // 内层 Result 的 Ok/Err 都是正常防御（断开/业务错误）；外层 Err 才是 panic
            Ok(_) => {}
            Err(e) => panics.push(e.to_string()),
        }
    }
    assert!(
        panics.is_empty(),
        "服务端 task 在垃圾流下 panic（{} 处）: {panics:?}",
        panics.len()
    );
}

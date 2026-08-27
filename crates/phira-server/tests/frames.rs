//! 协议帧层集成测试（阶段 1 验收，§14：帧 + 心跳链路）。
//!
//! 真实 TCP 连接驱动 `handle_connection`：
//! 1. 握手：客户端先发版本字节（§6.1），服务端读取
//! 2. Ping → Pong（心跳应答，§6.1：服务端不发 Ping 只回 Pong）
//! 3. 多帧连续收发（帧边界正确性）
//! 4. 服务端主动推送（阶段 2 广播路径的前提）
//! 5. 恶意帧拒绝（超长 / 非法 tag → 断开）
//!
//! 客户端用原始 socket + 手写读帧，不依赖 `Stream` 客户端 API——测线上字节行为
//! （Oracle 第二形态：不经过测试对象自身的便捷层）。

use std::{sync::Arc, time::Duration};

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory,
    RoomId, RoomResponse, ServerCommand, UserIdentity, decode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 最小测试上下文：无操作工厂 + 拒绝鉴权（心跳测试不鉴权，只验证帧层）。
fn test_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(NoopFactory);
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
    );
    let (task, registry, fact_tx) = LifecycleTask::new(
        bus.clone(),
        Duration::from_secs(10),
        Duration::from_millis(50),
    );
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
        admin_token: None,
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
    })
}

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
            msg: "noop auth".to_owned(),
        })
    }
}

/// 便捷：编码一帧（ULEB128 长度前缀 + 载荷，§6.1）。
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut x = payload.len() as u64;
    loop {
        let mut b = (x & 0x7f) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }
        out.push(b);
        if x == 0 {
            break;
        }
    }
    out.extend_from_slice(payload);
    out
}

/// 建立一对连接：服务端跑 `handle_connection`，返回已握手的原始客户端 socket。
async fn connect_pair() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, test_ctx()).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    // 客户端握手：先发 1 字节版本（§6.1）
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
}

/// 读一帧：ULEB128 长度 + 载荷，返回载荷字节。
async fn read_frame(sock: &mut TcpStream) -> Vec<u8> {
    let mut len = 0u64;
    let mut pos = 0;
    loop {
        let byte = sock.read_u8().await.unwrap();
        len |= u64::from(byte & 0x7f) << pos;
        pos += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    let mut payload = vec![0u8; usize::try_from(len).expect("test: frame len fits usize")];
    sock.read_exact(&mut payload).await.unwrap();
    payload
}

#[tokio::test]
async fn handshake_ping_pong() {
    let mut client = connect_pair().await;

    // Ping（golden 字节：tag 0）→ 服务端回 Pong（tag 0）
    client.write_all(&frame(&[0x00])).await.unwrap();
    let payload = read_frame(&mut client).await;
    assert_eq!(payload, vec![0x00], "Pong 载荷 = [tag 0]");
}

#[tokio::test]
async fn multiple_frames_order() {
    let mut client = connect_pair().await;

    for _ in 0..5 {
        client.write_all(&frame(&[0x00])).await.unwrap();
    }
    for _ in 0..5 {
        let payload = read_frame(&mut client).await;
        assert_eq!(payload, vec![0x00], "连续 Pong 顺序一致");
    }
}

#[tokio::test]
async fn pre_auth_non_ping_ignored() {
    // §6.5-13：鉴权前收到非 Ping/Authenticate 包 → 忽略（不回复、不打断连接）
    // Chat = [tag 2, uleb(2), 'h','i']
    let mut client = connect_pair().await;
    client
        .write_all(&frame(&[0x02, 0x02, 0x68, 0x69]))
        .await
        .unwrap();
    // 随后 Ping 仍被应答 → 连接未被破坏
    client.write_all(&frame(&[0x00])).await.unwrap();
    let payload = read_frame(&mut client).await;
    assert_eq!(payload, vec![0x00], "鉴权前非 Ping 忽略，Ping 仍被应答");
}

#[tokio::test]
async fn auth_failure_responds_and_stops() {
    // 鉴权（NoopAuth 拒绝）→ Authenticate(Err) 响应；随后帧被忽略（panicked）
    let mut client = connect_pair().await;
    client
        .write_all(&frame(&[0x01, 0x02, 0x61, 0x62]))
        .await
        .unwrap();
    let payload = read_frame(&mut client).await;
    let resp: ServerCommand = decode_packet(&payload).unwrap();
    assert!(
        matches!(resp, ServerCommand::Authenticate(Err(_))),
        "NoopAuth 拒绝应回 Authenticate(Err): {resp:?}"
    );
    // panicked：后续 Ping 无响应
    client.write_all(&frame(&[0x00])).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(200), read_frame(&mut client)).await;
    assert!(r.is_err(), "鉴权失败后不应再有响应");
}

/// 服务端主动推送路径（阶段 2 广播的前提；真实服务端 Stream 的 `send`）。
#[tokio::test]
async fn server_initiated_push() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let handler = Box::new(
            move |tx: Arc<tokio::sync::mpsc::Sender<phira_server::stream::Outbound>>,
                  cmd: ClientCommand| async move {
                if let ClientCommand::Ping = cmd {
                    // 主动推送 + 心跳应答（顺序 = 发送顺序）
                    tx.send(phira_server::stream::Outbound::Command(
                        ServerCommand::Chat(Err("stage-2-not-wired".to_owned())),
                    ))
                    .await
                    .unwrap();
                    tx.send(phira_server::stream::Outbound::Command(ServerCommand::Pong))
                        .await
                        .unwrap();
                }
            },
        );
        let stream = phira_server::stream::Stream::<ClientCommand>::new(
            None,
            stream,
            handler,
            std::sync::Arc::new(std::sync::atomic::AtomicU32::new(
                phira_server::stream::MAX_PACKET_SIZE,
            )),
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)), // 记账 dummy（客户端模式）
        )
        .await
        .unwrap();
        stream.await_closed().await;
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client.write_all(&frame(&[0x00])).await.unwrap(); // Ping

    // 先收主动推送（Chat Err），再收 Pong
    let chat_payload = read_frame(&mut client).await;
    let chat: ServerCommand = decode_packet(&chat_payload).unwrap();
    assert!(matches!(chat, ServerCommand::Chat(Err(_))));
    let pong_payload = read_frame(&mut client).await;
    let pong: ServerCommand = decode_packet(&pong_payload).unwrap();
    assert!(matches!(pong, ServerCommand::Pong));

    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
}

/// 恶意帧：帧长 > 2MiB → 服务端拒绝并断开（§6.1 协议上限）。
#[tokio::test]
async fn oversized_frame_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();

    // 帧长 = 2MiB + 1（ULEB128：2097153 = 0x81 0x80 0x80 0x01）
    sock.write_all(&[0x81, 0x80, 0x80, 0x01]).await.unwrap();

    let mut buf = [0u8; 1];
    let r = sock.read(&mut buf).await;
    assert_eq!(r.unwrap(), 0, "服务端应拒绝超长帧并断开");
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

/// §10.4 红线：鉴权前帧上限收紧 ~4KiB——4KiB+1 的帧即断开（堵死未鉴权 2MiB 帧攻击）。
#[tokio::test]
async fn pre_auth_4k_frame_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();

    // 帧长 = 4KiB + 1 = 4097（ULEB128：0x81 0x20）
    sock.write_all(&[0x81, 0x20]).await.unwrap();

    let mut buf = [0u8; 1];
    let r = sock.read(&mut buf).await;
    assert_eq!(r.unwrap(), 0, "鉴权前 >4KiB 帧应被拒绝并断开");
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

/// 握手版本校验（D2）：非 v1 版本握手后应立即断开——不再宽容任意版本（§6.1）。
///
/// 原语义「版本只读不校验」已被技术债 D2 推翻：不匹配的客户端（旧/新）拒绝比
/// 容忍安全，避免旧客户端发 v2 帧被误解析。
#[tokio::test]
async fn handshake_rejects_wrong_version_then_accepts_v1() {
    // 用例 1：非法版本（0x63 ≠ PROTOCOL_VERSION=1）→ 握手即断开，Ping 无应答。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[0x63]).await.unwrap(); // 非 v1 版本字节
    sock.write_all(&frame(&[0x00])).await.unwrap();
    // 服务端校验拒绝 → 关闭连接：读首字节应为 EOF（0）或错误，而非超时等待。
    let answered = tokio::time::timeout(Duration::from_millis(300), sock.read_u8()).await;
    // 超时（挂死没断）= 服务端未按 D2 拒绝 → 失败
    let Ok(byte) = answered else {
        panic!("非法版本握手后连接未被服务端断开");
    };
    match byte {
        Ok(0) => {}
        // 读到数据 = 服务端仍响应（未拒绝）→ 失败
        Ok(_) => panic!("非法版本握手后不应收到数据"),
        // IO 错误（连接 reset 等）= 连接已关（拒绝成功）
        Err(e) => {
            assert!(
                e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset,
                "非法版本握手后连接关闭异常: {e}"
            );
        }
    }
    drop(sock);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;

    // 用例 2：合法版本（PROTOCOL_VERSION=1）→ 握手成功，Ping → Pong 正常。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    sock.write_all(&frame(&[0x00])).await.unwrap();
    let payload = read_frame(&mut sock).await;
    assert_eq!(payload, vec![0x00], "合法版本握手后 Ping → Pong");
    drop(sock);
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

/// 非法包（未知命令 tag）→ 服务端断开（原版语义：解码失败 break）。
#[tokio::test]
async fn invalid_packet_disconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    sock.write_all(&frame(&[0xFF])).await.unwrap(); // tag 0xFF = 未知命令

    let mut buf = [0u8; 1];
    let r = sock.read(&mut buf).await;
    assert_eq!(r.unwrap(), 0, "未知命令应导致断开");
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

/// §6.5-20 / §6.1：10s 无任何包 → 心跳判定断线 → 服务器主动断开。
#[tokio::test]
async fn heartbeat_timeout_disconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap(); // 握手
    // 之后不发任何包

    let start = std::time::Instant::now();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(12), sock.read(&mut buf)).await;
    assert_eq!(r.unwrap().unwrap(), 0, "10s 无包应被服务器主动断开（EOF）");
    assert!(
        start.elapsed() >= Duration::from_secs(9),
        "断开应在心跳超时（~10s）之后: {:?}",
        start.elapsed()
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

// 注：SIGTERM 优雅停机路径未做自动化测试——给测试进程发 SIGTERM 会终止整个
// cargo test harness（2026-08 CI 实测 signal 15 杀进程）。其逻辑（维护广播 + grace）
// 由 e2e maintenance_broadcast_reaches_all + 部署实测（systemd SIGTERM）覆盖。

/// §10.4：半开连接防护——connect 后不发版本字节 → 5s 握手超时 → 服务器断开。
#[tokio::test]
async fn handshake_timeout_disconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    // 不发握手版本字节

    let start = std::time::Instant::now();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(8), sock.read(&mut buf)).await;
    assert_eq!(r.unwrap().unwrap(), 0, "5s 握手超时应被服务器断开（EOF）");
    assert!(
        start.elapsed() >= Duration::from_secs(4),
        "断开应在 ~5s 后: {:?}",
        start.elapsed()
    );
    let _ = tokio::time::timeout(Duration::from_secs(2), server_done).await;
}

/// §10.4：每 IP 未鉴权连接上限 5——第 6 个同 IP 连接被拒；释放后可再连。
#[tokio::test]
async fn per_ip_admission_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = test_ctx();
    tokio::spawn(async move {
        loop {
            let (stream, a) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, a, ctx).await;
            });
        }
    });

    // 5 个同 IP 未鉴权连接（不发握手 → 计入 pending）
    let mut socks: Vec<TcpStream> = Vec::new();
    for _ in 0..5 {
        socks.push(TcpStream::connect(addr).await.unwrap());
    }

    // 第 6 个：被服务器拒绝（try_acquire 失败 → drop → EOF）
    let mut sixth = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 1];
    let r = tokio::time::timeout(Duration::from_secs(3), sixth.read(&mut buf)).await;
    assert_eq!(r.unwrap().unwrap(), 0, "第 6 个同 IP 未鉴权连接应被拒绝");

    // 释放一个连接（pending-1）→ 新连接可被接受
    drop(socks.pop());
    tokio::time::sleep(Duration::from_millis(200)).await; // 等服务端收尾
    let mut seventh = TcpStream::connect(addr).await.unwrap();
    let r = tokio::time::timeout(Duration::from_millis(400), seventh.read(&mut buf)).await;
    assert!(
        r.is_err() || r.unwrap().unwrap() != 0,
        "释放后新连接应被接受（服务器在等握手而非立即断开）"
    );

    drop(socks);
    drop(sixth);
    drop(seventh);
}

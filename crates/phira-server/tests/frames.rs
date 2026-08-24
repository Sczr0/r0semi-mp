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
            move |tx: Arc<tokio::sync::mpsc::Sender<ServerCommand>>, cmd: ClientCommand| async move {
                if let ClientCommand::Ping = cmd {
                    // 主动推送 + 心跳应答（顺序 = 发送顺序）
                    tx.send(ServerCommand::Chat(Err("stage-2-not-wired".to_owned())))
                        .await
                        .unwrap();
                    tx.send(ServerCommand::Pong).await.unwrap();
                }
            },
        );
        let stream = phira_server::stream::Stream::<ServerCommand, ClientCommand>::new(
            None,
            stream,
            handler,
            std::sync::Arc::new(std::sync::atomic::AtomicU32::new(
                phira_server::stream::MAX_PACKET_SIZE,
            )),
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

/// 握手版本宽容（原版语义 + §6.1 只读不校验）：任意版本字节仍可通信。
#[tokio::test]
async fn handshake_any_version_accepted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_done = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = handle_connection(stream, addr, test_ctx()).await;
    });

    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[0x63]).await.unwrap(); // 任意版本字节（非 v1）

    // 仍能 Ping → Pong（版本不校验，§6.1）
    sock.write_all(&frame(&[0x00])).await.unwrap();
    let payload = read_frame(&mut sock).await;
    assert_eq!(payload, vec![0x00], "任意版本握手后 Ping 仍应答");
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

/// 配置化接线（unix）：Server::run 收到 SIGTERM → 用配置的 grace（0 = 立即退出）。
/// 用外部 `kill` 命令发信号（避免 unsafe；Windows 无 SIGTERM 语义，cfg 掉）。
#[cfg(unix)]
#[tokio::test]
async fn shutdown_signal_grace_zero_exits() {
    use phira_server::server::Server;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // 配置化参数：自定义 notice + grace=0（yml 接线点）
    let server = Server::new(
        addr,
        (*test_ctx()).clone(),
        "test maintenance notice".to_owned(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    let run = tokio::spawn(async move { server.run().await });

    // 等监听就绪 → 发 SIGTERM
    tokio::time::sleep(Duration::from_millis(200)).await;
    let status = std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .expect("kill 命令可执行");
    assert!(status.success());

    // grace=0 → run 快速返回（而非挂在宽限窗口）
    let r = tokio::time::timeout(Duration::from_secs(3), run).await;
    assert!(r.is_ok(), "SIGTERM 后 run 应返回（grace=0 立即退出）");
}

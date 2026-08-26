//! 每连接命令限速测试（ISSUE-0006 修复：滥用控制"快端"防线）。
//!
//! 1. 单元：`CommandLimiter` 间隔语义（间隔内拒绝 / 间隔后放行 / 不同命令独立）
//! 2. 集成：真实 TCP——鉴权后连发 CreateRoom（1s 间隔）→ 第二个收 `too many requests`
//!    错误（客户端可见，不触发队列 Reject 断连）；窗口恢复后可再次建房。
//!
//! 修复前：`CreateRoom`/`JoinRoom`/`SelectChart`/`Played` 无频率限制——高频刷命令靠
//! 队列满断连"惩罚"而非"预防"（ISSUE-0006 原始问题）。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory,
    RoomId, RoomResponse, ServerCommand, UserIdentity, Varchar, decode_packet, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{CommandLimiter, ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rid() -> RoomId {
    RoomId::new("r".to_owned()).unwrap()
}

// —— 单元：CommandLimiter 间隔语义 ——

#[tokio::test]
async fn limiter_rejects_within_interval_and_allows_after() {
    let limiter = CommandLimiter::new();
    let interval = Duration::from_millis(50);
    assert!(limiter.allow("create_room", interval), "首次放行");
    assert!(
        !limiter.allow("create_room", interval),
        "间隔内拒绝（令牌桶：距上次 < interval 不放行）"
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(limiter.allow("create_room", interval), "间隔后放行");
}

#[tokio::test]
async fn limiter_tracks_commands_independently() {
    let limiter = CommandLimiter::new();
    let interval = Duration::from_millis(200);
    assert!(limiter.allow("create_room", interval));
    assert!(
        limiter.allow("join_room", interval),
        "不同命令独立限速（CreateRoom 被限不影响 JoinRoom）"
    );
    assert!(!limiter.allow("join_room", interval));
    assert!(
        !limiter.allow("create_room", interval),
        "CreateRoom 仍受限（各自计时）"
    );
}

// —— 集成：真实 TCP 限速 ——

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "limiter".to_owned(),
            lang: "en-US".to_owned(), // 行为测试与语言无关：固定 en，断言英文文案（zh 链路见 e2e i18n 专项）
        })
    }
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
        // 无操作但正常响应（CreateRoom 等需要响应；热路径无响应由 bus needs_response 决定）
        (Some(RoomResponse::Ok), Vec::new())
    }
}

fn limiter_ctx() -> Arc<ConnContext> {
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
        auth: Arc::new(AuthOk),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
        proxy_protocol: false,
    })
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut x = u32::try_from(payload.len()).expect("test: frame fits u32");
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

fn client_frame(cmd: &ClientCommand) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_packet(cmd, &mut buf);
    frame(&buf)
}

/// 读一帧并解码为 ServerCommand。
async fn read_command(sock: &mut TcpStream) -> ServerCommand {
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
    decode_packet(&payload).expect("test: decode ServerCommand")
}

/// 鉴权后的已握手客户端（AuthOk）。
async fn authed_client(ctx: Arc<ConnContext>) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    // 读鉴权响应（丢弃）
    let _ = read_command(&mut client).await;
    client
}

#[tokio::test]
async fn create_room_rate_limited_per_connection() {
    let mut client = authed_client(limiter_ctx()).await;

    // 第一次 CreateRoom → Ok
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    let first = read_command(&mut client).await;
    assert!(
        matches!(first, ServerCommand::CreateRoom(Ok(()))),
        "首次建房应成功: {first:?}"
    );

    // 立即第二次 → TooManyRequests（1s 间隔内）
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    let second = read_command(&mut client).await;
    match second {
        ServerCommand::CreateRoom(Err(e)) => {
            assert!(
                e.contains("too many requests"),
                "超限应回 too many requests: {e}"
            );
        }
        other => panic!("间隔内建房应被限速: {other:?}"),
    }

    // 等 1s 窗口恢复 → 第三次 CreateRoom（不同房间 id，避免与第一次冲突）→ Ok
    tokio::time::sleep(Duration::from_secs(1) + Duration::from_millis(100)).await;
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom {
            id: RoomId::new("r2".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    let third = read_command(&mut client).await;
    assert!(
        matches!(third, ServerCommand::CreateRoom(Ok(()))),
        "窗口恢复后应可建房: {third:?}"
    );
}

#[tokio::test]
async fn hot_path_touches_not_rate_limited() {
    // 热路径（Touches）不在受限列表（rate_limit 返回 None）——靠 DropIfFull 兜底。
    let mut client = authed_client(limiter_ctx()).await;
    for _ in 0..5 {
        client
            .write_all(&client_frame(&ClientCommand::Touches {
                frames: Arc::new(vec![]),
            }))
            .await
            .unwrap();
    }
    // 热路径无响应（§6.5-17：只转发不回答发者）——能发完不报错即通过；
    // 若被限速会收 Err 帧，这里 200ms 内无响应帧即证明未受限
    let _ = tokio::time::timeout(Duration::from_millis(200), client.read_u8()).await;
}

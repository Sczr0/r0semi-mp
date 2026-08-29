//! ISSUE-0009 回归测试：重连/顶替后，旧 TCP 到达的命令以 epoch 校验拒绝（§4.9-3 旧连接失效）。
//!
//! 场景：连接 A 鉴权（epoch 1）→ 建房 → 连接 B 同用户再次鉴权（epoch 2 顶替，A 成为旧连接）→
//! A 发 Chat 应被拒绝（B 收不到广播、A 被 kicker 拆线）；B 发 Chat 正常广播回自己。
//!
//! 修复前：A 的命令直通派发（同 id 双活命令混进房间 channel，顺序语义未定义交织）；
//! 修复后：`handle_frame` 派发前校验 `current_epoch(user_id) == state.epoch`，不匹配拒绝 + force_close。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    ApiClient, ApiError, AuthError, AuthHandler, Chart, ClientCommand, RandomSource, Record,
    RoomConfig, RoomDeps, RoomFactory, RoomId, ServerCommand, UserIdentity, Varchar,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rid() -> RoomId {
    RoomId::new("stale".to_owned()).unwrap()
}

/// 任意 token 都鉴权为 user 1（两连接同用户——重连/顶替场景）。
struct AuthUser1;

#[async_trait::async_trait]
impl AuthHandler for AuthUser1 {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "stale-user".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

/// 真实房间实现需要的回源 API 桩（本测试不触达 /chart /record）。
struct NeverApi;

#[async_trait::async_trait]
impl ApiClient for NeverApi {
    async fn fetch_chart(&self, _id: i32) -> Result<Chart, ApiError> {
        Err(ApiError::Internal {
            msg: "unused".to_owned(),
        })
    }
    async fn fetch_record(&self, _id: i32) -> Result<Record, ApiError> {
        Err(ApiError::Internal {
            msg: "unused".to_owned(),
        })
    }
}

/// 确定性随机源（本测试不触达房主迁移；len==0 按契约返回 None）。
struct ZeroRng;

impl RandomSource for ZeroRng {
    fn pick_index(&self, len: usize) -> Option<usize> {
        if len == 0 { None } else { Some(0) }
    }
}

fn test_ctx() -> Arc<ConnContext> {
    test_ctx_with_auth_timeout(Duration::from_secs(10))
}

/// 可注入鉴权阶段超时（C-01 回归测试用短值加速）。
fn test_ctx_with_auth_timeout(auth_timeout: Duration) -> Arc<ConnContext> {
    let deps = RoomDeps {
        api: Arc::new(NeverApi),
        rng: Arc::new(ZeroRng),
    };
    let rooms = impl_rooms_v1::RoomsV1::new(RoomConfig::default(), deps);
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig::default()),
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
        auth: Arc::new(AuthUser1),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
        proxy_protocol: false,
        auth_timeout,
        admin_token: None,
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(phira_server::storage::ConfigStore::disabled()),
    })
}

/// 起一个服务器连接（已握手 + 已鉴权，鉴权响应已消费）。
async fn authed_client(ctx: Arc<ConnContext>) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    send_cmd(
        &mut client,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut client).await; // 丢弃鉴权响应（成功）
    client
}

/// 发送一帧（ULEB128 长度 + 载荷，§6.1）。
async fn send_cmd(sock: &mut TcpStream, cmd: &ClientCommand) {
    let mut payload = Vec::new();
    phira_api::encode_packet(cmd, &mut payload);
    let mut frame = Vec::new();
    let mut x = payload.len() as u64;
    loop {
        let mut b = (x & 0x7f) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }
        frame.push(b);
        if x == 0 {
            break;
        }
    }
    frame.extend_from_slice(&payload);
    sock.write_all(&frame).await.unwrap();
}

/// 接收一帧并解码（2s 超时防挂）。
async fn recv_cmd(sock: &mut TcpStream) -> ServerCommand {
    tokio::time::timeout(Duration::from_secs(2), async {
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
        let mut payload = vec![0u8; usize::try_from(len).unwrap()];
        sock.read_exact(&mut payload).await.unwrap();
        phira_api::decode_packet(&payload).unwrap()
    })
    .await
    .expect("recv_cmd timeout: 帧流错位或服务器无响应")
}

#[tokio::test]
async fn stale_connection_commands_rejected_after_reconnect() {
    let ctx = test_ctx();

    // 连接 A：鉴权（epoch 1）→ 建房成功
    let mut a = authed_client(ctx.clone()).await;
    send_cmd(&mut a, &ClientCommand::CreateRoom { id: rid() }).await;
    // 广播先于响应（原版：room.send 广播后返回 Ok）
    let broadcast = recv_cmd(&mut a).await;
    assert!(
        matches!(
            broadcast,
            ServerCommand::Message(phira_api::Message::CreateRoom { user: 1 })
        ),
        "应收到 CreateRoom 广播: {broadcast:?}"
    );
    assert!(
        matches!(recv_cmd(&mut a).await, ServerCommand::CreateRoom(Ok(()))),
        "连接 A 建房应成功"
    );

    // 连接 B：同用户二次鉴权 → epoch 2 顶替（A 成为旧连接，其投递槽位已归 B）
    let mut b = authed_client(ctx.clone()).await;

    // A 发 Chat —— 应被 epoch 校验拒绝：B 收不到任何广播（A 的身份已失效）
    send_cmd(
        &mut a,
        &ClientCommand::Chat {
            message: Varchar::new("stale".to_owned()).unwrap(),
        },
    )
    .await;
    let got = tokio::time::timeout(Duration::from_millis(300), recv_cmd(&mut b)).await;
    assert!(
        got.is_err(),
        "旧连接的 Chat 不应投递给新连接（epoch 校验拒绝），实际收到 {got:?}"
    );

    // B 发 Chat —— 正常广播回 B 自己（B 仍是活连接，未被 A 的死亡事实误伤）
    send_cmd(
        &mut b,
        &ClientCommand::Chat {
            message: Varchar::new("live".to_owned()).unwrap(),
        },
    )
    .await;
    let got = tokio::time::timeout(Duration::from_secs(1), recv_cmd(&mut b)).await;
    assert!(
        matches!(
            got,
            Ok(ServerCommand::Message(phira_api::Message::Chat { ref content, .. }))
                if content == "live"
        ),
        "活连接的 Chat 应广播回自己，实际 {got:?}"
    );

    // A 被 kicker（force_close，1s 轮询）拆线：连接应关闭（释放已鉴权名额）
    let closed = tokio::time::timeout(Duration::from_secs(4), async {
        let mut buf = [0u8; 1024];
        loop {
            match a.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "旧连接应在 4s 内被 kicker 拆线（force_close）"
    );
}

/// C-01 回归测试：未鉴权挂起连接到期断开。
///
/// 场景：客户端发版本字节后**不鉴权**，只持续发 Ping。心跳只看收包（§6.1）——
/// 持续 Ping 保持 last_recv 新鲜，若无 `auth_timeout`，未鉴权连接可无限挂占
/// 准入额度（§10.4）。本测试钉死：版本确认后到鉴权完成之间的硬上限
/// （`auth_deadline`，yml `auth_timeout`）到期必须断开。
#[tokio::test]
async fn unauthenticated_hung_connection_expires_at_auth_timeout() {
    let ctx = test_ctx_with_auth_timeout(Duration::from_millis(400));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    // 不鉴权，仅持续 Ping 保持 last_recv 新鲜
    let closed = tokio::time::timeout(Duration::from_secs(3), async {
        let mut buf = [0u8; 64];
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(30)) => {
                    send_cmd(&mut client, &ClientCommand::Ping).await;
                }
                r = client.read(&mut buf) => {
                    match r {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "未鉴权挂起连接应在 auth_timeout(400ms) 到期被断开（持续 Ping 保鲜不应阻止）"
    );
}

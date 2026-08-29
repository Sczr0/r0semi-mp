//! 反作弊观察者（P2，AdminKick 之后第二个真实 Moderator）集成测试：
//! 跨房 record 重放检测——同一 (user, record) 已在其他房间结算过再上报 → Moderated。
//!
//! 关键：intercept 在**路由之前**（§7.3），与房间状态机无关——无需完整开局即可
//! 验证"首次放行 + 跨房拒绝 + 同房重放不拦"三态。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, Moderator, Origin, RoomCommand, RoomConfig,
    RoomDeps, RoomId, ServerCommand, UserIdentity, Varchar,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::http::{HttpApiClient, ThreadRngSource};
use phira_server::server::{AntiCheatObserver, ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, AuthError> {
        let id: i32 = token
            .strip_prefix("tok")
            .and_then(|t| t.parse().ok())
            .unwrap_or(1);
        Ok(UserIdentity {
            user_id: id,
            name: "p".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

/// mock API（仅 /me；record 回源命中失败也无妨——反作弊拦截在回源之前）。
async fn mock_api(addr: std::net::SocketAddr) {
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (mut sock, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = sock.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&head);
            let body = if text.contains("GET /record/7 ") {
                // A2 回源：合法 record（player=1 → 结算成功，房间不关）
                r#"{"id": 7, "player": 1, "chart": 1, "score": 900000, "perfect": 1, "good": 0, "bad": 0, "miss": 0, "max_combo": 1, "accuracy": 100.0, "full_combo": true, "std": 0.0, "std_score": 0.0}"#
            } else if text.contains("GET /record/9 ") {
                r#"{"id": 9, "player": 2, "chart": 1, "score": 1, "perfect": 1, "good": 0, "bad": 0, "miss": 0, "max_combo": 1, "accuracy": 100.0, "full_combo": true, "std": 0.0, "std_score": 0.0}"#
            } else {
                r#"{"id": 1, "name": "p", "language": "zh-CN"}"#
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

/// ctx（观察者注入数组；反作弊测试即注入 AntiCheatObserver）。
fn setup_ctx(
    mock_addr: std::net::SocketAddr,
    moderators: Vec<Arc<dyn phira_api::Moderator>>,
) -> Arc<ConnContext> {
    let base = format!("http://{mock_addr}");
    let http = Arc::new(HttpApiClient::new(base));
    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn phira_api::ApiClient>,
        rng: Arc::new(ThreadRngSource) as Arc<dyn phira_api::RandomSource>,
    };
    let rooms = impl_rooms_v1::RoomsV1::new(RoomConfig { monitors: vec![] }, deps);
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn phira_api::RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
    )
    .with_api(Arc::clone(&http) as Arc<dyn phira_api::ApiClient>)
    .with_moderators(moderators);
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
        auth: Arc::new(AuthOk),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
        proxy_protocol: false,
        auth_timeout: Duration::from_secs(10),
        admin_token: Some("t0k".to_owned()),
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(phira_server::storage::ConfigStore::disabled()),
    })
}

async fn client_connect(addr: std::net::SocketAddr) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    sock
}

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

async fn recv_cmd(sock: &mut TcpStream) -> ServerCommand {
    tokio::time::timeout(Duration::from_secs(5), async {
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
    .expect("recv_cmd timeout")
}

async fn spawn_server(
    ctx: Arc<ConnContext>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });
    (addr, accept)
}

/// 收帧直到匹配目标（响应/广播帧可多帧——每命令的响应帧不确定在流中的位置）。
/// 直读至多 6 帧（确定性：不嵌套 timeout；找不到则带全部帧 panic 诊断）。
async fn recv_until(sock: &mut TcpStream, pred: fn(&ServerCommand) -> bool) -> ServerCommand {
    let mut seen = Vec::new();
    for _ in 0..6 {
        match tokio::time::timeout(Duration::from_secs(2), recv_cmd(sock)).await {
            Ok(cmd) => {
                if pred(&cmd) {
                    return cmd;
                }
                seen.push(format!("{cmd:?}"));
            }
            Err(_) => break,
        }
    }
    panic!("recv_until 未找到目标帧，已见: {seen:?}");
}

/// 三态：首次 Played 放行（actor 未开局 → 非 Moderated 业务失败）→ 换房重放 →
/// Moderated 拒绝；同房重放不拦。
#[tokio::test]
async fn anticheat_rejects_cross_room_record_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let anti = AntiCheatObserver::new();
    let ctx = setup_ctx(
        mock_addr,
        vec![Arc::clone(&anti) as Arc<dyn phira_api::Moderator>],
    );
    let (addr, _accept) = spawn_server(ctx).await;

    // 连接 A（user=1）：房 r1 首次 Played{7} → intercept 记录指纹后放行。
    // 用独立连接隔离 settle 异步回注——不回绕房间状态机；drop A 即弃。
    let mut a = client_connect(addr).await;
    send_cmd(
        &mut a,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut a).await;
    send_cmd(
        &mut a,
        &ClientCommand::CreateRoom {
            id: RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::CreateRoom(Ok(())))).await;
    send_cmd(&mut a, &ClientCommand::Played { id: 7 }).await;
    // 响应可迟到（A2 回源/settle 异步）——只确认 intercept 记录已发生
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::Played(_))).await;
    assert_eq!(
        anti.fingerprint_len(),
        1,
        "连接 A 首次 Played 应记录指纹 (1,7)"
    );
    // 显式离房（清路由——同一 user 才能在 B 重建新房做跨房重放）
    send_cmd(&mut a, &ClientCommand::LeaveRoom).await;
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::LeaveRoom(_))).await;
    drop(a);

    // 连接 B（同一 user=1 新会话）：房 r2 上报同 record → 跨房重放 → Moderated
    let mut b = client_connect(addr).await;
    send_cmd(
        &mut b,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut b).await;
    send_cmd(
        &mut b,
        &ClientCommand::CreateRoom {
            id: RoomId::new("r2".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_until(&mut b, |m| matches!(m, ServerCommand::CreateRoom(Ok(())))).await;
    send_cmd(&mut b, &ClientCommand::Played { id: 7 }).await;
    let replay = recv_until(&mut b, |m| matches!(m, ServerCommand::Played(_))).await;
    assert!(
        matches!(&replay, ServerCommand::Played(Err(msg)) if msg.contains("record replay")),
        "跨房重放应被 Moderated 拒绝: {replay:?}"
    );

    // 同房重放（还留在 r2）：观察者不拦（actor 幂等负责）→ 非 replay 错误/受理
    send_cmd(&mut b, &ClientCommand::Played { id: 7 }).await;
    let dupe = recv_until(&mut b, |m| matches!(m, ServerCommand::Played(_))).await;
    assert!(
        !matches!(&dupe, ServerCommand::Played(Err(msg)) if msg.contains("record replay")),
        "同房重放不应被反作弊拦（actor 幂等负责）: {dupe:?}"
    );

    // 拒绝记录进环形（/admin/anticheat 数据源）
    let rejects = anti.rejects_snapshot();
    assert_eq!(rejects.len(), 1, "应有 1 条拒绝记录: {rejects:?}");
    assert_eq!(rejects[0].user, 1);
    assert_eq!(rejects[0].record, 7);
    assert_eq!(rejects[0].room, "r2");
    assert_eq!(rejects[0].first_room, "r1");
}

#[tokio::test]
async fn anticheat_hotplug_and_admin_view() {
    eprintln!("[h1] start");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let ctx = setup_ctx(mock_addr, Vec::new());
    let (addr, _accept) = spawn_server(Arc::clone(&ctx)).await;

    // 挂载观察者（组合根单例热插拔→总线生效）
    let anti = Arc::clone(&ctx.admin_anticheat);
    ctx.bus
        .add_moderator(Arc::clone(&anti) as Arc<dyn phira_api::Moderator>);
    eprintln!("[h2] added");

    // 连接 A（user=2，独立指纹）：房 r1 首次 Played{9}
    let mut a = client_connect(addr).await;
    send_cmd(
        &mut a,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut a).await;
    send_cmd(
        &mut a,
        &ClientCommand::CreateRoom {
            id: RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::CreateRoom(Ok(())))).await;
    send_cmd(&mut a, &ClientCommand::Played { id: 9 }).await;
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::Played(_))).await;
    assert_eq!(anti.fingerprint_len(), 1, "挂载后首次 Played 应记录指纹");
    send_cmd(&mut a, &ClientCommand::LeaveRoom).await;
    eprintln!("[h3] leave sent");
    let _ = recv_until(&mut a, |m| matches!(m, ServerCommand::LeaveRoom(_))).await;
    eprintln!("[h4] left");
    drop(a);

    // 连接 B（user=2 新会话）：房 r2 上报同 record → 拦截
    let mut b = client_connect(addr).await;
    send_cmd(
        &mut b,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut b).await;
    send_cmd(
        &mut b,
        &ClientCommand::CreateRoom {
            id: RoomId::new("r2".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_until(&mut b, |m| matches!(m, ServerCommand::CreateRoom(Ok(())))).await;
    send_cmd(&mut b, &ClientCommand::Played { id: 9 }).await;
    let replay = recv_until(&mut b, |m| matches!(m, ServerCommand::Played(_))).await;
    eprintln!("[h5] replay = {replay:?}");
    assert!(
        matches!(&replay, ServerCommand::Played(Err(msg)) if msg.contains("record replay")),
        "挂载后跨房重放应被拒: {replay:?}"
    );
    assert_eq!(anti.rejects_snapshot().len(), 1, "拒绝记录应为 1");

    // admin data: endpoint serialization covered in anticheat_admin_endpoint (pure-admin)
    assert_eq!(
        ctx.admin_anticheat.rejects_snapshot().len(),
        1,
        "rejects should be 1"
    );
}

/// R2 频率规则：60s 窗口 ≥10 局 → flag（纯观测不拦）；窗口裁剪（旧记录不计）+ 环形上限。
#[tokio::test]
async fn anticheat_high_frequency_flag() {
    let anti = AntiCheatObserver::new();

    // 9 局（窗口内）不触发
    for i in 0..9 {
        anti.record_play_at(1000 + i * 5, 1, 900_000, 99.0);
    }
    assert!(anti.flags_snapshot().is_empty(), "9 局不应 flag");

    // 第 10 局（窗口内）触发
    anti.record_play_at(1045, 1, 900_000, 99.0);
    let flags = anti.flags_snapshot();
    assert_eq!(flags.len(), 1, "应 flag 1 条: {flags:?}");
    assert_eq!(flags[0].user, 1);
    assert_eq!(flags[0].reason, "high_frequency");
    assert_eq!(flags[0].hits, 10);

    // 窗口裁剪：窗口外的旧记录不计数——远窗外 10 条（间隔 1s，at 互距小但整块离上一块 >60s）
    for i in 0..10 {
        anti.record_play_at(100_000 + i, 1, 900_000, 99.0);
    }
    let flags2 = anti.flags_snapshot();
    assert_eq!(
        flags2.len(),
        2,
        "新窗口命中 → 第 2 条; 旧窗口不再重复 flag: {flags2:?}"
    );
    assert_eq!(flags2[0].hits, 10);

    // 总量兜底（多用户防内存暴涨）：批次写入不 panic 即可（环形上限在实现内保证）
    for i in 0..40 {
        anti.record_play_at(300_000 + i, i32::try_from(i).unwrap_or(0) + 3, 1, 0.0);
    }
}

/// 纯管理面：GET /admin/anticheat 返回 200 + serialized data (healthz-style).
#[tokio::test]
async fn anticheat_admin_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));
    let ctx = setup_ctx(mock_addr, Vec::new());

    // produce one reject via intercept (unit logic; end-to-end covered elsewhere)
    let anti = &ctx.admin_anticheat;
    let room1 = CmdCtx {
        origin: Origin::Client { user_id: 7 },
        room_id: RoomId::new("r1".into()).unwrap(),
    };
    let room2 = CmdCtx {
        origin: Origin::Client { user_id: 7 },
        room_id: RoomId::new("r2".into()).unwrap(),
    };
    anti.intercept(&RoomCommand::Played { id: 42 }, &room1)
        .await
        .unwrap();
    let _ = anti
        .intercept(&RoomCommand::Played { id: 42 }, &room2)
        .await;

    let admin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();
    let ctx_admin = Arc::clone(&ctx);
    tokio::spawn(async move {
        let (stream, addr) = admin_listener.accept().await.unwrap();
        phira_server::admin::http_serve(stream, addr, ctx_admin)
            .await
            .unwrap();
    });
    let mut client = TcpStream::connect(admin_addr).await.unwrap();
    client
        .write_all(b"GET /admin/anticheat HTTP/1.1\r\nHost: t\r\nAuthorization: Bearer t0k\r\n\r\n")
        .await
        .unwrap();
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = client.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        resp.extend_from_slice(&buf[..n]);
    }
    let resp = String::from_utf8_lossy(&resp).into_owned();
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "admin/anticheat should 200: {resp}"
    );
    assert!(resp.contains("rejects"), "should contain rejects: {resp}");
    assert!(
        resp.contains("first_room"),
        "reject detail serialized: {resp}"
    );
}

//! 端到端全流程测试（阶段 2 验收，§14：真客户端开房联机全流程）。
//!
//! 组件全真实（除 mock API）：
//! - mock API：`/me` 按 token 返回用户身份（§9 Oracle 环境）
//! - HttpAuth + HttpApiClient（真实 HTTP 客户端）
//! - RoomsV1（真实货物）+ Bus + LifecycleTask + SessionSink + handle_connection
//!
//! 流程：握手 → 鉴权 → 建房 → 广播 → 第二用户加入 → 双端广播。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    ClientCommand, CmdCtx, JoinRoomResponse, Origin, RoomCommand, RoomConfig, RoomDeps, RoomError,
    RoomErrorCode, RoomEvent, RoomResponse, ServerCommand, TouchFrame, UserInfo, Varchar,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::http::{HttpApiClient, HttpAuth, ThreadRngSource};
use phira_server::server::{ConnContext, SessionSink, handle_connection, in_flight_bytes};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// —— mock API（§9）：按 token 返回身份 ——

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
            let body = if text.contains("Bearer tok1") {
                r#"{"id": 1, "name": "p1", "language": "en-US"}"#
            } else if text.contains("Bearer tok2") {
                r#"{"id": 2, "name": "p2", "language": "en-US"}"#
            } else if text.contains("Bearer tok3") {
                r#"{"id": 3, "name": "p3", "language": "en-US"}"#
            } else if text.contains("Bearer tokzh1") {
                // B2 i18n：中文用户（验收：错误文案按 lang 本地化）
                r#"{"id": 11, "name": "房子主", "language": "zh-CN"}"#
            } else if text.contains("Bearer tokzh2") {
                r#"{"id": 12, "name": "房客", "language": "zh-TW"}"#
            } else if text.contains("GET /chart/1 ") {
                r#"{"id": 1, "name": "Test Chart"}"#
            } else {
                r#"{"error": "invalid token"}"#
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

/// 真实组合（与 main.rs 同构，base = mock API）。
fn setup_ctx(mock_addr: std::net::SocketAddr) -> Arc<ConnContext> {
    setup_ctx_custom(mock_addr, vec![], None, vec![], vec![], None)
}

/// 指定 monitor 白名单的测试上下文（§6.5-4：monitor 需权限）。
fn setup_ctx_with_monitors(
    mock_addr: std::net::SocketAddr,
    monitors: Vec<i32>,
) -> Arc<ConnContext> {
    setup_ctx_custom(mock_addr, monitors, None, vec![], vec![], None)
}

/// 注入观察者/拦截者（§7.3）的测试上下文。
fn setup_ctx_with_moderators(
    mock_addr: std::net::SocketAddr,
    moderators: Vec<Arc<dyn phira_api::Moderator>>,
) -> Arc<ConnContext> {
    setup_ctx_custom(mock_addr, vec![], None, vec![], moderators, None)
}

/// 完整参数化上下文（欢迎语 + 私密房间前缀 + 观察者，§运营）。
fn setup_ctx_custom(
    mock_addr: std::net::SocketAddr,
    monitors: Vec<i32>,
    welcome: Option<&str>,
    hidden_prefixes: Vec<&str>,
    moderators: Vec<Arc<dyn phira_api::Moderator>>,
    admin_token: Option<&str>,
) -> Arc<ConnContext> {
    let base = format!("http://{mock_addr}");
    let http = Arc::new(HttpApiClient::new(base.clone()));
    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn phira_api::ApiClient>,
        rng: Arc::new(ThreadRngSource) as Arc<dyn phira_api::RandomSource>,
    };
    let rooms = impl_rooms_v1::RoomsV1::new(
        RoomConfig {
            monitors: monitors.clone(),
        },
        deps,
    );
    let config = Arc::new(RoomConfig { monitors });
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn phira_api::RoomFactory>,
        Arc::clone(&config),
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
    let room_list = Arc::new(phira_server::server::RoomListSink::new(
        hidden_prefixes.into_iter().map(str::to_owned).collect(),
    ));
    // 观察者组合（§7.3）：SessionSink + RoomListSink 一起挂 bus（与 Server::run 一致）
    let composite = Arc::new(phira_server::server::CompositeSink::new(vec![
        Arc::clone(&sink) as Arc<dyn phira_core::EventSink>,
        Arc::clone(&room_list) as Arc<dyn phira_core::EventSink>,
    ]));
    bus.attach_sink(composite as Arc<dyn phira_core::EventSink>);

    Arc::new(ConnContext {
        bus,
        auth: Arc::new(HttpAuth::new(base.clone())),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: welcome.map(str::to_owned),
        room_list,
        proxy_protocol: false,
        admin_token: admin_token.map(str::to_owned),
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
        config_store: Arc::new(phira_server::storage::ConfigStore::disabled()),
    })
}

/// 客户端：连接 + 握手，返回原始 socket。
async fn client_connect(addr: std::net::SocketAddr) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    sock
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

#[allow(clippy::too_many_lines)] // 端到端全流程脚本：步骤长是验收场景需求
#[tokio::test]
async fn full_flow_create_join_chat() {
    // mock API
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    // 服务器（随机端口）
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // —— 用户 1：握手 + 鉴权 ——
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    let auth1 = recv_cmd(&mut c1).await;
    let (ui, room_state) = match auth1 {
        ServerCommand::Authenticate(Ok((ui, rs))) => (ui, rs),
        other => panic!("鉴权应成功: {other:?}"),
    };
    assert_eq!(ui.id, 1);
    assert_eq!(ui.name, "p1");
    assert!(room_state.is_none(), "未入房时应无房间状态");

    // —— 用户 1 建房 ——
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    // 广播先于响应（原版：room.send 广播后返回 Ok）
    let broadcast = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            broadcast,
            ServerCommand::Message(phira_api::Message::CreateRoom { user: 1 })
        ),
        "应收到 CreateRoom 广播: {broadcast:?}"
    );
    let create_resp = recv_cmd(&mut c1).await;
    assert!(
        matches!(create_resp, ServerCommand::CreateRoom(Ok(()))),
        "建房应成功: {create_resp:?}"
    );

    // —— 用户 2：鉴权 + 加入 ——
    let mut c2 = client_connect(server_addr).await;
    send_cmd(
        &mut c2,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    let auth2 = recv_cmd(&mut c2).await;
    assert!(matches!(auth2, ServerCommand::Authenticate(Ok((ui, _))) if ui.id == 2));

    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    // 双端广播先到（OnJoinRoom + Message(JoinRoom)），响应在后
    let c1_onjoin = recv_cmd(&mut c1).await;
    assert!(
        matches!(&c1_onjoin, ServerCommand::OnJoinRoom(ui) if ui.id == 2),
        "c1 应收到 OnJoinRoom(user2): {c1_onjoin:?}"
    );
    let c1_join_msg = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &c1_join_msg,
            ServerCommand::Message(phira_api::Message::JoinRoom { user: 2, .. })
        ),
        "c1 应收到 JoinRoom 广播: {c1_join_msg:?}"
    );
    let c2_onjoin = recv_cmd(&mut c2).await;
    // 原版语义：OnJoinRoom 广播含自己（user2 收到自己的加入通知）
    assert!(matches!(&c2_onjoin, ServerCommand::OnJoinRoom(ui) if ui.id == 2));
    let c2_join_msg = recv_cmd(&mut c2).await;
    assert!(matches!(
        &c2_join_msg,
        ServerCommand::Message(phira_api::Message::JoinRoom { user: 2, .. })
    ));

    // JoinRoom 响应（含房内列表）
    let join_resp = recv_cmd(&mut c2).await;
    let JoinRoomResponse { users, live, .. } = match join_resp {
        ServerCommand::JoinRoom(Ok(jr)) => jr,
        other => panic!("加入应成功: {other:?}"),
    };
    assert_eq!(users.len(), 2, "房内应有 2 人: {users:?}");
    assert!(!live);

    // —— 聊天广播 ——
    send_cmd(
        &mut c1,
        &ClientCommand::Chat {
            message: Varchar::new("hello".into()).unwrap(),
        },
    )
    .await;
    // 广播先于响应（原版语义）
    let c1_chat_msg = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &c1_chat_msg,
            ServerCommand::Message(phira_api::Message::Chat { user: 1, content }) if content == "hello"
        ),
        "c1 应收到自己消息的广播: {c1_chat_msg:?}"
    );
    let c2_chat_msg = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &c2_chat_msg,
            ServerCommand::Message(phira_api::Message::Chat { user: 1, content }) if content == "hello"
        ),
        "c2 应收到聊天广播: {c2_chat_msg:?}"
    );
    let c1_chat_resp = recv_cmd(&mut c1).await;
    assert!(matches!(c1_chat_resp, ServerCommand::Chat(Ok(()))));

    // —— 心跳仍正常 ——
    send_cmd(&mut c1, &ClientCommand::Ping).await;
    assert!(matches!(recv_cmd(&mut c1).await, ServerCommand::Pong));

    drop(c1);
    drop(c2);
}

/// 重复入房（§6.5-27）：用户在房 A 再加入房 B → 错误响应。
#[tokio::test]
async fn duplicate_join_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::Authenticate(Ok(_))
    ));

    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await; // CreateRoom 广播（先）
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::CreateRoom(Ok(()))
    ));

    // 已在房 A，再建房 B → AlreadyInRoom
    // （等限速窗口：ISSUE-0006 限速 CreateRoom 1s 间隔——否则先撞 TooManyRequests，
    //   本测试验证的是 §6.5-27 重复入房语义，限速语义由 rate_limit 测试单独覆盖）
    tokio::time::sleep(Duration::from_millis(1100)).await;
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r2".into()).unwrap(),
        },
    )
    .await;
    let resp = recv_cmd(&mut c1).await;
    assert!(
        matches!(&resp, ServerCommand::CreateRoom(Err(msg)) if msg.contains("already in room")),
        "重复建房应被拒: {resp:?}"
    );
    // 已有房间仍可用（心跳/聊天正常）
    send_cmd(&mut c1, &ClientCommand::Ping).await;
    assert!(matches!(recv_cmd(&mut c1).await, ServerCommand::Pong));
    drop(c1);
}

#[allow(dead_code)]
fn _unused(_: RoomResponse, _: UserInfo, _: mpsc::Receiver<()>) {}

/// 游戏流程：SelectChart（回源 /chart）→ RequestStart → Ready → 全员 Ready → StartPlaying。
#[allow(clippy::too_many_lines)] // 游戏全流程脚本长是验收场景需求
#[tokio::test]
async fn game_flow_select_ready_start() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 用户 1 鉴权 + 建房
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("g1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await; // CreateRoom 广播
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::CreateRoom(Ok(()))
    ));

    // 未选图 RequestStart → 拒绝（§6.5-7）
    send_cmd(&mut c1, &ClientCommand::RequestStart).await;
    let resp = recv_cmd(&mut c1).await;
    assert!(
        matches!(&resp, ServerCommand::RequestStart(Err(msg)) if msg.contains("no chart")),
        "未选图请求开始应拒绝: {resp:?}"
    );

    // 选图（回源 mock /chart/1）
    send_cmd(&mut c1, &ClientCommand::SelectChart { id: 1 }).await;
    let broadcast = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &broadcast,
            ServerCommand::Message(phira_api::Message::SelectChart { id: 1, name, .. }) if name == "Test Chart"
        ),
        "选图广播应含谱面名: {broadcast:?}"
    );
    let state = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &state,
            ServerCommand::ChangeState(phira_api::RoomState::SelectChart(Some(1)))
        ),
        "选图后 ChangeState(SelectChart(Some(1))): {state:?}"
    );
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::SelectChart(Ok(()))
    ));

    // 用户 2 加入 + Ready
    let mut c2 = client_connect(server_addr).await;
    send_cmd(
        &mut c2,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c2).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("g1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await; // c1: OnJoinRoom + Message(JoinRoom)
    }
    for _ in 0..3 {
        let _ = recv_cmd(&mut c2).await; // c2: OnJoinRoom + Message(JoinRoom) + JoinRoom 响应
    }

    // 用户 1 RequestStart（进入 WaitForReady，host 默认 ready，§6.5-7）
    send_cmd(&mut c1, &ClientCommand::RequestStart).await;
    let gs = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &gs,
            ServerCommand::Message(phira_api::Message::GameStart { user: 1 })
        ),
        "RequestStart 应 GameStart 广播: {gs:?}"
    );
    let wfr = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &wfr,
            ServerCommand::ChangeState(phira_api::RoomState::WaitingForReady)
        ),
        "应 ChangeState(WaitingForReady): {wfr:?}"
    );
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::RequestStart(Ok(()))
    ));
    // c2 也收到 RequestStart 的 All 广播（GameStart + ChangeState）
    let _ = recv_cmd(&mut c2).await;
    let _ = recv_cmd(&mut c2).await;

    // 用户 2 Ready（WaitForReady 状态有效）
    send_cmd(&mut c2, &ClientCommand::Ready).await;
    let c2_ready_bcast = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &c2_ready_bcast,
            ServerCommand::Message(phira_api::Message::Ready { user: 2 })
        ),
        "c2 应收到自己的 Ready 广播: {c2_ready_bcast:?}"
    );
    // 全员 ready（host 默认 + user2）→ StartPlaying + ChangeState(Playing)
    let c1_ready = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &c1_ready,
            ServerCommand::Message(phira_api::Message::Ready { user: 2 })
        ),
        "c1 应收到 Ready 广播: {c1_ready:?}"
    );
    let c2_start = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &c2_start,
            ServerCommand::Message(phira_api::Message::StartPlaying)
        ),
        "c2 应收到 StartPlaying: {c2_start:?}"
    );
    let c1_start = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &c1_start,
            ServerCommand::Message(phira_api::Message::StartPlaying)
        ),
        "c1 应收到 StartPlaying: {c1_start:?}"
    );
    let c1_playing = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &c1_playing,
            ServerCommand::ChangeState(phira_api::RoomState::Playing)
        ),
        "应 ChangeState(Playing): {c1_playing:?}"
    );
    drop(c1);
    drop(c2);
}

/// §10.4 红线闭环：鉴权前 4KiB 收紧（frames.rs 测拒绝），鉴权后放开到 2MiB——
/// 已鉴权连接发 >4KiB 帧应被接受（连接不断，心跳仍应答）。
#[tokio::test]
async fn authed_large_frame_accepted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 鉴权成功
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    let auth = recv_cmd(&mut c1).await;
    assert!(matches!(auth, ServerCommand::Authenticate(Ok(_))));

    // 大帧：1000 个 TouchFrame（每个 ~5 字节 ≈ 5KiB > 4KiB 收紧线）
    // 时间戳用 f32 表示（TouchFrame.time 协议字段）；i32→f32 精度损失对测试帧无意义
    #[allow(clippy::cast_precision_loss)]
    let frames = (0..1000)
        .map(|i| phira_api::TouchFrame {
            time: i as f32,
            points: Vec::new(),
        })
        .collect::<Vec<_>>();
    send_cmd(
        &mut c1,
        &ClientCommand::Touches {
            frames: std::sync::Arc::new(frames),
        },
    )
    .await;

    // 连接未被断：心跳仍应答
    send_cmd(&mut c1, &ClientCommand::Ping).await;
    let pong = recv_cmd(&mut c1).await;
    assert!(
        matches!(pong, ServerCommand::Pong),
        "鉴权后大帧应被接受（连接存活，Ping→Pong）: {pong:?}"
    );
    drop(c1);
}

/// §6.5-4：观战者（monitor）加入——不占玩家名额、需白名单权限；
/// 全员（玩家 + monitor）ready 才 StartPlaying（impl check_all_ready 语义）。
#[allow(clippy::too_many_lines)] // 全流程脚本长是验收场景需求（同 game_flow）
#[tokio::test]
async fn monitor_join_and_game_flow() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx_with_monitors(mock_addr, vec![2]);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;

    // 双方鉴权
    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
    }

    // 建房
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::Message(phira_api::Message::CreateRoom { user: 1 })
    ));
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::CreateRoom(Ok(()))
    ));

    // user2 以 monitor 身份加入（白名单 [2]，§6.5-4）
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: true,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await; // c1: OnJoinRoom + Message(JoinRoom)
    }
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(&f, ServerCommand::OnJoinRoom(ui) if ui.id == 2),
        "c2 收 OnJoinRoom: {f:?}"
    );
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::JoinRoom { user: 2, .. })
        ),
        "c2 收 Message(JoinRoom): {f:?}"
    );
    assert!(matches!(
        recv_cmd(&mut c2).await,
        ServerCommand::JoinRoom(Ok(_))
    ));

    // 选图（host）
    send_cmd(&mut c1, &ClientCommand::SelectChart { id: 1 }).await;
    let _ = recv_cmd(&mut c1).await; // Message(SelectChart)
    let _ = recv_cmd(&mut c1).await; // ChangeState
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::SelectChart(Ok(()))
    ));
    for _ in 0..2 {
        let _ = recv_cmd(&mut c2).await; // c2: Message(SelectChart) + ChangeState
    }

    // RequestStart → WaitForReady（host 默认 ready）
    send_cmd(&mut c1, &ClientCommand::RequestStart).await;
    let _ = recv_cmd(&mut c1).await; // Message(GameStart)
    let _ = recv_cmd(&mut c1).await; // ChangeState(WaitingForReady)
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::RequestStart(Ok(()))
    ));
    for _ in 0..2 {
        let _ = recv_cmd(&mut c2).await; // c2: Message(GameStart) + ChangeState
    }

    // monitor ready → 全员（玩家 + monitor）→ StartPlaying
    send_cmd(&mut c2, &ClientCommand::Ready).await;
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::Ready { user: 2 })
        ),
        "c2 收自己的 Ready 广播: {f:?}"
    );
    let _ = recv_cmd(&mut c2).await; // Ready(Ok)
    let f = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::Ready { user: 2 })
        ),
        "c1 收 Ready 广播: {f:?}"
    );
    let f = recv_cmd(&mut c1).await;
    assert!(
        matches!(&f, ServerCommand::Message(phira_api::Message::StartPlaying)),
        "玩家 + monitor 全员 ready → StartPlaying: {f:?}"
    );
    drop(c1);
    drop(c2);
}

/// §6.5-5：房主离开 → 房间顺延给下一位（NewHost + ChangeHost 单播）。
#[tokio::test]
async fn host_leave_transfers_ownership() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;

    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
    }

    // user1 建房 + user2 加入（玩家）
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await;
    }
    for _ in 0..3 {
        let _ = recv_cmd(&mut c2).await;
    }

    // user1 离开 → user2 成为新 host
    send_cmd(&mut c1, &ClientCommand::LeaveRoom).await;
    // 离开者也被投递（§4.9-4 先解析后应用）：LeaveRoom 广播 + NewHost 广播 +
    // ChangeHost(false)（单播旧 host）+ LeaveRoom(Ok) 响应
    let mut c1_frames = Vec::new();
    for _ in 0..4 {
        c1_frames.push(recv_cmd(&mut c1).await);
    }
    assert!(
        c1_frames
            .iter()
            .any(|f| matches!(f, ServerCommand::LeaveRoom(Ok(())))),
        "c1 应收到 LeaveRoom(Ok): {c1_frames:?}"
    );
    assert!(
        c1_frames
            .iter()
            .any(|f| matches!(f, ServerCommand::ChangeHost(false))),
        "旧 host 收 ChangeHost(false): {c1_frames:?}"
    );
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::LeaveRoom { user: 1, .. })
        ),
        "c2 收 LeaveRoom 广播: {f:?}"
    );
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::NewHost { user: 2 })
        ),
        "c2 收 NewHost 广播: {f:?}"
    );
    let f = recv_cmd(&mut c2).await;
    assert!(
        matches!(f, ServerCommand::ChangeHost(true)),
        "新 host 收 ChangeHost(true): {f:?}"
    );
    drop(c1);
    drop(c2);
}

/// §11 优雅停机：维护广播送达所有在线会话（不依赖房间状态机）。
#[tokio::test]
async fn maintenance_broadcast_reaches_all() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    let ctx_bg = Arc::clone(&ctx);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx_bg);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 两个已鉴权会话
    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;
    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
    }

    // 维护广播（§11：系统 Chat，user=0）
    ctx.sink
        .broadcast(ServerCommand::Message(phira_api::Message::Chat {
            user: 0,
            content: "服务器维护中".to_owned(),
        }))
        .await;

    for (name, c) in [("c1", &mut c1), ("c2", &mut c2)] {
        let f = recv_cmd(c).await;
        assert!(
            matches!(
                &f,
                ServerCommand::Message(phira_api::Message::Chat {
                    user: 0,
                    content,
                }) if content.contains("维护中")
            ),
            "{name} 应收到维护广播: {f:?}"
        );
    }
    drop(c1);
    drop(c2);
}

/// Never Trust the Client：非 host 越权全链路（socket → 会话层 → bus → actor）→ OnlyHost 拒绝。
/// 契约测试在 actor 直驱层已断言；本测试验证全链路无旁路（用户 id 来自鉴权，不可伪造）。
#[tokio::test]
async fn non_host_privilege_escalation_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;
    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
    }

    // user1 建房 + user2 加入（普通玩家）
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await;
    }
    for _ in 0..3 {
        let _ = recv_cmd(&mut c2).await;
    }

    // user2（非 host）越权：LockRoom / CycleRoom / SelectChart / RequestStart
    for cmd in [
        ClientCommand::LockRoom { lock: true },
        ClientCommand::CycleRoom { cycle: true },
        ClientCommand::SelectChart { id: 1 },
        ClientCommand::RequestStart,
    ] {
        send_cmd(&mut c2, &cmd).await;
        let f = recv_cmd(&mut c2).await;
        let err_text = match &cmd {
            ClientCommand::LockRoom { .. } => "LockRoom",
            ClientCommand::CycleRoom { .. } => "CycleRoom",
            ClientCommand::SelectChart { .. } => "SelectChart",
            ClientCommand::RequestStart => "RequestStart",
            _ => unreachable!(),
        };
        assert!(
            matches!(&f, ServerCommand::LockRoom(Err(m)) | ServerCommand::CycleRoom(Err(m))
                | ServerCommand::SelectChart(Err(m)) | ServerCommand::RequestStart(Err(m))
                if m == "only host can do this"),
            "{err_text} 非 host 应被拒 OnlyHost: {f:?}"
        );
    }

    // user2 伪造 user1 身份不可能（user_id 来自鉴权 state）——已鉴权命令不携带用户 id
    drop(c1);
    drop(c2);
}

/// 服务器逻辑闭环：玩家断线（心跳 10s 超时）→ Disconnected → 房间驱逐 → 房内用户收到 LeaveRoom。
#[tokio::test]
async fn disconnect_evicts_from_room() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;
    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        let r = recv_cmd(c).await;
        assert!(matches!(r, ServerCommand::Authenticate(Ok(_))));
    }

    // user1 建房 + user2 加入
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await;
    }
    for _ in 0..3 {
        let _ = recv_cmd(&mut c2).await;
    }

    // user1 直接断开（不 LeaveRoom）→ 10s 心跳超时 → 服务器驱逐 → user2 收到 LeaveRoom
    drop(c1);

    // c2 持续心跳保持存活（否则自己也 10s 超时）；同时读帧等 LeaveRoom 广播。
    // 无超时读帧（外层 20s 兜底）——recv_cmd 的 2s 不够等 10s 心跳。
    let deadline = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(2)) => {
                    send_cmd(&mut c2, &ClientCommand::Ping).await;
                }
                f = read_frame_raw(&mut c2) => {
                    let cmd: ServerCommand = phira_api::decode_packet(&f).unwrap();
                    if matches!(
                        &cmd,
                        ServerCommand::Message(phira_api::Message::LeaveRoom { user: 1, .. })
                    ) {
                        return;
                    }
                }
            }
        }
    })
    .await;
    assert!(
        deadline.is_ok(),
        "20s 内 user2 应收到 user1 断线的 LeaveRoom 广播"
    );
    drop(c2);
}

/// 无超时读一帧（raw 载荷）。
async fn read_frame_raw(sock: &mut TcpStream) -> Vec<u8> {
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
    payload
}

/// §运营：HTTP 独立端口——GET /rooms 返回公开房间列表（私密前缀过滤）。
/// 回归锚点：源码 CRLF 转义曾被污染成真实换行，读循环空行检测失效导致客户端收不到
/// （strace 实锤 sendto 174 成功但 shutdown ENOTCONN，连接已死）。此测试保底。
#[tokio::test]
async fn http_rooms_endpoint_with_private_filter() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx_custom(mock_addr, vec![], None, vec!["solo"], vec![], None);

    // 管理 HTTP 端点（独立端口，§运营）
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_ctx = Arc::clone(&ctx);
    tokio::spawn(async move {
        phira_server::admin::http_accept_loop(Some(http_listener), http_ctx).await;
    });

    let mp_ctx = Arc::clone(&ctx);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&mp_ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 协议路径：建房（公开 pub1 + 私密 solo-x）
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("pub1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    // 注意：不 drop c1——房主断线房间销毁（§6.5），需保持连接让房间存活

    // 第二个用户建私密房间（solo 前缀）
    let mut c2 = client_connect(server_addr).await;
    send_cmd(
        &mut c2,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c2).await;
    send_cmd(
        &mut c2,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("solo-9f3a".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c2).await;
    let _ = recv_cmd(&mut c2).await;
    // 同上，保持 c2 连接

    // HTTP 路径：独立端口 GET /rooms
    let mut sock = TcpStream::connect(http_addr).await.unwrap();
    sock.write_all(b"GET /rooms HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut resp = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = sock.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        resp.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "应返回 200: {text}");
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    assert!(body.contains("pub1"), "公开房间应展示: {body}");
    assert!(!body.contains("solo-9f3a"), "私密前缀房间不应展示: {body}");

    // MP 协议路径不受 HTTP 影响（c1/c2 仍保持连接）
    let mut c3 = client_connect(server_addr).await;
    send_cmd(&mut c3, &ClientCommand::Ping).await;
    assert!(matches!(recv_cmd(&mut c3).await, ServerCommand::Pong));
    drop(c1);
    drop(c2);
    drop(c3);
}

/// §运营：进服欢迎语——鉴权成功后收到 user=0 系统消息。
#[tokio::test]
async fn welcome_message_sent_after_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx_custom(
        mock_addr,
        vec![],
        Some("欢迎来到 r0semi"),
        vec![],
        vec![],
        None,
    );
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    // 鉴权响应 + 欢迎语（user=0 系统消息）
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    let f = recv_cmd(&mut c1).await;
    assert!(
        matches!(
            &f,
            ServerCommand::Message(phira_api::Message::Chat { user: 0, content })
                if content == "欢迎来到 r0semi"
        ),
        "鉴权后应收到欢迎语: {f:?}"
    );
    drop(c1);
}

/// §6.5 规则 3：对局中（Playing）加入 → 客户端收到 GameOngoing 提示（全链路）。
#[allow(clippy::too_many_lines)] // 全流程脚本长是验收场景需求（同 game_flow）
#[tokio::test]
async fn join_during_game_rejected_with_hint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    let mut c1 = client_connect(server_addr).await;
    let mut c2 = client_connect(server_addr).await;
    for (c, tok) in [(&mut c1, "tok1"), (&mut c2, "tok2")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
    }

    // user1 建房 + user2 加入
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    for _ in 0..2 {
        let _ = recv_cmd(&mut c1).await;
    }
    for _ in 0..3 {
        let _ = recv_cmd(&mut c2).await;
    }

    // 开局：选图 + RequestStart + user2 Ready → StartPlaying
    send_cmd(&mut c1, &ClientCommand::SelectChart { id: 1 }).await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::SelectChart(Ok(()))
    ));
    for _ in 0..2 {
        let _ = recv_cmd(&mut c2).await;
    }
    send_cmd(&mut c1, &ClientCommand::RequestStart).await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::RequestStart(Ok(()))
    ));
    for _ in 0..2 {
        let _ = recv_cmd(&mut c2).await;
    }
    send_cmd(&mut c2, &ClientCommand::Ready).await;
    assert!(matches!(
        recv_cmd(&mut c2).await,
        ServerCommand::Message(phira_api::Message::Ready { user: 2 })
    ));
    let _ = recv_cmd(&mut c2).await; // Ready(Ok)
    // 全员 ready → StartPlaying（双端）
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c1).await;
    let _ = recv_cmd(&mut c2).await;

    // user3 加入对局中房间 → 收到 GameOngoing 提示
    let mut c3 = client_connect(server_addr).await;
    send_cmd(
        &mut c3,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok3".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c3).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut c3,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    let f = recv_cmd(&mut c3).await;
    assert!(
        matches!(
            &f,
            ServerCommand::JoinRoom(Err(msg)) if msg.contains("ongoing")
        ),
        "对局中加入应收到 GameOngoing 提示: {f:?}"
    );
    drop(c1);
    drop(c2);
    drop(c3);
}

/// 热路径内存压测（§6.5-17 编码一次共享 + §10.4 记账平衡）：完整对局进入 Playing，
/// 多 monitor 在线，房主发 N 次大 Touches 帧 → 广播给全部 monitor → EncodeCache 编码一次共享。
/// 量 `in_flight_bytes` 峰值（必须 ≤ 64MiB 守卫）与压测后回落（无泄漏）。
#[allow(clippy::too_many_lines)] // 完整对局脚本长是验收场景需求（同 monitor 流程）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hotpath_touches_broadcast_memory() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    // monitor 白名单 = [2, 3]（两个观战者，验证同一帧编码一次共享）
    let ctx = setup_ctx_with_monitors(mock_addr, vec![2, 3]);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 房主（tok1）鉴权 + 建房
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("hp".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await; // CreateRoom 广播
    assert!(matches!(
        recv_cmd(&mut c1).await,
        ServerCommand::CreateRoom(Ok(()))
    ));

    // 两个 monitor（tok2/tok3）加入（白名单 [2,3]）
    let mut c2 = client_connect(server_addr).await;
    let mut c3 = client_connect(server_addr).await;
    for (c, tok) in [(&mut c2, "tok2"), (&mut c3, "tok3")] {
        send_cmd(
            c,
            &ClientCommand::Authenticate {
                token: Varchar::new(tok.into()).unwrap(),
            },
        )
        .await;
        assert!(matches!(
            recv_cmd(c).await,
            ServerCommand::Authenticate(Ok(_))
        ));
        send_cmd(
            c,
            &ClientCommand::JoinRoom {
                id: phira_api::RoomId::new("hp".into()).unwrap(),
                monitor: true,
            },
        )
        .await;
        for _ in 0..2 {
            let _ = recv_cmd(&mut c1).await; // c1: OnJoinRoom + Message(JoinRoom)
        }
        for _ in 0..3 {
            let _ = recv_cmd(c).await; // monitor: OnJoinRoom + Message(JoinRoom) + JoinRoom 响应
        }
    }

    // 选图 + RequestStart → WaitForReady
    send_cmd(&mut c1, &ClientCommand::SelectChart { id: 1 }).await;
    for _ in 0..3 {
        let _ = recv_cmd(&mut c1).await; // Message(SelectChart) + ChangeState + SelectChart(Ok)
    }
    send_cmd(&mut c1, &ClientCommand::RequestStart).await;
    for _ in 0..3 {
        let _ = recv_cmd(&mut c1).await; // Message(GameStart) + ChangeState(WaitingForReady) + Ok
    }
    let _ = recv_cmd(&mut c2).await;
    let _ = recv_cmd(&mut c2).await;
    let _ = recv_cmd(&mut c3).await;
    let _ = recv_cmd(&mut c3).await;

    // monitor ready → 全员（房主默认 + 2 monitor）→ StartPlaying（live）
    // ready 是 All 广播：每次 ready 都广播给全部在线者，帧序难精确推演——
    // 稳健做法是循环读直到收到 StartPlaying（读定匹配判定，带超时兜底）。
    for c in [&mut c2, &mut c3] {
        send_cmd(c, &ClientCommand::Ready).await;
    }
    // 三个连接都循环读直到各自看到 StartPlaying
    let mut got_start = false;
    'drain: for c in [&mut c1, &mut c2, &mut c3] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            let f = recv_cmd(c).await;
            if matches!(f, ServerCommand::Message(phira_api::Message::StartPlaying)) {
                got_start = true;
                continue 'drain;
            }
        }
    }
    assert!(got_start, "全员 ready 应 StartPlaying");

    // —— 热路径：房主发 N 次大 Touches 帧 → 广播给 2 个 monitor ——
    // 每帧 ~1MiB 编码（8 万 TouchFrame），N 次连续；monitor 只读不消费 → 队列积压记账
    let baseline = in_flight_bytes();
    let big_frames: Vec<TouchFrame> = (0..80_000)
        .map(|i| TouchFrame {
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            time: i as f32 * 0.001,
            points: vec![],
        })
        .collect();
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // 监控峰值
    let peak_watch = Arc::clone(&peak);
    let monitor_task = tokio::spawn(async move {
        let end = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < end {
            let v = in_flight_bytes();
            if v > peak_watch.load(std::sync::atomic::Ordering::SeqCst) {
                peak_watch.store(v, std::sync::atomic::Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    for _ in 0..10 {
        send_cmd(
            &mut c1,
            &ClientCommand::Touches {
                frames: Arc::new(big_frames.clone()),
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    monitor_task.await.unwrap();
    let peak_v = peak.load(std::sync::atomic::Ordering::SeqCst);

    eprintln!(
        "== 热路径压测 ==\nbaseline={} peak={} (MiB: {})",
        baseline,
        peak_v,
        peak_v / 1_048_576
    );

    assert!(
        peak_v <= 64 * 1024 * 1024,
        "热路径在途字节峰值 {} MiB 超 64 MiB 守卫",
        peak_v / 1_048_576
    );
    assert!(
        peak_v > baseline,
        "热路径应产生在途记账增长: baseline={baseline} peak={peak_v}"
    );

    // 回落：断开 monitor（慢消费者积压场景，§10.4）——连接关闭后写任务清账（MemoryReleaser）
    // 前提：monitor 未消费的积压字节仍记账；断开后写任务退出应释放。
    drop(c2);
    drop(c3);
    sleep_to_release().await;
    let after = in_flight_bytes();
    eprintln!(
        "断开 monitor 后 after={} (MiB: {})",
        after,
        after / 1_048_576
    );
    assert!(
        after <= baseline + 64 * 1024,
        "断开慢消费者后记账应回落（无泄漏）: baseline={baseline} after={after}"
    );

    drop(c1);
}

/// 等写任务清账回落（连接关闭 → MemoryReleaser → in_flight 递减）。
async fn sleep_to_release() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// B2 i18n 验收：错误文案按用户 `language` 本地化（对齐原版 per-user 三语）。
///
/// zh-CN 房主建房 + 锁房 → zh-TW 用户 JoinRoom 被 RoomLocked 拒绝 →
/// 断言收到**繁体**文案「房間已鎖定」（同时覆盖 zh-CN 房主的简洁侧解析）。
#[tokio::test]
async fn error_messages_localized_by_user_language() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx(mock_addr);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // zh-CN 房主（id=11）鉴权 + 建房 + 锁房
    let mut host = client_connect(server_addr).await;
    send_cmd(
        &mut host,
        &ClientCommand::Authenticate {
            token: Varchar::new("tokzh1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut host).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut host,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("i18n".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut host).await; // CreateRoom 广播
    assert!(matches!(
        recv_cmd(&mut host).await,
        ServerCommand::CreateRoom(Ok(()))
    ));

    // zh-TW 客人（id=12）先加入（未锁，成功）→ 房主锁房 → 再离开后重进被拒？
    // 简化：客人先不动，直接让房主锁房；再让一个**新** zh-TW 连接尝试加入 →
    // RoomLocked 中文（繁体）文案。
    send_cmd(&mut host, &ClientCommand::LockRoom { lock: true }).await;
    let _ = recv_cmd(&mut host).await; // Message(LockRoom) 广播
    assert!(matches!(
        recv_cmd(&mut host).await,
        ServerCommand::LockRoom(Ok(()))
    ));

    // zh-TW 客人鉴权 + 加入已锁房间 → 应收**繁体**错误
    let mut guest = client_connect(server_addr).await;
    send_cmd(
        &mut guest,
        &ClientCommand::Authenticate {
            token: Varchar::new("tokzh2".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut guest).await,
        ServerCommand::Authenticate(Ok(_))
    ));
    send_cmd(
        &mut guest,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("i18n".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    let resp = recv_cmd(&mut guest).await;
    match resp {
        ServerCommand::JoinRoom(Err(msg)) => {
            assert_eq!(msg, "房間已鎖定", "zh-TW 用户应收到繁体本地化文案: {msg}");
        }
        other => panic!("锁定房间的加入应失败: {other:?}"),
    }

    drop(host);
    drop(guest);
}

// —— §7.3 观察者拦截：core 到协议出口全链路 ——

/// 被拦截命令到达真连接：Failure(Moderated)（code 无 l10n 映射 → 回落观察者英文文案），
/// 非目标用户不受影响，连接保持健康（Ping→Pong 通）。
#[tokio::test]
async fn moderator_intercept_reaches_client_as_failure() {
    // mock API
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx_with_moderators(
        mock_addr,
        vec![Arc::new(BlockUser1) as Arc<dyn phira_api::Moderator>],
    );
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 目标用户 1（tok1）：CreateRoom → Failure(Moderated)（文案透传），连接仍健康
    let mut c = client_connect(server_addr).await;
    send_cmd(
        &mut c,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        recv_cmd(&mut c).await,
        ServerCommand::Authenticate(Ok((_, None)))
    ));
    send_cmd(
        &mut c,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("mod-e2e".into()).unwrap(),
        },
    )
    .await;
    match recv_cmd(&mut c).await {
        ServerCommand::CreateRoom(Err(msg)) => {
            assert_eq!(msg, "blocked by e2e moderator", "拦截文案应透传到客户端");
        }
        other => panic!("被拦命令应回 Failure(Moderated)，got {other:?}"),
    }
    // 连接健康：Ping → Pong
    send_cmd(&mut c, &ClientCommand::Ping).await;
    assert!(matches!(recv_cmd(&mut c).await, ServerCommand::Pong));

    // 非目标用户 2（tok2）：正常建房
    let mut c2 = client_connect(server_addr).await;
    send_cmd(
        &mut c2,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    recv_cmd(&mut c2).await; // Authenticate(Ok)
    send_cmd(
        &mut c2,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("mod-ok".into()).unwrap(),
        },
    )
    .await;
    // 未受影响用户：广播先于响应（原版语义，与 full_flow 一致）
    assert!(matches!(
        recv_cmd(&mut c2).await,
        ServerCommand::Message(phira_api::Message::CreateRoom { user: 2 })
    ));
    assert!(
        matches!(recv_cmd(&mut c2).await, ServerCommand::CreateRoom(Ok(()))),
        "非目标用户不得被拦"
    );
}

/// 拦 user 1 的全部客户端命令（e2e 用替身，§7.3）。
struct BlockUser1;

#[async_trait::async_trait]
impl phira_api::Moderator for BlockUser1 {
    fn kind(&self) -> &'static str {
        "block-user-1"
    }

    async fn intercept(&self, _cmd: &RoomCommand, ctx: &CmdCtx) -> Result<(), RoomError> {
        if let Origin::Client { user_id: 1 } = ctx.origin {
            return Err(RoomError::Business {
                code: RoomErrorCode::Moderated,
                msg: "blocked by e2e moderator".to_owned(),
            });
        }
        Ok(())
    }

    async fn on_event(&self, _ev: &RoomEvent) {}
}

/// 手写 HTTP 请求（管理端点测试用；头体一次写入，服务端容忍同包）。
#[allow(clippy::too_many_lines)] // 手写 HTTP 客户端全流程，测试专用
async fn http_raw(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    auth: Option<&str>,
) -> String {
    use std::fmt::Write;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: test\r\n");
    if method == "POST" {
        let _ = write!(req, "Content-Length: {}\r\n", body.len());
    }
    if let Some(auth) = auth {
        let _ = write!(req, "Authorization: {auth}\r\n");
    }
    req.push_str("\r\n");
    client.write_all(req.as_bytes()).await.unwrap();
    if !body.is_empty() {
        client.write_all(body.as_bytes()).await.unwrap();
    }
    let mut resp = Vec::new();
    client.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

/// 写面全链路（阶段 2，docs/admin-api.md §4）：管理员 HTTP POST kick → 系统命令族 →
/// 房内广播 LeaveRoom（含被踢者本人，先解析后应用 §4.9-4）；审计可查；无 token 401。
#[allow(clippy::too_many_lines)] // 端到端剧本：步骤长是验收场景需求
#[tokio::test]
async fn admin_kick_roundtrip_via_http() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(mock_api(mock_addr));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    let ctx = setup_ctx_custom(mock_addr, vec![], None, vec![], vec![], Some("admin-token"));

    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_ctx = Arc::clone(&ctx);
    tokio::spawn(async move {
        phira_server::admin::http_accept_loop(Some(http_listener), http_ctx).await;
    });
    let mp_ctx = Arc::clone(&ctx);
    tokio::spawn(async move {
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&mp_ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 用户 1 建房 + 用户 2 入房
    let mut c1 = client_connect(server_addr).await;
    send_cmd(
        &mut c1,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok1".into()).unwrap(),
        },
    )
    .await;
    recv_cmd(&mut c1).await; // Authenticate(Ok)
    send_cmd(
        &mut c1,
        &ClientCommand::CreateRoom {
            id: phira_api::RoomId::new("adm-r1".into()).unwrap(),
        },
    )
    .await;
    let _ = recv_cmd(&mut c1).await; // Message::CreateRoom 广播
    let _ = recv_cmd(&mut c1).await; // CreateRoom(Ok)

    let mut c2 = client_connect(server_addr).await;
    send_cmd(
        &mut c2,
        &ClientCommand::Authenticate {
            token: Varchar::new("tok2".into()).unwrap(),
        },
    )
    .await;
    recv_cmd(&mut c2).await; // Authenticate(Ok)
    send_cmd(
        &mut c2,
        &ClientCommand::JoinRoom {
            id: phira_api::RoomId::new("adm-r1".into()).unwrap(),
            monitor: false,
        },
    )
    .await;
    // c2 收到 JoinRoom 响应（此前可能有 OnJoinRoom 广播）
    let mut joined = false;
    for _ in 0..4 {
        if matches!(recv_cmd(&mut c2).await, ServerCommand::JoinRoom(Ok(_))) {
            joined = true;
            break;
        }
    }
    assert!(joined, "user2 应入房");

    // —— 管理员踢 user2 ——
    let resp = http_raw(
        http_addr,
        "POST",
        "/admin/rooms/adm-r1/kick",
        r#"{"user_id": 2}"#,
        Some("Bearer admin-token"),
    )
    .await;
    assert!(
        resp.contains("200 OK") && resp.contains("\"ok\":true"),
        "kick 应成功: {resp}"
    );

    // 房内广播 LeaveRoom(user:2)：c1 和 c2（被踢者本人）都收到
    let mut left_for_c1 = false;
    let mut left_for_c2 = false;
    for _ in 0..4 {
        if matches!(
            recv_cmd(&mut c1).await,
            ServerCommand::Message(phira_api::Message::LeaveRoom { user: 2, .. })
        ) {
            left_for_c1 = true;
            break;
        }
    }
    for _ in 0..4 {
        if matches!(
            recv_cmd(&mut c2).await,
            ServerCommand::Message(phira_api::Message::LeaveRoom { user: 2, .. })
        ) {
            left_for_c2 = true;
            break;
        }
    }
    assert!(left_for_c1, "c1 应收到 user2 被踢的 LeaveRoom");
    assert!(left_for_c2, "被踢者本人也应收到 LeaveRoom（先解析后应用）");

    // 审计可查 + 无 token 401
    let audit = http_raw(
        http_addr,
        "GET",
        "/admin/audit",
        "",
        Some("Bearer admin-token"),
    )
    .await;
    assert!(audit.contains("admin.kick"), "审计应含 kick: {audit}");
    assert!(audit.contains("adm-r1"), "审计带目标房: {audit}");
    let noauth = http_raw(
        http_addr,
        "POST",
        "/admin/rooms/adm-r1/kick",
        r#"{"user_id": 2}"#,
        None,
    )
    .await;
    assert!(
        noauth.contains("401 Unauthorized"),
        "无 token 应 401: {noauth}"
    );
}

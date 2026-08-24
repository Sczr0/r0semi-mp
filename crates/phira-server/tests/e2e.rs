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
    ClientCommand, JoinRoomResponse, RoomConfig, RoomDeps, RoomResponse, ServerCommand, UserInfo,
    Varchar,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::http::{HttpApiClient, HttpAuth, ThreadRngSource};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
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
                r#"{"id": 1, "name": "p1", "language": "zh"}"#
            } else if text.contains("Bearer tok2") {
                r#"{"id": 2, "name": "p2", "language": "zh"}"#
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
    let base = format!("http://{mock_addr}");
    let http = Arc::new(HttpApiClient::new(base.clone()));
    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn phira_api::ApiClient>,
        rng: Arc::new(ThreadRngSource) as Arc<dyn phira_api::RandomSource>,
    };
    let rooms = impl_rooms_v1::RoomsV1::new(RoomConfig { monitors: vec![] }, deps);
    let config = Arc::new(RoomConfig { monitors: vec![] });
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn phira_api::RoomFactory>,
        Arc::clone(&config),
    );

    let (task, registry, fact_tx) = LifecycleTask::new(bus.clone(), Duration::from_secs(10));
    tokio::spawn(task.run());

    let sink = Arc::new(SessionSink::new());
    bus.attach_sink(Arc::clone(&sink) as Arc<dyn phira_core::EventSink>);

    Arc::new(ConnContext {
        bus,
        auth: Arc::new(HttpAuth::new(base.clone())),
        registry,
        fact_tx,
        sink,
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

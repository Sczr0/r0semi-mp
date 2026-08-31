//! 真客户端一致性测试（崩溃猎手，client-behavior-review §5/§8）。
//!
//! 与契约测试的本质区别：**服务端的对端是真 SDK**（`phira-mp-client`，游戏客户端
//! Cargo.toml 锁定的同一 rev cc822df）——"服务端多说话会不会炸客户端"不再靠推理，
//! 直接跑。剧本对应 client-behavior-review.md §5 的 A1–A6 不变式。
//!
//! 运行模型（对齐 e2e.rs 全真组件 + 真客户端持有形态）：`#[tokio::test(flavor = "multi_thread")]`
//! 运行时内 `tokio::spawn` 起服务器；SDK 以 **`Arc<Client>`** 持有（真客户端 panel.rs
//! 就是 `Option<Arc<Client>>`），async 方法直接 await；`blocking_*` 访问器按真客户端
//! "UI 线程调用"姿势经 `spawn_blocking`（Arc 克隆移入——Client 经真客户端验证
//! Send+Sync）。
//!
//! 坑位（实测踩过）：
//! 1. **不能**用默认的 current_thread flavor——`sdk_connect` 的阻塞
//!    `std::net::TcpStream::connect` + `Client::new` 的多任务启动在单线程运行时下
//!    使 `authenticate` 永久挂起（连 SDK 自家 7s 超时都不触发）。
//! 2. **也不能用 2 个 worker**：2026-08 实测 `worker_threads = 2` 下 a1_a5/a6
//!    随机整体僵死——进程 CPU 总计 ~0.016s、所有任务 park（tokio 唤醒丢失，
//!    非协议问题：同样的 2 线程跑"最小协议正确服务器 + 真 SDK"完全正常）。
//!    取 4 与 e2e.rs 的 `worker_threads = 4` 对齐，连续多跑稳定。
//! 3. **禁止** `std::net::TcpStream::connect + from_std`：tokio 1.43+ Linux 拒绝
//!    注册阻塞 fd（issue 7172）→ CI 红本地绿；用 tokio 原生 connect。
//!
//! 许可证：SDK 为 Apache-2.0（与 GPL-3.0 的游戏客户端仓库是两个仓库），dev-dependency
//! 引入无污染（client-behavior-review §9）。

#![allow(clippy::unwrap_used, clippy::expect_used)] // 测试环境允许 unwrap

use std::sync::Arc;
use std::time::Duration;

use phira_api::{RoomConfig, RoomDeps};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_mp_client::Client;
use phira_server::http::{HttpApiClient, HttpAuth, ThreadRngSource};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// —— mock API（按 token 返回身份；/chart/1 返回谱面） ——

/// 绑定好的 mock HTTP 服务器（listener 由调用方预绑定，避免"drop 后重绑端口"竞态）。
async fn mock_api(listener: TcpListener) {
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
            let body = if text.contains("GET /chart/1 ") {
                r#"{"id": 1, "name": "Test Chart"}"#
            } else if text.contains("Bearer tokA") {
                r#"{"id": 101, "name": "hunter-a", "language": "en-US"}"#
            } else if text.contains("Bearer tokB") {
                r#"{"id": 102, "name": "hunter-b", "language": "zh-CN"}"#
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

/// 进程内起完整真实服务器（e2e.rs 同构；随机端口），返回监听地址。
async fn spawn_server() -> std::net::SocketAddr {
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(mock_api(mock_listener));

    let base = format!("http://{mock_addr}");
    let http = Arc::new(HttpApiClient::new_with_timeout(
        base.clone(),
        Duration::from_secs(5),
    ));
    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn phira_api::ApiClient>,
        rng: Arc::new(ThreadRngSource) as Arc<dyn phira_api::RandomSource>,
    };
    let rooms = impl_rooms_v1::RoomsV1::new(
        RoomConfig {
            monitors: vec![102],
        },
        deps,
    );
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn phira_api::RoomFactory>,
        Arc::new(RoomConfig {
            monitors: vec![102],
        }),
    )
    .with_api(Arc::clone(&http) as Arc<dyn phira_api::ApiClient>);

    let (task, registry, fact_tx) = LifecycleTask::new(
        bus.clone(),
        Duration::from_secs(10),
        Duration::from_millis(50),
    );
    tokio::spawn(task.run());

    let sink = Arc::new(SessionSink::new());
    let room_list = Arc::new(phira_server::server::RoomListSink::new(vec![]));
    let composite = Arc::new(phira_server::server::CompositeSink::new(vec![
        Arc::clone(&sink) as Arc<dyn phira_core::EventSink>,
        Arc::clone(&room_list) as Arc<dyn phira_core::EventSink>,
    ]));
    bus.attach_sink(composite as Arc<dyn phira_core::EventSink>);

    let ctx = Arc::new(ConnContext {
        bus,
        auth: Arc::new(HttpAuth::new(base)),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list,
        proxy_protocol: false,
        auth_timeout: Duration::from_secs(10),
        admin_token: None,
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(phira_server::storage::ConfigStore::disabled()),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, peer, ctx).await;
            });
        }
    });
    addr
}

fn rid_ok(s: &str) -> phira_mp_common::RoomId {
    phira_mp_common::RoomId::try_from(s.to_owned()).unwrap()
}

/// 真客户端持有形态：`Arc<Client>`（panel.rs:60 同款）。
///
/// 2026 修：**不用** `std::net::TcpStream::connect + TcpStream::from_std`——tokio
/// 1.43+ 在 Linux 拒绝把阻塞 fd 注册进运行时（`tokio_allow_from_blocking_fd` 检查，
/// github.com/tokio-rs/tokio/issues/7172）；Windows 无此检查所以本地绿、CI 红。
/// 直接 tokio 原生 connect（SDK 的 `Client::new` 收 tokio stream）。
async fn sdk_connect(addr: std::net::SocketAddr) -> Arc<Client> {
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let client = Client::new(stream).await.expect("Client::new 握手");
    Arc::new(client)
}

/// UI 线程姿势的 blocking 访问器（真客户端 panel.rs 直接调；这里经 spawn_blocking）。
async fn sdk_blocking_state(client: &Arc<Client>) -> Option<phira_mp_common::ClientRoomState> {
    let c = Arc::clone(client);
    tokio::task::spawn_blocking(move || c.blocking_state())
        .await
        .unwrap()
}

// —— A2 握手/鉴权顺序 + A3 心跳预算 ——

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2_a3_real_sdk_connect_auth_ping() {
    let addr = spawn_server().await;

    let client = sdk_connect(addr).await;
    // A2/A1：Authenticate oneshot 回调配对——服务端重复响应即 SDK panic
    client
        .authenticate("tokA")
        .await
        .expect("真 SDK 鉴权应成功");

    // A3 预算：Pong 必须 <2s 回来（SDK notify 超时即失败计数）
    let delay = tokio::time::timeout(Duration::from_secs(2), client.ping())
        .await
        .expect("Pong 超 2s 未回（违反 A3 预算）")
        .expect("ping 应成功");
    assert!(delay < Duration::from_secs(2));
    assert_eq!(client.ping_fail_count(), 0);
}

// —— A1 响应唯一性 + A5 重连环路 ——

/// 建房 → 第二人入房 → 断线重连（全新 TCP + 重新鉴权）→ 快照带原房间。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a1_a5_room_lifecycle_and_reconnect_snapshot() {
    let addr = spawn_server().await;

    // 房主 A：建房（响应唯一——重复响应会 panic SDK）
    let host = sdk_connect(addr).await;
    host.authenticate("tokA").await.unwrap();
    host.create_room(rid_ok("conf-r1"))
        .await
        .expect("建房应成功");
    let host_room = tokio::task::spawn_blocking({
        let h = Arc::clone(&host);
        move || h.blocking_room_id().map(|r| r.to_string())
    })
    .await
    .unwrap();
    assert_eq!(host_room.as_deref(), Some("conf-r1"));

    // 玩家 B 入房（10s 兜底：SDK 自家 7s 超时/回环瞬时应远小于此）
    let guest = sdk_connect(addr).await;
    guest.authenticate("tokB").await.unwrap();
    let join_res = tokio::time::timeout(
        Duration::from_secs(10),
        guest.join_room(rid_ok("conf-r1"), false),
    )
    .await;
    join_res.expect("入房应成功").expect("入房应成功");
    let joined = sdk_blocking_state(&guest).await.expect("入房后应有房态");
    assert_eq!(joined.users.len(), 2);

    // B 断开（Drop 关闭 TCP）→ 新连接重连+重新鉴权 → 快照应带回原房间（A5）
    drop(guest);
    tokio::time::sleep(Duration::from_millis(300)).await; // 服务端处理断线

    let guest2 = sdk_connect(addr).await;
    guest2.authenticate("tokB").await.unwrap();
    // 协议无独立 GetClientState：房间快照随鉴权响应返回（真客户端重连环路）
    let snap = sdk_blocking_state(&guest2)
        .await
        .expect("重连鉴权应带回房间快照（A5）");
    assert_eq!(snap.id.to_string(), "conf-r1", "快照应是原房间");
    assert_eq!(snap.users.len(), 2, "座位应保留");
}

// —— A6 字节级对称 ——

/// 触发各类 Message 广播后双方心跳仍健康（recv 未因坏帧退出 = 解码全通过）；
/// LockRoom 推送被真客户端应用（locked=true 可见）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a6_broadcasts_decode_by_real_sdk() {
    let addr = spawn_server().await;

    let host = sdk_connect(addr).await;
    host.authenticate("tokA").await.unwrap();
    host.create_room(rid_ok("conf-x")).await.unwrap();

    let guest = sdk_connect(addr).await;
    guest.authenticate("tokB").await.unwrap();
    guest
        .join_room(rid_ok("conf-x"), true)
        .await
        .expect("monitor 加入");

    // 广播面：选图 / 锁房 / 循环切换
    host.select_chart(1).await.unwrap();
    host.lock_room(true).await.unwrap();
    host.cycle_room(false).await.unwrap();

    // 广播消化后双方心跳仍健康（recv 任务未因坏帧退出 = 解码全通过）
    tokio::time::timeout(Duration::from_secs(2), host.ping())
        .await
        .expect("host ping 超时")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), guest.ping())
        .await
        .expect("guest ping 超时")
        .unwrap();

    // LockRoom 推送被真客户端应用（locked=true 可见）
    let st = sdk_blocking_state(&guest).await.expect("guest 应仍在房间");
    assert!(st.locked, "guest 视角的 locked 应为 true");
}

// —— 断言库雏形（P4，client-conformance.md 五步规划步骤 2 第一笔） ——
//
// 对抗性序列 × 真 SDK 的可复用断言。当前一条：A2 负向注入——服务端绝不向
// 未入房用户推送房间事件（Message::{LockRoom,CycleRoom,LeaveRoom}、ChangeState、
// ChangeHost，无房 → 真客户端 panic，lib.rs:453-483）。确定性断言（窗口内缺席），
// 不依赖时序，非 flaky。后续对抗性场景（并发 LockRoom 竞态等）复用同一辅助扩展。

/// A2 负向注入：断言 `client` 在 `window` 内未收到任何房间推送（连接健康、无房态）。
///
/// 观察面与 SDK 事实对齐：`take_messages` 只暴露 `Message`（LockRoom/CycleRoom/LeaveRoom）；
/// `ChangeState`/`ChangeHost` 是 `ServerCommand` 变体（无公共读取面），由其副作用覆盖——
/// 若服务端向未入房用户推它们，SDK 处理时踩裸 unwrap panic（lib.rs:453-483）→ recv 任务
/// 退出 → 心跳失败，被 ping 断言捕获。三者合一即 A2 全量。
async fn assert_no_room_push_to_unjoined(
    client: &Arc<phira_mp_client::Client>,
    window: Duration,
    label: &str,
) {
    use phira_mp_common::Message;
    tokio::time::sleep(window).await;
    // 1) Message 层：LockRoom/CycleRoom/LeaveRoom 推送必须缺席
    //    （blocking_* 访问器须经 spawn_blocking——真客户端"UI 线程"姿势，同 sdk_blocking_state）
    let c = Arc::clone(client);
    let msgs = tokio::task::spawn_blocking(move || c.blocking_take_messages())
        .await
        .expect("take_messages 任务 panicked");
    for m in &msgs {
        assert!(
            !matches!(
                m,
                Message::LockRoom { .. } | Message::CycleRoom { .. } | Message::LeaveRoom { .. }
            ),
            "{label}: 未入房用户收到房间推送 {m:?}（A2 违例）"
        );
    }
    // 2) 状态层：从未进入房间（ChangeState/ChangeHost 未越过房间边界）
    let st = sdk_blocking_state(client).await;
    assert!(
        st.is_none(),
        "{label}: 未入房用户状态应保持 None（A2 违例）"
    );
    // 3) 心跳层：recv 任务未因坏帧/panic 退出（ChangeState/ChangeHost 越权的副作用面）
    tokio::time::timeout(Duration::from_secs(2), client.ping())
        .await
        .expect("{label}: ping 超时（A2 违例）")
        .unwrap();
}

/// A2 负向注入（真 SDK）：鉴权后不入房，静置窗口内绝不能收到房间推送——
/// 服务端若在用户未入房时推 LockRoom/ChangeState 等，真客户端会踩裸 unwrap panic
/// （client-behavior-review §5 A2）。这是"服务端多说话就出事"的第一条可执行断言。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2_no_room_push_to_unjoined_user() {
    let addr = spawn_server().await;
    let guest = sdk_connect(addr).await;
    guest.authenticate("tokB").await.unwrap();
    // 不入房，静置 2s——服务端若有越界推送必在窗口内到达
    assert_no_room_push_to_unjoined(&guest, Duration::from_secs(2), "未入房 guest").await;
}

/// A2 正向基线（对照）：入房用户应正常收到房间事件（锁房推送被应用）——
/// 证明上面的负向断言不是"服务端从不推任何东西"，而是精确地"只对未入房者禁推"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2_in_room_user_gets_pushes() {
    let addr = spawn_server().await;
    let host = sdk_connect(addr).await;
    host.authenticate("tokA").await.unwrap();
    host.create_room(rid_ok("conf-x")).await.unwrap();
    let guest = sdk_connect(addr).await;
    guest.authenticate("tokB").await.unwrap();
    guest
        .join_room(rid_ok("conf-x"), true)
        .await
        .expect("monitor 加入");

    host.lock_room(true).await.unwrap();
    let st = sdk_blocking_state(&guest).await.expect("guest 应仍在房间");
    assert!(st.locked, "入房用户应收到锁房推送（正向基线）");
}

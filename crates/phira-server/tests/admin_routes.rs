//! 管理写面路由全分支测试（TOCTOU 无——http_serve 无状态翻译层）：
//! POST /admin/rooms/{id}/broadcast、/admin/users/{id}/disconnect、
//! /admin/rooms/{id}/kick、/admin/users/{id}/ban、/admin/observers
//! + 请求层错误（400/404/405/409/413）与审计落环。
//!
//! healthz.rs 已覆盖读面 + kick 成功路径；本文件补齐写面剩余分支
//! （broadcast 全路径、disconnect 全路径、kick/ban/observers 错误分支、
//! 413 拒读、分体 POST 读余量）。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, RoomCommand, RoomConfig, RoomError,
    RoomErrorCode, RoomEvent, RoomFactory, RoomId, RoomResponse, UserIdentity, Varchar,
    encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::admin::http_serve;
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rid() -> RoomId {
    RoomId::new("r".to_owned()).unwrap()
}

/// 有状态的房间 actor（镜像 impl-rooms-v1 的用户簿语义，供管理命令分支测试）：
/// - CreateRoom：房主入簿
/// - JoinRoom：用户入簿
/// - AdminKick：不在簿 → Business(NotInRoom)；在簿 → 出簿 + Ok
/// - AdminBroadcast：系统 Chat（user=0）广播 + Ok
struct BookActor {
    users: HashSet<i32>,
}

#[async_trait::async_trait]
impl phira_api::RoomActor for BookActor {
    async fn handle(
        &mut self,
        ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        match cmd {
            RoomCommand::CreateRoom { .. } => {
                self.users.insert(1);
                (
                    Some(RoomResponse::Ok),
                    vec![RoomEvent::RoomCreated {
                        room_id: rid(),
                        host: 1,
                    }],
                )
            }
            RoomCommand::JoinRoom { .. } => {
                let phira_api::Origin::Client { user_id } = ctx.origin else {
                    return (None, Vec::new());
                };
                self.users.insert(user_id);
                (
                    Some(RoomResponse::Ok),
                    vec![RoomEvent::UserJoined {
                        room_id: rid(),
                        user: phira_api::UserInfo {
                            id: user_id,
                            name: "p2".to_owned(),
                            monitor: false,
                        },
                    }],
                )
            }
            RoomCommand::AdminKick { user_id } => {
                if self.users.remove(&user_id) {
                    (Some(RoomResponse::Ok), Vec::new())
                } else {
                    (
                        Some(RoomResponse::Failure(RoomError::Business {
                            code: RoomErrorCode::NotInRoom,
                            msg: "not in room".to_owned(),
                        })),
                        Vec::new(),
                    )
                }
            }
            RoomCommand::AdminBroadcast { content } => (
                Some(RoomResponse::Ok),
                vec![RoomEvent::Chat {
                    room_id: rid(),
                    user: 0,
                    content,
                }],
            ),
            // 其余命令：测试替身不感兴趣，零副作用应答
            _ => (None, Vec::new()),
        }
    }
}

struct BookFactory;

impl RoomFactory for BookFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(BookActor {
            users: HashSet::new(),
        })
    }
}

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "admin".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

fn test_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(BookFactory);
    let bus = Bus::new(
        factory as Arc<dyn RoomFactory>,
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
        auth: Arc::new(AuthOk),
        registry,
        fact_tx,
        sink,
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
        proxy_protocol: false,
        auth_timeout: Duration::from_secs(10),
        admin_token: Some("test-token".to_owned()),
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(phira_server::storage::ConfigStore::disabled()),
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

async fn spawn_mp(ctx: Arc<ConnContext>) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let ctx = Arc::clone(&ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).await.expect("读鉴权响应");
    assert!(n > 0, "鉴权应成功");
    client
}

/// 建 MP 连接并建房，返回连接（读端由调用方消费）。
async fn create_room_client(ctx: Arc<ConnContext>) -> TcpStream {
    let mut client = spawn_mp(ctx).await;
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    client
}

/// 向 http_serve 发一个 HTTP 请求（可拆两段发 body 以覆盖 read_exact 余量路径）。
async fn http_raw(ctx: &Arc<ConnContext>, req: &[u8], split_body_at: Option<usize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = Arc::clone(ctx);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        http_serve(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    match split_body_at {
        Some(at) => {
            client.write_all(&req[..at]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            client.write_all(&req[at..]).await.unwrap();
        }
        None => client.write_all(req).await.unwrap(),
    }
    let mut resp = Vec::new();
    client.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

async fn http_admin(ctx: &Arc<ConnContext>, method: &str, path: &str, body: &str) -> String {
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\nAuthorization: Bearer test-token\r\n\r\n{body}",
        body.len()
    );
    if body.is_empty() {
        req = format!(
            "{method} {path} HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer test-token\r\n\r\n"
        );
    }
    http_raw(ctx, req.as_bytes(), None).await
}

fn audit_actions(ctx: &Arc<ConnContext>) -> Vec<String> {
    ctx.admin_audit
        .snapshot()
        .into_iter()
        .map(|e| e.action)
        .collect()
}

// —— broadcast 全路径 ——

#[tokio::test]
async fn admin_broadcast_ok_delivers_and_audits() {
    let ctx = test_ctx();
    let mut client = create_room_client(Arc::clone(&ctx)).await;
    let resp = http_admin(
        &ctx,
        "POST",
        "/admin/rooms/r/broadcast",
        r#"{"content":"hello"}"#,
    )
    .await;
    assert!(resp.contains("200 OK"), "广播应成功: {resp}");
    assert!(resp.contains(r#""ok":true"#), "ok=true: {resp}");
    assert!(
        audit_actions(&ctx).contains(&"admin.broadcast".to_owned()),
        "广播必须落审计: {:?}",
        audit_actions(&ctx)
    );
    // 房内用户应收到系统 Chat（user=0）
    let mut buf = [0u8; 4096];
    let got = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
        .await
        .expect("应在超时前收到公告");
    let text = String::from_utf8_lossy(&buf[..got.unwrap_or(0)]);
    assert!(text.contains("hello"), "公告应投递到房内: {text}");
}

#[tokio::test]
async fn admin_broadcast_unknown_room_404() {
    let ctx = test_ctx();
    let resp = http_admin(
        &ctx,
        "POST",
        "/admin/rooms/nope/broadcast",
        r#"{"content":"x"}"#,
    )
    .await;
    assert!(resp.contains("404 Not Found"), "未知房间应 404: {resp}");
    assert!(
        audit_actions(&ctx).contains(&"admin.broadcast".to_owned()),
        "失败也要落审计"
    );
}

#[tokio::test]
async fn admin_broadcast_missing_content_400() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/rooms/r/broadcast", "{}").await;
    assert!(
        resp.contains("400 Bad Request"),
        "缺 content 应 400: {resp}"
    );
}

#[tokio::test]
async fn admin_broadcast_bad_room_id_400() {
    let ctx = test_ctx();
    let resp = http_admin(
        &ctx,
        "POST",
        "/admin/rooms/!!/broadcast",
        r#"{"content":"x"}"#,
    )
    .await;
    assert!(
        resp.contains("400 Bad Request"),
        "非法房间 id 应 400: {resp}"
    );
}

// —— disconnect 全路径 ——

#[tokio::test]
async fn admin_disconnect_online_user_kicks_tcp() {
    let ctx = test_ctx();
    let mut client = create_room_client(Arc::clone(&ctx)).await;
    let resp = http_admin(&ctx, "POST", "/admin/users/1/disconnect", "").await;
    assert!(resp.contains("200 OK"), "在线用户断连应 200: {resp}");
    assert!(
        audit_actions(&ctx).contains(&"admin.disconnect".to_owned()),
        "断连须落审计"
    );
    // 客户端应被断：kicker 1s 轮询执行拆除，先排空在途数据，直到 EOF/RST
    let mut buf = [0u8; 1024];
    let r = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(r.is_ok(), "断连后连接应被拆除（读超时 = 未踢）");
}

#[tokio::test]
async fn admin_disconnect_offline_user_404() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/users/2/disconnect", "").await;
    assert!(resp.contains("404 Not Found"), "离线用户应 404: {resp}");
}

#[tokio::test]
async fn admin_disconnect_bad_user_id_400() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/users/abc/disconnect", "").await;
    assert!(
        resp.contains("400 Bad Request"),
        "非法 user id 应 400: {resp}"
    );
}

// —— kick 错误分支（成功路径 healthz 已覆盖） ——

#[tokio::test]
async fn admin_kick_missing_user_id_400() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/rooms/r/kick", "{}").await;
    assert!(
        resp.contains("400 Bad Request"),
        "缺 user_id 应 400: {resp}"
    );
}

#[tokio::test]
async fn admin_kick_bad_room_id_400() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/rooms/!!/kick", r#"{"user_id":1}"#).await;
    assert!(
        resp.contains("400 Bad Request"),
        "非法房间 id 应 400: {resp}"
    );
}

#[tokio::test]
async fn admin_kick_user_not_in_room_409() {
    let ctx = test_ctx();
    let _client = create_room_client(Arc::clone(&ctx)).await;
    let resp = http_admin(&ctx, "POST", "/admin/rooms/r/kick", r#"{"user_id":2}"#).await;
    assert!(resp.contains("409 Conflict"), "不在房用户应 409: {resp}");
    assert!(resp.contains("not_in_room"), "错误码: {resp}");
}

#[tokio::test]
async fn admin_kick_ok_removes_user() {
    let ctx = test_ctx();
    // 用户 2 建房（user_id 由 AuthHandler 决定 = 1；改用 JoinRoom 由第二个连接加入失败——
    // AuthOk 恒返回 user 1，故"第二个用户"不可得；本测试验证踢在簿用户成功路径
    //（BookActor：CreateRoom 后 host=1 在簿 → AdminKick user=1 → Ok）。
    let _client = create_room_client(Arc::clone(&ctx)).await;
    let resp = http_admin(&ctx, "POST", "/admin/rooms/r/kick", r#"{"user_id":1}"#).await;
    assert!(resp.contains("200 OK"), "在簿用户踢出应 200: {resp}");
}

// —— ban / observers 错误分支 ——

#[tokio::test]
async fn admin_ban_bad_user_id_400() {
    let ctx = test_ctx();
    let resp = http_admin(&ctx, "POST", "/admin/users/xyz/ban", "").await;
    assert!(
        resp.contains("400 Bad Request"),
        "非法 user id 应 400: {resp}"
    );
}

#[tokio::test]
async fn admin_observers_error_branches() {
    let ctx = test_ctx();
    let missing_kind = http_admin(&ctx, "POST", "/admin/observers", r#"{"op":"add"}"#).await;
    assert!(missing_kind.contains("400 Bad Request"), "缺 kind 应 400");
    let missing_op = http_admin(&ctx, "POST", "/admin/observers", r#"{"kind":"ban"}"#).await;
    assert!(missing_op.contains("400 Bad Request"), "缺 op 应 400");
    let bad_op = http_admin(
        &ctx,
        "POST",
        "/admin/observers",
        r#"{"kind":"ban","op":"nope"}"#,
    )
    .await;
    assert!(bad_op.contains("400 Bad Request"), "非法 op 应 400");
    let bad_kind = http_admin(
        &ctx,
        "POST",
        "/admin/observers",
        r#"{"kind":"nope","op":"add"}"#,
    )
    .await;
    assert!(bad_kind.contains("400 Bad Request"), "非法 kind 应 400");
}

// —— 请求层 ——

#[tokio::test]
async fn admin_unsupported_endpoints() {
    let ctx = test_ctx();
    let post = http_admin(&ctx, "POST", "/admin/nope", "").await;
    assert!(
        post.contains("405 Method Not Allowed"),
        "未知写端点应 405: {post}"
    );
    let get = http_admin(&ctx, "GET", "/admin/nope", "").await;
    assert!(get.contains("404 Not Found"), "未知读端点应 404: {get}");
    // GET 打到写端点（kick）→ 读路由按房间详情处理 → 404（设计如此：方法即路由第 1 维）
    let get_kick = http_admin(&ctx, "GET", "/admin/rooms/r/kick", "").await;
    assert!(
        get_kick.contains("404 Not Found"),
        "GET kick 应 404（读路由兜底）: {get_kick}"
    );
}

#[tokio::test]
async fn admin_body_too_large_413_without_reading() {
    let ctx = test_ctx();
    // 只发头（Content-Length 声明 2000 > 1KiB 上限）——服务端按声明拒读，体无需到达；
    // 若真把 2000B 体发出去，服务端拒读时 receive buffer 未读 → close 带 RST，响应被丢弃。
    let req = "POST /admin/rooms/r/kick HTTP/1.1\r\nHost: test\r\nContent-Length: 2000\r\nAuthorization: Bearer test-token\r\n\r\n";
    // 拒读路径：服务端读到 Content-Length 即回 413 并关闭
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_ctx = Arc::clone(&ctx);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        http_serve(stream, addr, srv_ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let r = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf)).await;
        if matches!(r, Ok(Ok(0) | Err(_)) | Err(_)) {
            break;
        }
        if let Ok(Ok(n)) = r {
            resp.extend_from_slice(&buf[..n]);
        }
    }
    let resp = String::from_utf8_lossy(&resp);
    assert!(
        resp.contains("413 Payload Too Large"),
        "超限体应 413: {resp}"
    );
}

#[tokio::test]
async fn admin_post_body_read_exact_rest_on_split_send() {
    let ctx = test_ctx();
    // 头 + 部分体一段，剩余体一段：覆盖 http_serve 的 read_exact 补读路径
    let _client = create_room_client(Arc::clone(&ctx)).await;
    let body = r#"{"content":"split-body-test"}"#;
    let head = format!(
        "POST /admin/rooms/r/broadcast HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\nAuthorization: Bearer test-token\r\n\r\n",
        body.len()
    );
    let mut req = head.into_bytes();
    req.extend_from_slice(body.as_bytes());
    let resp = http_raw(&ctx, &req, Some(req.len() - 7)).await;
    assert!(resp.contains("200 OK"), "分体 POST 应成功: {resp}");
}

//! Server 启停全链路测试（组合根视角）：
//!
//! - `Server::run_with_shutdown`：accept_loop 真实跑起来（MP 握手 + 鉴权）、
//!   admin 监听端口可服务（/healthz）、停机注入 → 维护通知广播 + 宽限 + 退出
//! - 覆盖点（此前全 0 命中）：`Server::new` 的 http_port 分支、`accept_loop`
//!   主体、`run_with_shutdown` 的停机分支、`http_accept_loop` 主体

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, Server, SessionSink};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 抢占式拿一个空闲端口（bind → 取址 → 释放；竞态窗口可接受——测试专用）。
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
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
        _ctx: phira_api::CmdCtx,
        _cmd: phira_api::RoomCommand,
    ) -> (Option<phira_api::RoomResponse>, Vec<RoomEvent>) {
        (None, Vec::new())
    }
}

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "boot".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

fn test_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(NoopFactory);
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
        admin_token: None,
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

/// 全链路：Server::new（含 admin 监听）→ run_with_shutdown →
/// MP 握手鉴权 / admin /healthz 可达 → 触发停机 → 宽限后返回。
#[tokio::test]
async fn server_boot_serves_mp_and_http_then_shuts_down_gracefully() {
    let mp_port = free_port().await;
    let admin_port = free_port().await;
    let ctx = test_ctx();
    let server = Server::new(
        std::net::SocketAddr::from(([127, 0, 0, 1], mp_port)),
        (*ctx).clone(),
        "maintenance soon".to_owned(),
        Duration::from_millis(300),
        Some(admin_port),
    )
    .await
    .expect("Server::new 应成功（含 admin 监听）");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(server.run_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));

    // MP 入口：握手 + 鉴权（accept_loop → handle_connection 全链路）
    let mut client = TcpStream::connect(("127.0.0.1", mp_port)).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.expect("读鉴权响应");
    assert!(n > 0, "鉴权应成功返回");

    // admin 监听端口：/healthz 可达（http_accept_loop → http_serve 全链路）
    let mut http = TcpStream::connect(("127.0.0.1", admin_port)).await.unwrap();
    http.write_all(b"GET /healthz HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    let mut health = Vec::new();
    http.read_to_end(&mut health).await.unwrap();
    let health = String::from_utf8_lossy(&health);
    assert!(health.contains("200 OK"), "healthz: {health}");
    assert!(health.contains("\"status\":\"ok\""), "status ok: {health}");

    // 触发停机：维护通知广播 → 宽限 300ms → run 返回
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("run_with_shutdown 应在超时前退出")
        .expect("run_with_shutdown 应 Ok")
        .expect("run 返回 Ok");
}

/// 优雅停机时在线的客户端应收维护通知（系统 Chat, user=0）。
#[tokio::test]
async fn maintenance_notice_reaches_connected_clients() {
    let mp_port = free_port().await;
    let ctx = test_ctx();
    let server = Server::new(
        std::net::SocketAddr::from(([127, 0, 0, 1], mp_port)),
        (*ctx).clone(),
        "server going down".to_owned(),
        Duration::from_millis(500),
        None,
    )
    .await
    .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(server.run_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));

    let mut client = TcpStream::connect(("127.0.0.1", mp_port)).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.expect("读鉴权响应");
    assert!(n > 0);

    // 触发停机 → 维护通知应投递到在线会话
    shutdown_tx.send(()).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), async {
        let mut all = Vec::new();
        // 通知到达即停（连接不复用/不关闭——维护广播后 TCP 仍活）
        while !String::from_utf8_lossy(&all).contains("server going down") {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => all.extend_from_slice(&buf[..n]),
            }
        }
        all
    })
    .await
    .expect("读维护通知");
    let text = String::from_utf8_lossy(&got);
    assert!(
        text.contains("server going down"),
        "客户端应收到维护通知: {text:?}"
    );

    let _ = server_task.await;
}

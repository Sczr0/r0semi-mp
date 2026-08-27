//! ISSUE-0008 回归测试：`Server::run` 必须持续运行直到停机信号。
//!
//! 修复前：`tokio::select!` 任一分支完成即退出——`http_port` 未配置时
//! `http_accept_loop(None)` 立即返回 → **默认配置下服务器启动即退出**（上线即崩）。
//! 修复：accept 循环放后台任务，shutdown 是唯一退出路径。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    RoomResponse, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, Server, SessionSink};
use tokio::net::TcpListener;

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
            msg: "no".to_owned(),
        })
    }
}

fn test_ctx() -> ConnContext {
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
    ConnContext {
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
    }
}

/// 默认配置（`http_port = None`）：run() 必须持续运行，不得立即退出（ISSUE-0008）。
#[tokio::test]
async fn server_run_stays_alive_without_http_port() {
    // 先占一个端口再让出（拿可用地址；Server::new 会重新 bind）
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let server = Server::new(
        addr,
        test_ctx(),
        String::new(),
        Duration::from_secs(1),
        None,
    )
    .await
    .unwrap();
    // run() 不应退出：timeout 后仍运行 = 修复生效（修复前 http_accept_loop(None) 立即返回 → run 立即返回）
    let result = tokio::time::timeout(Duration::from_millis(400), server.run()).await;
    assert!(
        result.is_err(),
        "run() 应持续运行（http_port=None 时也不得退出，ISSUE-0008）: {result:?}"
    );
    // 500ms 内服务可连接（accept 循环在跑）
    let _ = tokio::time::timeout(Duration::from_secs(2), tcp_stream_connect(&addr)).await;
}

/// 连接探测（timeout 内尝试连上即证明 accept 活着；连上后立即断开）。
async fn tcp_stream_connect(addr: &std::net::SocketAddr) {
    use tokio::io::AsyncWriteExt;
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(mut s) => {
                let _ = s.write_all(&[1]).await; // 版本字节
                return;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

//! C-02 异常断连遥测的**接线**验证（单元级 `reset_telemetry_*` 已有：
//! 计数/窗口/有界表；本文件验证 handle_connection 收尾把真实 TCP RST
//! 落到暗线遥测 + 审计（`reset_flood` ≥10 次/5min 告警））。
//!
//! RST 制造：SO_LINGER=0 关闭（丢弃未读数据 → 对端收到 RST）。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::AsyncWriteExt;
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
        _ctx: phira_api::CmdCtx,
        _cmd1: phira_api::RoomCommand,
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
            name: "rst".to_owned(),
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

/// 建连 → 发版本 + 鉴权帧 → 等服务端响应到达本端（未读）→ 关闭。
/// 关闭时本端 receive buffer 内有未读数据 → Windows/Linux 均发 RST
/// （服务端读侧报 ECONNRESET，C-02 遥测来源）。
async fn rst_connection(addr: std::net::SocketAddr) -> std::io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(&[PROTOCOL_VERSION]).await?;
    let mut auth = Vec::new();
    encode_packet(
        &ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        },
        &mut auth,
    );
    stream.write_all(&frame(&auth)).await?;
    // 轮询 peek（不消费）等响应到达——响应在缓冲时关闭必发 RST
    let mut probe = [0u8; 16];
    let mut waited = 0u32;
    loop {
        if stream.peek(&mut probe).await.unwrap_or(0) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        waited += 1;
        if waited > 300 {
            return Err(std::io::Error::other("server response never arrived"));
        }
    }
    drop(stream); // 未读响应在缓冲 → close 发 RST
    Ok(())
}

/// ≥10 次同一 IP 的 ECONNRESET（5min 窗口内）→ 暗线计数 + audit `reset_flood`。
#[tokio::test]
async fn reset_flood_alerts_and_audits_after_threshold() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_ctx = test_ctx();
    let ctx = Arc::clone(&srv_ctx);
    tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let ctx = Arc::clone(&srv_ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 阈值 10 + 余量 3（服务端须在窗口内收到全部 RST；个别连接可能被准入/其他路径吃掉）
    for _ in 0..13 {
        rst_connection(addr).await.expect("RST 连接应建立");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 等待服务端收尾（读侧报错 → close_cause → 遥测 + 审计）
    let deadline = Duration::from_secs(5);
    let found = tokio::time::timeout(deadline, async {
        loop {
            let actions: Vec<String> = ctx
                .admin_audit
                .snapshot()
                .into_iter()
                .map(|e| e.action)
                .collect();
            if actions.contains(&"reset_flood".to_owned()) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        found.unwrap_or(false),
        "≥10 次 RST 后应产生 reset_flood 审计条目"
    );
}

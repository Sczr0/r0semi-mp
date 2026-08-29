//! 管理面持久化测试（组合根 storage，docs/admin-api.md §持久化）：
//! audit 归档回填 / ban 名单跨重启 / config 快照与回滚 / 损坏容忍（fail soft）。
//!
//! harness 与 healthz.rs 同款：直连 `http_serve`，不经 TCP 协议面。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    RoomResponse, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::admin::http_serve;
use phira_server::server::{ConnContext, SessionSink};
use phira_server::storage::ConfigStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "h".to_owned(),
            lang: "zh".to_owned(),
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
        (Some(RoomResponse::Ok), Vec::new())
    }
}

/// ctx 工厂（persist_dir 注入三个持久化对象；backfill = 启动回填的 rollback 快照）。
fn test_ctx_with_persist(dir: &std::path::Path) -> Arc<ConnContext> {
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
        admin_audit: phira_server::admin::AuditLog::new_with_file(dir),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new_with_file(dir),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(ConfigStore::new(dir)),
    })
}

/// 带管理 Bearer 认证的请求（/admin/* 全部端点需要）。
async fn http_admin(ctx: Arc<ConnContext>, method: &str, path: &str, body: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        http_serve(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let req = if method == "POST" {
        format!(
            "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\nAuthorization: Bearer test-token\r\n\r\n{body}",
            body.len()
        )
    } else {
        format!("GET {path} HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer test-token\r\n\r\n")
    };
    client.write_all(req.as_bytes()).await.unwrap();
    let mut resp = String::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = client.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        resp.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    resp
}

/// 隔离的临时持久化目录（Drop 时清理；tag 区分并行测试）。
struct TempPersistDir(std::path::PathBuf);

impl TempPersistDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("r0semi-persist-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("临时持久化目录应可创建");
        Self(dir)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempPersistDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 审计归档：写操作同步落盘 → 重启（同目录新 ctx）回填环，GET /admin/audit 含历史。
#[tokio::test]
async fn audit_archived_and_backfilled_after_restart() {
    let dir = TempPersistDir::new("audit");

    // 第一生命周期：两次写操作（ban 77 + kick 不存在房间——后者仍记审计）
    let ctx1 = test_ctx_with_persist(dir.path());
    let r1 = http_admin(ctx1.clone(), "POST", "/admin/users/77/ban", "").await;
    assert!(r1.starts_with("HTTP/1.1 200"), "ban 应成功: {r1}");
    let r2 = http_admin(
        ctx1.clone(),
        "POST",
        "/admin/rooms/r1/kick",
        "{\"user_id\":5}",
    )
    .await;
    // 业务失败形态（房间不存在 404 / 用户不在房 not_in_room）——但都必须记审计
    assert!(
        r2.contains("404") || r2.contains("not_in_room"),
        "kick 应业务失败（404/not_in_room）但仍记审计: {r2}"
    );
    drop(ctx1);

    // 归档文件应有两行（写操作无论成败都记）
    let audit_file = dir.path().join("audit.jsonl");
    let lines = std::fs::read_to_string(&audit_file).expect("audit.jsonl 应已生成");
    assert_eq!(lines.lines().count(), 2, "两次写操作 → 两行归档: {lines}");

    // 第二生命周期（重启）：回填至多 256 行，历史可查
    let ctx2 = test_ctx_with_persist(dir.path());
    let resp = http_admin(ctx2, "GET", "/admin/audit", "").await;
    assert!(resp.contains("admin.ban"), "重启后审计应含 ban: {resp}");
    assert!(resp.contains("77"), "重启后审计应含目标: {resp}");
}

/// ban 名单跨重启：落盘 → 重启加载 → 观察者仍拦（名单 = 组合根真实源）。
#[tokio::test]
async fn ban_list_persisted_across_restart() {
    let dir = TempPersistDir::new("ban");

    let ctx1 = test_ctx_with_persist(dir.path());
    let resp = http_admin(ctx1.clone(), "POST", "/admin/users/77/ban", "").await;
    assert!(resp.starts_with("HTTP/1.1 200"), "ban 应成功: {resp}");
    drop(ctx1);

    // 文件应含 77
    let bans_file = dir.path().join("bans.json");
    let text = std::fs::read_to_string(&bans_file).expect("bans.json 应已生成");
    assert!(text.contains("77"), "bans.json 应含 77: {text}");

    // 重启：new_with_file 加载名单 → 观察者仍持有；挂载后管理面展示一致
    let ctx2 = test_ctx_with_persist(dir.path());
    assert_eq!(
        ctx2.admin_ban_observer.banned_users(),
        vec![77],
        "重启后名单应加载"
    );
    let resp = http_admin(
        ctx2,
        "POST",
        "/admin/observers",
        r#"{"kind":"ban","op":"add"}"#,
    )
    .await;
    assert!(resp.contains("77"), "挂载后 banned 列表应含 77: {resp}");
}

/// config 快照：record_success 两级落盘 → load 还原 → rollback 交换（文件层与内存层同语义）。
#[tokio::test]
async fn config_snapshot_and_rollback_files() {
    let dir = TempPersistDir::new("config");
    let store = ConfigStore::new(dir.path());

    // 首次写：无 last（首次=无上一份）
    store.record_success(&serde_json::json!({"rooms": {"monitors": [2]}}));
    let (current, last) = store.load();
    let cur = phira_server::storage::config_from_json(&current.unwrap()).unwrap();
    assert_eq!(cur.monitors, vec![2], "current 应还原 monitors=[2]");
    assert!(last.is_none(), "首次写无 last");

    // 二次写：旧 current → last
    store.record_success(&serde_json::json!({"rooms": {"monitors": [9, 3]}}));
    let (current, last) = store.load();
    assert_eq!(
        phira_server::storage::config_from_json(&current.unwrap())
            .unwrap()
            .monitors,
        vec![9, 3]
    );
    assert_eq!(
        phira_server::storage::config_from_json(&last.unwrap())
            .unwrap()
            .monitors,
        vec![2],
        "last 应保留上一份 [2]"
    );

    // rollback：last → current；last 清空（回切一次语义）
    store.record_rollback();
    let (current, last) = store.load();
    assert_eq!(
        phira_server::storage::config_from_json(&current.unwrap())
            .unwrap()
            .monitors,
        vec![2],
        "rollback 后 current 应回到 [2]"
    );
    assert!(last.is_none(), "rollback 后 last 应清空");
    assert!(
        !dir.path().join("config.last.json").exists(),
        "rollback 后 last 文件应删除"
    );
}

/// 重启后 rollback 仍可用：main.rs 启动序列（load → config_from_json → stash）后
/// take_last 应取回上一份——"重启再 rollback"是持久化快照的核心价值。
#[tokio::test]
async fn rollback_available_after_restart() {
    let dir = TempPersistDir::new("rollback");
    let store = ConfigStore::new(dir.path());
    store.record_success(&serde_json::json!({"rooms": {"monitors": [2]}}));
    store.record_success(&serde_json::json!({"rooms": {"monitors": [7]}}));

    // 模拟 main.rs 启动序列
    let (_, last) = store.load();
    let state = phira_server::admin::AdminConfigState::new();
    if let Some(last) = last {
        let rc = phira_server::storage::config_from_json(&last).unwrap();
        state.stash(Arc::new(rc));
    }
    let taken = state.take_last().expect("重启后应有上一份可回滚");
    assert_eq!(taken.monitors, vec![2], "重启后 rollback 目标 = [2]");
    assert!(state.take_last().is_none(), "回切一次后清空（语义不变）");
}

/// HTTP 端到端：POST config → rollback，文件与运行时一致。
#[tokio::test]
async fn config_http_roundtrip_with_rollback() {
    let dir = TempPersistDir::new("config-http");
    let ctx = test_ctx_with_persist(dir.path());

    let resp = http_admin(
        ctx.clone(),
        "POST",
        "/admin/config",
        r#"{"rooms": {"monitors": [2]}}"#,
    )
    .await;
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "设置 config 应成功: {resp}"
    );
    let resp = http_admin(
        ctx.clone(),
        "POST",
        "/admin/config",
        r#"{"rooms": {"monitors": [9]}}"#,
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 200"), "再次设置应成功: {resp}");

    let resp = http_admin(ctx, "POST", "/admin/config/rollback", "").await;
    assert!(resp.starts_with("HTTP/1.1 200"), "rollback 应成功: {resp}");

    let (current, last) = ConfigStore::new(dir.path()).load();
    assert_eq!(
        phira_server::storage::config_from_json(&current.unwrap())
            .unwrap()
            .monitors,
        vec![2],
        "HTTP rollback 后 current 文件应回到 [2]"
    );
    assert!(last.is_none(), "HTTP rollback 后 last 应清空");
}

/// 损坏容忍（fail soft）：bans.json 写垃圾 → 启动不崩、空名单加载、告警。
#[tokio::test]
async fn corrupt_bans_file_fails_open() {
    let dir = TempPersistDir::new("corrupt");
    std::fs::write(dir.path().join("bans.json"), "not-json-at-all").unwrap();

    let ctx = test_ctx_with_persist(dir.path());
    assert!(
        ctx.admin_ban_observer.banned_users().is_empty(),
        "损坏名单应按空加载（fail soft）"
    );
    // 空名单下 ban 仍可用（写盘覆盖损坏内容）
    let resp = http_admin(ctx, "POST", "/admin/users/42/ban", "").await;
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "损坏后 ban 仍应工作: {resp}"
    );
    let text = std::fs::read_to_string(dir.path().join("bans.json")).unwrap();
    assert!(text.contains("42"), "损坏文件应被覆盖为合法名单: {text}");
}

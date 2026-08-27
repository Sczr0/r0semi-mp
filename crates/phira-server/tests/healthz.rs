//! 管理 HTTP 端点测试（§11.1 方案 B + §运营）：`/healthz` 健康检查 + `/rooms` 回归。
//!
//! ISSUE-0005 修复：peek 分流已放弃（Windows/current_thread 实测不稳定），
//! 实际实现 = 独立端口 `http_port`——`/rooms` 房间列表 + `/healthz` 健康检查。
//! `/healthz` 不依赖官方 API（验收标准：官方挂掉不影响测活）。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    RoomResponse, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::admin::http_serve;
use phira_server::server::{ConnContext, SessionSink};
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
        admin_token: Some("test-token".to_owned()),
        admin_audit: phira_server::admin::AuditLog::new(),
    })
}

/// 向 http_serve 发一个 GET 请求，返回完整 HTTP 响应文本。
async fn http_get(ctx: Arc<ConnContext>, path: &str) -> String {
    http_request(ctx, "GET", path, &[], None).await
}

/// 带管理 Bearer 认证的请求（/admin/* 全部端点需要）。
async fn http_admin(ctx: Arc<ConnContext>, method: &str, path: &str, body: &str) -> String {
    http_request(
        ctx,
        method,
        path,
        body.as_bytes(),
        Some("Bearer test-token"),
    )
    .await
}

async fn http_request(
    ctx: Arc<ConnContext>,
    method: &str,
    path: &str,
    body: &[u8],
    auth: Option<&str>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        http_serve(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: test\r\n");
    if method == "POST" {
        req = format!(
            "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\n",
            body.len()
        );
    }
    if let Some(auth) = auth {
        use std::fmt::Write;
        let _ = write!(req, "Authorization: {auth}\r\n");
    }
    req.push_str("\r\n");
    client.write_all(req.as_bytes()).await.unwrap();
    if !body.is_empty() {
        client.write_all(body).await.unwrap();
    }
    let mut resp = Vec::new();
    client.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

#[tokio::test]
async fn healthz_returns_ok_json_without_api_dependency() {
    let resp = http_get(test_ctx(), "/healthz").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(resp.contains("\"status\":\"ok\""), "status ok: {resp}");
    assert!(resp.contains("\"version\":\""), "version 字段: {resp}");
    assert!(resp.contains("\"uptime_s\":"), "uptime 字段: {resp}");
    assert!(
        resp.contains("\"connections\":0"),
        "连接数（无会话）: {resp}"
    );
    assert!(resp.contains("\"rooms\":0"), "房间数: {resp}");
    // B3（技术债）：Metrics 必须暴露给 /healthz，不让可观测性数据进黑洞
    assert!(
        resp.contains("\"internal_errors\":0"),
        "内部错误数（无故障）: {resp}"
    );
    assert!(
        resp.contains("\"metrics\":{"),
        "metrics 字段（即使空对象）: {resp}"
    );
}

/// B3（技术债）：bus 收集的 Metrics 要在 /healthz 暴露——数据驱动验证：
/// 先 dispatch 一个命令记录统计，再断言 /healthz 返回的 metrics 含该命令且 calls≥1。
#[tokio::test]
async fn healthz_exposes_bus_metrics() {
    use phira_api::CmdCtx;
    let bus = {
        let ctx = test_ctx();
        // dispatch 一个命令（记录 metrics；放到未知/空房间会返回 Err，但 calls 仍 +1）
        let _ = ctx
            .bus
            .dispatch(
                CmdCtx {
                    origin: phira_api::Origin::System,
                    room_id: RoomId::new("none".to_owned()).expect("test room id"),
                },
                RoomCommand::Tick { now: 1000 },
            )
            .await;
        let resp = http_get(ctx, "/healthz").await;
        assert!(
            resp.contains("\"metrics\":{") && resp.contains("\"tick\""),
            "metrics 应含 dispatched 命令 tick（command_name 小写）: {resp}"
        );
        resp
    };
    // 空对象守卫：metrics 不是空对象
    assert!(
        bus.contains("\"calls\":"),
        "metrics 条目含 calls 统计: {bus}"
    );
}

#[tokio::test]
async fn rooms_endpoint_still_works() {
    let resp = http_get(test_ctx(), "/rooms").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(resp.contains("[]"), "空房间列表: {resp}");
}

#[tokio::test]
async fn unknown_path_is_404() {
    let resp = http_get(test_ctx(), "/nope").await;
    assert!(resp.contains("404 Not Found"), "未知路径 404: {resp}");
}

#[tokio::test]
async fn root_lists_endpoints() {
    let resp = http_get(test_ctx(), "/").await;
    assert!(resp.contains("\"endpoints\""), "端点列表: {resp}");
    assert!(resp.contains("/healthz"), "端点含 /healthz: {resp}");
}

// —— 阶段 1 管理面（docs/admin-api.md §4）：只读端点 ——

/// 向 RoomListSink 喂一个房间事件（测试直驱 EventSink，绕过 bus）。
async fn feed_room_event(ctx: &Arc<ConnContext>, ev: &RoomEvent) {
    use phira_core::EventSink;
    EventSink::deliver(&*ctx.room_list, 0, ev).await;
}

#[tokio::test]
async fn admin_rooms_list_with_state_filter() {
    let ctx = test_ctx();
    // 造两房：r1（SelectChart）r2（Playing）
    feed_room_event(
        &ctx,
        &RoomEvent::RoomCreated {
            room_id: RoomId::new("r1".into()).unwrap(),
            host: 1,
        },
    )
    .await;
    feed_room_event(
        &ctx,
        &RoomEvent::RoomCreated {
            room_id: RoomId::new("r2".into()).unwrap(),
            host: 2,
        },
    )
    .await;
    feed_room_event(
        &ctx,
        &RoomEvent::StartPlaying {
            room_id: RoomId::new("r2".into()).unwrap(),
        },
    )
    .await;

    // 全量列表：两房都在，含 cycle 字段（阶段 1 详情字段）
    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/rooms", "").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(resp.contains("\"r1\""), "r1 在列表: {resp}");
    assert!(resp.contains("\"r2\""), "r2 在列表: {resp}");
    assert!(resp.contains("\"cycle\":false"), "cycle 字段存在: {resp}");

    // state 过滤：play 只留 r2（子串 + 大小写不敏感）
    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/rooms?state=play", "").await;
    assert!(
        resp.contains("\"r2\"") && !resp.contains("\"r1\""),
        "playing 过滤: {resp}"
    );

    let resp = http_admin(
        Arc::clone(&ctx),
        "GET",
        "/admin/rooms?state=selectchart",
        "",
    )
    .await;
    assert!(
        resp.contains("\"r1\"") && !resp.contains("\"r2\""),
        "selectchart 过滤: {resp}"
    );
}

#[tokio::test]
async fn admin_room_detail_and_404() {
    let ctx = test_ctx();
    feed_room_event(
        &ctx,
        &RoomEvent::RoomCreated {
            room_id: RoomId::new("solo-1".into()).unwrap(),
            host: 7,
        },
    )
    .await;

    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/rooms/solo-1", "").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(resp.contains("\"id\":\"solo-1\""), "id: {resp}");
    assert!(resp.contains("\"host\":7"), "host: {resp}");

    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/rooms/nope", "").await;
    assert!(resp.contains("404 Not Found"), "不存在应 404: {resp}");
}

#[tokio::test]
async fn admin_users_online_with_name() {
    let ctx = test_ctx();
    // 注册在线会话（SessionSink）+ 注册表名字（组合根视角拼装 /admin/users）
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    ctx.sink
        .register(
            42,
            Arc::new(tx),
            Arc::new(phira_server::server::Backpressure::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            phira_server::l10n::Locale::EnUs,
        )
        .await;
    let _ = ctx.registry.register(42, "admin-tester".to_owned());

    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/users", "").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(resp.contains("\"user_id\":42"), "在线用户 42: {resp}");
    assert!(
        resp.contains("\"name\":\"admin-tester\""),
        "名字来自注册表: {resp}"
    );
    assert!(
        resp.contains("\"room_id\":null"),
        "无房间归属 → null: {resp}"
    );
}

#[tokio::test]
async fn admin_metrics_exposes_bus_statistics() {
    let ctx = test_ctx();
    // 造一条命令统计（与 healthz B3 测试同构）
    ctx.bus
        .dispatch(
            CmdCtx {
                origin: phira_api::Origin::System,
                room_id: RoomId::new("x".into()).unwrap(),
            },
            RoomCommand::Tick { now: 0 },
        )
        .await
        .ok();
    let resp = http_admin(Arc::clone(&ctx), "GET", "/admin/metrics", "").await;
    assert!(resp.contains("200 OK"), "状态行: {resp}");
    assert!(
        resp.contains("\"internal_errors\":"),
        "internal_errors 键: {resp}"
    );
    assert!(resp.contains("\"metrics\":"), "metrics 键: {resp}");
}

// —— 阶段 2：认证 + 审计（docs/admin-api.md §2/§3） ——

#[tokio::test]
async fn admin_requires_bearer_token() {
    let ctx = test_ctx();
    // 无 Authorization → 401
    let resp = http_get(Arc::clone(&ctx), "/admin/rooms").await;
    assert!(resp.contains("401 Unauthorized"), "缺 token 应 401: {resp}");
    // 错 token → 401
    let resp = http_request(
        Arc::clone(&ctx),
        "GET",
        "/admin/rooms",
        &[],
        Some("Bearer wrong"),
    )
    .await;
    assert!(resp.contains("401 Unauthorized"), "错 token 应 401: {resp}");
    // 公共面不受影响
    let resp = http_get(Arc::clone(&ctx), "/rooms").await;
    assert!(resp.contains("200 OK"), "/rooms 公共: {resp}");
}

#[tokio::test]
async fn admin_disabled_when_no_token_configured() {
    // token 未配置 → 整个管理面禁用（403），哪怕带任意头也不放行
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut ctx_no_auth = test_ctx();
    let _ = Arc::get_mut(&mut ctx_no_auth);
    // test_ctx 配了 token；为测"未配置"，构造一个等价但 token=None 的 ctx
    let ctx2 = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let _ = addr;
        std::sync::Arc::new(ConnContext {
            bus: ctx_no_auth.bus.clone(),
            auth: ctx_no_auth.auth.clone(),
            registry: ctx_no_auth.registry.clone(),
            fact_tx: ctx_no_auth.fact_tx.clone(),
            sink: ctx_no_auth.sink.clone(),
            admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
            welcome_message: None,
            room_list: ctx_no_auth.room_list.clone(),
            proxy_protocol: false,
            admin_token: None,
            admin_audit: phira_server::admin::AuditLog::new(),
        })
    };
    let _ = addr;
    let resp = http_get(Arc::clone(&ctx2), "/admin/rooms").await;
    assert!(
        resp.contains("403 Forbidden"),
        "未配 token 管理面应禁用 403: {resp}"
    );
}

#[tokio::test]
async fn admin_kick_records_audit_even_on_failure() {
    let ctx = test_ctx();
    // 房间不存在 → kick 404，但审计必须记录（写操作无论成败都落审计）
    let resp = http_admin(
        Arc::clone(&ctx),
        "POST",
        "/admin/rooms/nope/kick",
        r#"{"user_id": 42}"#,
    )
    .await;
    assert!(
        resp.contains("404 Not Found"),
        "房不存在 kick 应 404: {resp}"
    );
    let audit = http_admin(Arc::clone(&ctx), "GET", "/admin/audit", "").await;
    assert!(audit.contains("admin.kick"), "失败写操作也须审计: {audit}");
    assert!(
        audit.contains("nope") || audit.contains("user:42"),
        "审计带目标: {audit}"
    );
}

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
use phira_server::server::{ConnContext, SessionSink, http_serve};
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
    })
}

/// 向 http_serve 发一个 GET 请求，返回完整 HTTP 响应文本。
async fn http_get(ctx: Arc<ConnContext>, path: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        http_serve(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
        .await
        .unwrap();
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

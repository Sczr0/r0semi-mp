//! 安全锁 A/B 测试（§10.4/§11 承诺兑现）：
//!
//! A. 全局在途字节记账：投递大帧 → 记账增长；写任务消费/连接关闭 → 回落（无泄漏）
//! B. 每连接 send 队列超限 → 强制踢出（kicker 不等积压超时）
//!
//! 注意：`IN_FLIGHT_BYTES` 是进程级 static，tokio 测试并行会互相干扰——
//! 断言用**相对基线**（投递后增长、关闭后回落），不断言绝对 0。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, CompactPos, RoomCommand, RoomConfig, RoomEvent,
    RoomFactory, RoomId, RoomResponse, Targets, TouchFrame, UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection, in_flight_bytes};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rid() -> RoomId {
    RoomId::new("r".to_owned()).unwrap()
}

/// 大触摸帧（~1MiB 编码：8 万 TouchFrame ≈ 12B/帧）。
fn big_touches() -> RoomEvent {
    let frames: Vec<TouchFrame> = (0..80_000)
        .map(|i| TouchFrame {
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)] // 测试大帧构造
            time: i as f32 * 0.001,
            points: vec![(1i8, CompactPos::new(0.5, 0.5))],
        })
        .collect();
    RoomEvent::RelayTouches {
        room_id: rid(),
        targets: Targets::Specific(vec![1]),
        player: 1,
        frames: Arc::new(frames),
    }
}

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "mem".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

/// 每个命令返回 ~1MiB 大帧（投递路径触发记账）；CreateRoom 附带路由注册事件。
struct BigFrameActor;

#[async_trait::async_trait]
impl phira_api::RoomActor for BigFrameActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        // B6/B1：心跳 Tick 不产生大帧（真实 impl 处理 Tick 成本 ≈0），
        // 否则周期心跳会把内存记账测试的输入变成不可控的持续洪峰。
        if matches!(cmd, RoomCommand::Tick { .. }) {
            return (None, Vec::new());
        }
        let mut events = vec![big_touches()];
        if matches!(cmd, RoomCommand::CreateRoom { .. }) {
            events.push(RoomEvent::RoomCreated {
                room_id: rid(),
                host: 1,
            });
        }
        (Some(RoomResponse::Ok), events)
    }
}

struct BigFactory;

impl RoomFactory for BigFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(BigFrameActor)
    }
}

fn test_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(BigFactory);
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
        admin_token: None,
        admin_audit: phira_server::admin::AuditLog::new(),
        admin_config: phira_server::admin::AdminConfigState::new(),
        admin_ban_observer: phira_server::server::BanObserver::new(),
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

/// 鉴权后的已握手客户端（AuthOk，user_id=1）。
async fn authed_client(ctx: Arc<ConnContext>) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    // 读鉴权响应（丢弃）
    let mut buf = [0u8; 4096];
    let _ = client.read(&mut buf).await;
    client
}

#[tokio::test]
async fn memory_accounting_grows_on_charge_and_drains_on_close() {
    let baseline = in_flight_bytes();
    let ctx = test_ctx();
    let mut client = authed_client(ctx.clone()).await;

    // 建房（路由注册）+ 投递 3 个大帧（只写不读 → 写任务卡 socket → 队列积压记账）
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 用 Touches（热路径不限速）而非 Chat（D1 已限速）——BigFrameActor 任意命令都返回大帧
    for _ in 0..3 {
        client
            .write_all(&client_frame(&ClientCommand::Touches {
                frames: Arc::new(vec![]),
            }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let charged = in_flight_bytes();
    assert!(
        charged > baseline + 100_000,
        "大帧投递应记账增长: baseline={baseline} charged={charged}"
    );

    // 关闭连接（写任务收尾清账：消费释放 + MemoryReleaser 兜底）→ 记账回落。
    // 注意：`IN_FLIGHT_BYTES` 是全局 static，本文件两个测试并行共享——CI（Linux）
    // send buffer 大，并行测试的帧可能正在陆续写出/释放，固定 sleep 会读到 mid-flight
    // 账（本地 Windows 小 buffer 秒卡死所以稳定绿）——改轮询等待回落（≤2s），
    // 真正等"收尾完成"而非赌时序。
    drop(client);
    let mut after = in_flight_bytes();
    for _ in 0..100 {
        if after <= baseline + 64 * 1024 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        after = in_flight_bytes();
    }
    assert!(
        after <= baseline + 64 * 1024,
        "连接关闭后记账应回落（无泄漏）: baseline={baseline} after={after}"
    );
}

#[tokio::test]
async fn per_conn_memory_over_limit_kicks_client() {
    let baseline = in_flight_bytes();
    let ctx = test_ctx();
    let mut client = authed_client(ctx).await;

    // 建房（路由注册）
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 投递 ~20MiB（> PER_CONN_MEM_LIMIT=8MiB）——客户端只写不读，队列积压。
    // 20 帧留消费余量：即使写任务消费一半仍超 8MiB 阈值（防并行负载下 flaky）。
    // 用 Touches（热路径不限速，rate_limit 返回 None）而非 Chat（D1 已限速 2/s）——
    // BigFrameActor 对任何命令都返回大帧，Touches 足够触发记账且不受限。
    for _ in 0..20 {
        client
            .write_all(&client_frame(&ClientCommand::Touches {
                frames: Arc::new(vec![]),
            }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let charged = in_flight_bytes();
    assert!(
        charged > baseline + 8 * 1024 * 1024,
        "累计投递应超每连接上限: charged={charged}"
    );

    // 等 kicker（1s 粒度）踢出
    let mut buf = [0u8; 1024];
    let kicked = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(
        kicked.is_ok(),
        "内存超限连接应在 4s 内被踢出（kicker 检查 force_close）"
    );

    // 踢出后记账回落（无泄漏）
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = in_flight_bytes();
    assert!(
        after <= baseline + 64 * 1024,
        "踢出后记账应回落: baseline={baseline} after={after}"
    );
}

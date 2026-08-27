//! 慢消费者保护测试（ISSUE-0004 修复）：
//!
//! 1. 单元：`deliver` 满队列**丢帧不阻塞**（§10.4"绝不阻塞房间 actor"兑现）——try_send 语义
//! 2. 单元：`Backpressure` 积压标记（mark 幂等 / 恢复清除）——踢乌龟判定的数据源
//! 3. 集成：真实 TCP 乌龟客户端（只写不读）→ 队列持续满 → 服务端按阈值踢出（断连）
//!
//! 修复前：`deliver` 用 `send().await`（满时无限等待）→ bus 投递循环串行 await →
//! room_loop 卡住 → 整个房间被一个乌龟 monitor 间接阻塞（ISSUE-0004 原始问题）。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, RoomCommand, RoomConfig, RoomEvent, RoomFactory,
    RoomId, RoomResponse, UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, EventSink, lifecycle::LifecycleTask};
use phira_server::server::{Backpressure, ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

fn rid() -> RoomId {
    RoomId::new("r".to_owned()).unwrap()
}

fn chat_event() -> RoomEvent {
    RoomEvent::Chat {
        room_id: rid(),
        user: 2,
        content: "flood".to_owned(),
    }
}

// —— 单元：deliver 满队列丢帧不阻塞 + 积压标记 ——

#[tokio::test]
async fn deliver_drops_when_queue_full_without_blocking() {
    let sink = SessionSink::new();
    let (tx, _rx) = mpsc::channel::<phira_server::stream::Outbound>(1);
    let bp = Arc::new(Backpressure::new());
    sink.register(
        1,
        Arc::new(tx),
        Arc::clone(&bp),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        phira_server::l10n::Locale::default(),
    )
    .await;

    sink.deliver(1, &chat_event()).await;
    assert!(
        bp.elapsed().is_none(),
        "第一条应投递成功且未积压（rx 未消费，占满容量 1）"
    );
    // 第二条：队列满 → try_send 失败 → 丢帧 + 标记积压；deliver 立即返回（不阻塞）
    sink.deliver(1, &chat_event()).await;
    assert!(
        bp.elapsed().is_some(),
        "第二条应丢帧并标记积压（不阻塞房间投递）"
    );
}

#[tokio::test]
async fn backpressure_clears_when_queue_drains() {
    let sink = SessionSink::new();
    let (tx, mut rx) = mpsc::channel::<phira_server::stream::Outbound>(1);
    let bp = Arc::new(Backpressure::new());
    sink.register(
        1,
        Arc::new(tx),
        Arc::clone(&bp),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        phira_server::l10n::Locale::default(),
    )
    .await;

    sink.deliver(1, &chat_event()).await;
    sink.deliver(1, &chat_event()).await;
    assert!(bp.elapsed().is_some(), "满队列应标记积压");
    // 写任务消费一条 → 队列有空位 → 下一次投递成功 → 清除标记（正常波动自愈）
    rx.recv().await.unwrap();
    sink.deliver(1, &chat_event()).await;
    assert!(bp.elapsed().is_none(), "队列恢复应清除积压标记");
}

#[tokio::test]
async fn backpressure_mark_is_idempotent() {
    let bp = Backpressure::new();
    assert!(bp.elapsed().is_none(), "初始未积压");
    bp.mark();
    let t1 = bp.elapsed().unwrap();
    bp.mark();
    let t2 = bp.elapsed().unwrap();
    assert!(
        t2 >= t1,
        "mark 幂等：不重置开始时刻（避免刷 mark 无限续命）"
    );
    bp.clear();
    assert!(bp.elapsed().is_none(), "clear 后未积压");
}

// —— 集成：真实 TCP 乌龟踢出 ——

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, _token: &str) -> Result<UserIdentity, AuthError> {
        Ok(UserIdentity {
            user_id: 1,
            name: "turtle".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

struct FloodFactory;

impl RoomFactory for FloodFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(FloodActor)
    }
}

/// 洪泛 actor：每个命令返回 100 条 Chat 事件（Targets::All → 投递给房内用户）。
/// 用途：让服务端持续向客户端投递事件，填满发送队列 + 阻塞写任务（客户端不读）。
struct FloodActor;

#[async_trait::async_trait]
impl phira_api::RoomActor for FloodActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        _cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        let events = (0..100)
            .map(|i| RoomEvent::Chat {
                room_id: rid(),
                user: 1,
                content: format!("flood-{i}"),
            })
            .collect();
        (Some(RoomResponse::Ok), events)
    }
}

fn flood_ctx() -> Arc<ConnContext> {
    let factory = Arc::new(FloodFactory);
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

/// ULEB128 长度前缀 + 载荷（与 frames.rs 同款，不依赖测试对象自身的便捷层）。
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

/// 客户端命令 → 协议帧（encode_packet 编码载荷 + ULEB128 长度前缀）。
fn client_frame(cmd: &ClientCommand) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_packet(cmd, &mut buf);
    frame(&buf)
}

#[tokio::test]
async fn slow_consumer_kicked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, flood_ctx()).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    // 鉴权（AuthOk）
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    // 读鉴权响应（此后停止读——乌龟）
    let mut buf = [0u8; 4096];
    let n = client.read(&mut buf).await.expect("读鉴权响应");
    assert!(n > 0, "鉴权应成功返回");

    // 建房：触发 actor 返回 100 事件（首次投递）
    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    // 洪泛：300 个命令 × 每命令 100 事件 = 30000 次投递 → 写任务卡 socket（客户端不读）
    // → 发送队列满 → try_send 失败 → mark 积压 → 持续满超阈值（5s）→ kicker 踢出
    for _ in 0..300 {
        client
            .write_all(&client_frame(&ClientCommand::Chat {
                message: Varchar::new("x".repeat(200)).unwrap(),
            }))
            .await
            .unwrap();
    }
    // 等积压阈值（SLOW_CONSUMER_KICK_AFTER = 5s + 1s 检查粒度）+ 余量
    tokio::time::sleep(Duration::from_secs(8)).await;

    // 服务端应已断开：踢出后积压数据仍在途，需循环读——EOF(0)/RST(Err) = 已断；
    // 若连接仍存活（未被踢），写任务会持续产出数据，读循环不会结束 → 3s 超时判失败
    let mut total = 0usize;
    let kicked = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match client.read(&mut buf).await {
                // FIN（干净断开）或 RST（有在途数据被丢弃）都算被踢
                Ok(0) | Err(_) => break,
                Ok(n) => total += n, // 积压数据（踢出前已写入），继续读
            }
        }
    })
    .await;
    assert!(
        kicked.is_ok(),
        "读超时——连接仍存活，乌龟未被踢（已读 {total} 字节）"
    );
}

// —— ISSUE-0003 方案 2：热路径编码一次共享（EncodeCache）——

use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn encode_cache_encodes_once_per_frame_key() {
    let cache = phira_server::server::EncodeCache::new(64);
    let calls = Arc::new(AtomicUsize::new(0));
    let key = 0x1234usize;
    let f = || {
        calls.fetch_add(1, Ordering::SeqCst);
        vec![1u8, 2, 3]
    };
    let a = cache.get_or_encode(key, Box::new(()), f);
    let b = cache.get_or_encode(key, Box::new(()), f);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "同 key 只编码一次");
    assert_eq!(&*a, &*b, "同一帧共享同一载荷（Arc 引用）");
}

#[tokio::test]
async fn encode_cache_evicts_when_full() {
    let cache = phira_server::server::EncodeCache::new(2);
    let calls = Arc::new(AtomicUsize::new(0));
    for i in 0..3 {
        cache.get_or_encode(i, Box::new(()), || {
            calls.fetch_add(1, Ordering::SeqCst);
            vec![u8::try_from(i).unwrap()]
        });
    }
    // 第 3 条插入时满 → 清空；旧 key(0) 重新编码
    cache.get_or_encode(0, Box::new(()), || {
        calls.fetch_add(1, Ordering::SeqCst);
        vec![9]
    });
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "满则清空：3 条插入 + 旧 key 重新编码 = 4 次"
    );
}

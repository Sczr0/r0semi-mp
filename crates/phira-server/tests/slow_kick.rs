//! 5 秒积压踢出（SLOW_CONSUMER_KICK_AFTER）真实触发测试。
//!
//! `slow_consumer.rs` 的 `slow_consumer_kicked` 用的是 Chat 洪泛——Chat 已限速
//! （2/s）+ 响应风暴，实测踢出实际走**内存守卫（forced）路径**，`elapsed >= 5s`
//! 分支从未执行（覆盖率实证：kicker 的 elapsed 分支 0 命中）。
//!
//! 本测试用热路径 Touches（不限速、小帧）：
//! - 小帧 → 1024 帧队列可填满但 queue_bytes 远低于 8MiB（per-conn 记账不触发）
//! - 总洪泛 30MB → socket 缓冲必满 → 队列持续满 → 积压标记持续 → 5s 阈值到期
//! - 踢出点：kicker 的 `elapsed >= SLOW_CONSUMER_KICK_AFTER` 分支（非 forced）
//!
//! 排除项：心跳超时 10s > 踢出 ~6-7s；限速（Touches 不限）；内存守卫（~1MB << 8MiB）。

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, CmdCtx, CompactPos, RoomCommand, RoomConfig, RoomEvent,
    RoomFactory, RoomId, RoomResponse, TouchFrame, UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rid() -> RoomId {
    RoomId::new("r".to_owned()).unwrap()
}

/// 洪泛 actor：每个命令返回 100 条小尺寸 RelayTouches（~1KB/条，热路径不限速）。
/// 用途：填满 1024 帧发送队列而不触发 8MiB 字节记账（1024 × 1KB ≈ 1MB）。
struct FloodFactory;

impl RoomFactory for FloodFactory {
    fn create(&self, _room_id: RoomId) -> Box<dyn phira_api::RoomActor> {
        Box::new(FloodActor)
    }
}

struct FloodActor;

#[async_trait::async_trait]
impl phira_api::RoomActor for FloodActor {
    async fn handle(
        &mut self,
        _ctx: CmdCtx,
        _cmd: RoomCommand,
    ) -> (Option<RoomResponse>, Vec<RoomEvent>) {
        // 200 触点 × ~5B/点 ≈ 1KB 帧：够撑满 socket 缓冲（踢出可靠性），
        // 单帧小（总账 1024×1KB ≈ 1MB << 8MiB per-conn，不触发内存守卫）
        let points: Vec<(i8, CompactPos)> = (0..200)
            .map(|i| {
                let finger = i8::try_from(i % 8).unwrap();
                let x = f32::from(u8::try_from(i % 8).unwrap());
                (finger, CompactPos::new(x * 0.1, 0.5))
            })
            .collect();
        let events = (0..100)
            .map(|i| {
                if i % 2 == 0 {
                    RoomEvent::RelayTouches {
                        room_id: rid(),
                        targets: phira_api::Targets::All,
                        player: 1,
                        frames: Arc::new(vec![TouchFrame {
                            time: 1.0,
                            points: points.clone(),
                        }]),
                    }
                } else {
                    // 混入判定事件：覆盖 convert 的 RelayJudges 转 ServerCommand::Judges
                    RoomEvent::RelayJudges {
                        room_id: rid(),
                        targets: phira_api::Targets::All,
                        player: 1,
                        judges: Arc::new(vec![phira_api::JudgeEvent {
                            time: 1.0,
                            line_id: 1,
                            note_id: 1,
                            judgement: phira_api::Judgement::Perfect,
                        }]),
                    }
                }
            })
            .collect();
        (Some(RoomResponse::Ok), events)
    }
}

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

/// 5s 积压阈值踢出（非 forced）：真实 TCP 乌龟 + Touches 小帧洪泛。
/// 踢出时刻 ≈ 首次持续积压 + 5s + ≤1s 轮询粒度，早于心跳超时（10s）。
#[tokio::test]
async fn slow_consumer_kicked_via_elapsed_threshold() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, flood_ctx()).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
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

    client
        .write_all(&client_frame(&ClientCommand::CreateRoom { id: rid() }))
        .await
        .unwrap();
    // 洪泛：400 命令 × 100 事件 × ~1KB = ~40MB —— socket 缓冲（≤几 MB）必满，
    // 1024 帧队列持续满 → 积压标记持续 → 5s 阈值到期踢出（总洪泛时间 >> 5s 窗口）。
    for _ in 0..400 {
        client
            .write_all(&client_frame(&ClientCommand::Touches {
                frames: Arc::new(vec![TouchFrame {
                    time: 1.0,
                    points: vec![],
                }]),
            }))
            .await
            .unwrap();
    }

    // 等积压阈值（5s）+ 轮询粒度（1s）+ 洪泛尾差 + 余量
    tokio::time::sleep(Duration::from_secs(9)).await;

    // 踢出后积压数据仍在途需循环读——EOF(0)/RST(Err) = 已断；连接仍活 → 超时判失败
    let mut total = 0usize;
    let kicked = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match client.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n,
            }
        }
    })
    .await;
    assert!(
        kicked.is_ok(),
        "读超时——连接仍存活，5s 积压踢出未触发（已读 {total} 字节）"
    );
}

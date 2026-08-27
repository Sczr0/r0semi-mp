//! 广播扇出 CPU 基准（手动：`cargo test -p phira-server --test bench_broadcast -- --ignored --nocapture`）。
//!
//! 场景 = §10.1.1 "1500 人狂按键"的本地复现：N 连接同房（1 宿主 CreateRoom + 其余 JoinRoom），
//! 全部 16Hz Touches，持续 `--duration` 秒。配合采样定位 CPU 热点：
//! ```text
//! samply record -- cargo test -p phira-server --test bench_broadcast -- --ignored --nocapture
//! ```
//! 规模由环境变量控制：`R0SEMI_BENCH_N`（默认 300）、`R0SEMI_BENCH_SECS`（默认 5）。
//!
//! 只做观测基准：不入 CI、不断言（无 assert），产出打印供分析。

use std::sync::Arc;
use std::time::{Duration, Instant};

use phira_api::{
    ApiClient, ApiError, AuthError, AuthHandler, Chart, RandomSource, Record, RoomConfig, RoomDeps,
    RoomFactory, RoomId, UserIdentity,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, SessionSink, handle_connection, in_flight_bytes};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 回源桩（bench 不发 Played/不放谱，永不触达）。
struct NoopApi;

#[async_trait::async_trait]
impl ApiClient for NoopApi {
    async fn fetch_chart(&self, _id: i32) -> Result<Chart, ApiError> {
        Err(ApiError::Internal {
            msg: "no chart in bench".to_owned(),
        })
    }
    async fn fetch_record(&self, _id: i32) -> Result<Record, ApiError> {
        Err(ApiError::Internal {
            msg: "no record in bench".to_owned(),
        })
    }
}

struct AuthOk;

#[async_trait::async_trait]
impl AuthHandler for AuthOk {
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, AuthError> {
        // token 形如 `tok<user_id>`——分流到不同 user（同 user 会串台）
        let id: i32 = token
            .strip_prefix("tok")
            .and_then(|t| t.parse().ok())
            .unwrap_or(1);
        Ok(UserIdentity {
            user_id: id,
            name: "b".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

struct Rng;

impl RandomSource for Rng {
    fn pick_index(&self, len: usize) -> Option<usize> {
        len.checked_sub(1)
    }
}

fn test_ctx() -> Arc<ConnContext> {
    let deps = RoomDeps {
        api: Arc::new(NoopApi) as Arc<dyn ApiClient>,
        rng: Arc::new(Rng) as Arc<dyn RandomSource>,
    };
    let rooms = impl_rooms_v1::RoomsV1::new(RoomConfig { monitors: vec![] }, deps);
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn RoomFactory>,
        Arc::new(RoomConfig { monitors: vec![] }),
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

fn encode_frame(cmd: &phira_api::ClientCommand) -> Vec<u8> {
    let mut payload = Vec::new();
    phira_api::encode_packet(cmd, &mut payload);
    let mut frame = Vec::with_capacity(payload.len() + 5);
    let mut x = payload.len() as u64;
    loop {
        let mut b = (x & 0x7f) as u8;
        x >>= 7;
        if x != 0 {
            b |= 0x80;
        }
        frame.push(b);
        if x == 0 {
            break;
        }
    }
    frame.extend_from_slice(&payload);
    frame
}

/// 连接 + 握手 + 进房（串行调用——每连接握手完成后释放 pending 槽位，
/// 绕过生产 `MAX_PENDING_PER_IP=5`（每 IP 未鉴权并发上限，§10.4 防风暴））
async fn connect_player(
    addr: std::net::SocketAddr,
    user_id: i32,
    room: RoomId,
    is_host: bool,
) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    sock.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    let auth = encode_frame(&phira_api::ClientCommand::Authenticate {
        token: phira_api::Varchar::new(format!("tok{user_id}")).unwrap(),
    });
    sock.write_all(&auth).await.unwrap();
    let _auth_ok = recv_frame(&mut sock).await; // 等 AuthOk（释放未鉴权连接槽）

    let enter = if is_host {
        phira_api::ClientCommand::CreateRoom { id: room.clone() }
    } else {
        phira_api::ClientCommand::JoinRoom {
            id: room.clone(),
            monitor: false,
        }
    };
    sock.write_all(&encode_frame(&enter)).await.unwrap();
    let _enter = recv_frame(&mut sock).await; // 等进房回复（响应帧）
    sock
}

/// 精确读一帧（2s 超时防挂；帧头 ULEB128 + 载荷，§6.1）。
async fn recv_frame(sock: &mut TcpStream) -> phira_api::ServerCommand {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut len = 0u64;
        let mut pos = 0;
        loop {
            let byte = sock.read_u8().await.unwrap();
            len |= u64::from(byte & 0x7f) << pos;
            pos += 7;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let mut payload = vec![0u8; usize::try_from(len).unwrap()];
        sock.read_exact(&mut payload).await.unwrap();
        phira_api::decode_packet(&payload).unwrap()
    })
    .await
    .expect("recv_frame timeout: 帧流错位或服务器无响应")
}

/// 触摸流：16Hz Touches（帧 Arc 共享）持续 secs 秒；每 128ms 稀疏清读缓冲——
/// 贴近真实客户端（勤读），避免被服务端"乌龟踢除"（§10.4 backpressure kicker）。
async fn touch_loop(mut sock: TcpStream, touches: Vec<u8>, secs: u64) -> u64 {
    let interval = Duration::from_millis(1000 / 16);
    let mut sent: u64 = 0;
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut buf = [0u8; 4096];
    let mut next_drain = Instant::now();
    while Instant::now() < deadline {
        sock.write_all(&touches).await.unwrap();
        sent += 1;
        if Instant::now() >= next_drain {
            // 稀疏清读：把积压广播全部读丢（不解析——只防队列超限踢除）
            let mut drained = 0usize;
            loop {
                match sock.try_read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        drained += n;
                        if drained > 1 << 20 {
                            break;
                        }
                    }
                }
            }
            next_drain = Instant::now() + Duration::from_millis(128);
        }
        tokio::time::sleep(interval).await;
    }
    sent
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "手动压测：samply record -- ..."]
async fn broadcast_fanout_bench() {
    let n: usize = std::env::var("R0SEMI_BENCH_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let secs: u64 = std::env::var("R0SEMI_BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    let ctx = test_ctx();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_ctx = Arc::clone(&ctx);
    let accept = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let ctx = Arc::clone(&accept_ctx);
            tokio::spawn(async move {
                let _ = handle_connection(stream, addr, ctx).await;
            });
        }
    });

    // 逐个连入（串行握手 → 未鉴权槽位逐连接释放，绕过每 IP 5 并发上限）
    eprintln!("连入 {n} 客户端…");
    let room = phira_api::RoomId::new("r1".to_owned()).unwrap();
    let mut socks = Vec::with_capacity(n);
    for i in 0..n {
        let id = i32::try_from(i).unwrap_or(i32::MAX) + 1;
        socks.push(connect_player(addr, id, room.clone(), i == 0).await);
    }
    let frames = Arc::new(vec![phira_api::TouchFrame {
        time: 0.0,
        points: vec![(0, phira_api::CompactPos::new(0.0, 0.0))],
    }]);
    let touches = encode_frame(&phira_api::ClientCommand::Touches {
        frames: Arc::clone(&frames),
    });

    // 同时开 tou流（这才是"1500 人狂按键"的 CPU 画像）
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n);
    for sock in socks {
        handles.push(tokio::spawn(touch_loop(sock, touches.clone(), secs)));
    }
    let mut total: u64 = 0;
    for h in handles {
        if let Ok(s) = h.await {
            total += s;
        }
    }
    let elapsed = start.elapsed();
    accept.abort();
    let _ = accept.await;

    // 等连接收尾（写任务清账）
    tokio::time::sleep(Duration::from_millis(200)).await;
    // epochs 锁探针（R0SEMI_EPOCHS_PROBE=1 时取数；默认关闭零开销）
    if let Some((calls, slow, wait_us)) = ctx.registry.probe_snapshot() {
        eprintln!("== epochs 锁探针 == 调用={calls} 慢锁(>50µs)={slow} 总等待={wait_us}µs");
    }
    drop(ctx);

    eprintln!(
        "== 广播扇出基准 ==
客户端: {n}  | 时长: {elapsed:?} | 房内触摸帧/秒: {:.0} | 在途: {:.1} MiB",
        f64::from(u32::try_from(total).unwrap_or(u32::MAX)) / elapsed.as_secs_f64(),
        f64::from(u32::try_from(in_flight_bytes()).unwrap_or(u32::MAX)) / 1_048_576.0,
    );
}

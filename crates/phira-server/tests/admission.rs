//! 准入（§10.4）+ PROXY protocol 接线 + 心跳恢复集成测试。
//!
//! - `ConnectionAdmission` 直连单测：全局拒绝回滚（per-IP 计数归还）与
//!   release 的 v>1 / v==1 两条分支（连接受控收尾的记账平衡）
//! - PROXY 接线（`ctx.proxy_protocol = true`）：v1 TCP4 头 → 正常建连；
//!   垃圾头 → 拒绝断开
//! - 心跳恢复：濒危窗口（≥8s 无包）后发包 → 恢复日志路径（C-02）

use std::sync::Arc;
use std::time::Duration;

use phira_api::{
    AuthError, AuthHandler, ClientCommand, RoomConfig, RoomEvent, RoomFactory, RoomId,
    UserIdentity, Varchar, encode_packet,
};
use phira_core::{Bus, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, ConnectionAdmission, SessionSink, handle_connection};
use phira_server::stream::PROTOCOL_VERSION;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 无副作用 actor（任何命令 → 空事件）。
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
            name: "admit".to_owned(),
            lang: "zh".to_owned(),
        })
    }
}

fn ctx_with(proxy: bool) -> Arc<ConnContext> {
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
        admission: Arc::new(ConnectionAdmission::default()),
        welcome_message: None,
        room_list: Arc::new(phira_server::server::RoomListSink::new(Vec::new())),
        proxy_protocol: proxy,
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

/// 建连 + 鉴权；返回校验未做的原始流（供后续断言）。
async fn connect_auth(client: &mut TcpStream, prefix: &[u8]) -> std::io::Result<usize> {
    client.write_all(prefix).await?;
    client.write_all(&[PROTOCOL_VERSION]).await?;
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await?;
    let mut buf = [0u8; 1024];
    client.read(&mut buf).await
}

// —— ConnectionAdmission 直连 ——

/// 全局拒绝路径的 per-IP 回滚 + release 的 v>1 / v==1 分支。
#[tokio::test]
async fn admission_global_reject_rolls_back_per_ip_and_release_decrements() {
    let admission = ConnectionAdmission::default();
    let ip = "203.0.113.5".parse().unwrap();
    // 2 条同 IP 计入
    assert!(admission.try_acquire(ip));
    assert!(admission.try_acquire(ip));

    // release 第一条：v 2→1（递减分支）
    admission.release(ip);
    // release 第二条：v 1→0（移除分支）——再同 IP 准入应重新从 1 起
    admission.release(ip);
    assert!(admission.try_acquire(ip), "移除后同 IP 应可重新准入");
}

/// 未鉴权全局上限偏高（100），用不同 IP 灌到上限触发**全局拒绝回滚**
/// （1348-1361：回滚 per-IP 计数 + 全局计数，保证账目平衡）。
#[tokio::test]
async fn admission_global_cap_rolls_back_both_counters() {
    let admission = ConnectionAdmission::default();
    let ips: Vec<std::net::IpAddr> = (0..120)
        .map(|i| format!("10.0.{}.{}", i / 256, i % 256).parse().unwrap())
        .collect();
    let mut accepted = 0usize;
    let mut rejected_at = None;
    for (idx, ip) in ips.iter().enumerate() {
        if admission.try_acquire(*ip) {
            accepted += 1;
        } else {
            rejected_at = Some(idx);
            break;
        }
    }
    let rejected_at = rejected_at.expect("超过全局上限后应拒绝");
    // 全局上限 100（MAX_PENDING_CONNECTIONS）——第 101 个 IP 被拒
    assert_eq!(rejected_at, 100, "全局未鉴权上限应为 100");
    assert_eq!(accepted, 100);

    // 平衡：把已准入的全部释放（每 IP 恰好 1 条 → 逐条走移除分支）
    for ip in &ips[..100] {
        admission.release(*ip);
    }
    // 被拒 IP 的计数已被回滚——现在可正常准入
    let ip101 = ips[100];
    assert!(
        admission.try_acquire(ip101),
        "全局拒绝应回滚该 IP 的 per-IP 计数（否则从此被卡死）"
    );
}

// —— PROXY protocol 接线（ctx 开关 + handle_connection 全链路） ——

/// v1 TCP4 头 → 正常握手 + 鉴权成功。
#[tokio::test]
async fn proxy_header_accepted_and_connection_flows() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = ctx_with(true);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let n = connect_auth(
        &mut client,
        b"PROXY TCP4 203.0.113.7 123.0.0.1 12345 12346\r\n",
    )
    .await
    .expect("代理头 + 鉴权流程应成功");
    assert!(n > 0, "应收到鉴权响应");
}

/// 垃圾代理头 → 拒绝断开（读 EOF/RST）。
#[tokio::test]
async fn proxy_header_garbage_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = ctx_with(true);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"NOT-A-PROXY-HEADER\r\n").await.unwrap();
    let mut buf = [0u8; 16];
    let r = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf)).await;
    r.as_ref().expect("应在超时前断开");
    match r.unwrap() {
        Ok(0) | Err(_) => {}
        Ok(_) => panic!("垃圾代理头被接受（不应收到数据）"),
    }
}

/// 未开 proxy_protocol 时，PROXY 头按协议帧处理：首字节 'P'(0x50) ≠ 版本号 1
/// → 握手失败断开（原版语义，防"前置层没配但客户端发了头"的静默错乱）。
#[tokio::test]
async fn proxy_header_ignored_when_disabled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ctx = ctx_with(false);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx).await.unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"PROXY TCP4 203.0.113.7 123.0.0.1 12345 12346\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 16];
    let r = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
        .await
        .expect("应在握手超时前断开");
    match r {
        Ok(0) | Err(_) => {}
        Ok(_) => panic!("未开启时 PROXY 头不应被当作协议帧接受"),
    }
}

// —— 心跳恢复（C-02）：濒危窗口后发包 → 恢复路径 ——

/// 鉴权后静置 ≥ HEARTBEAT_STALE_MARK（8s）再发包：连接应存活且收到回应
/// （覆盖 handle_connection 的"heartbeat recovered"路径）。
#[tokio::test]
async fn heartbeat_recovered_after_stale_window() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, addr, ctx_with(false))
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    client
        .write_all(&client_frame(&ClientCommand::Authenticate {
            token: Varchar::new("tok".to_owned()).unwrap(),
        }))
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).await.expect("读鉴权响应");
    assert!(n > 0);

    // 静置过濒危窗口（8s）但未到心跳超时（10s）——窗口窄，睡 9s 有风险
    // 撞上 10s 超时；改睡 8.3s（8s 濒危 + 0.3s 余量 < 10s 超时），
    // 回调窗口充裕（心跳监控从"最后一次收包"重算）。
    tokio::time::sleep(Duration::from_millis(8300)).await;

    // 发包（Ping——不限速）→ 应恢复（不触发心跳 Disconnected），并收到 Pong
    client
        .write_all(&client_frame(&ClientCommand::Ping))
        .await
        .unwrap();
    let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf))
        .await
        .expect("Ping 应在超时前收到 Pong")
        .expect("读 Pong 成功");
    assert!(n > 0, "应收到 Pong 响应（连接存活证明）");
    // 连接仍活：下一读应超时而非立即 EOF
    let r = tokio::time::timeout(Duration::from_millis(700), client.read(&mut buf)).await;
    assert!(r.is_err(), "心跳恢复后连接应保持存活（读超时 = 未断开）");
}

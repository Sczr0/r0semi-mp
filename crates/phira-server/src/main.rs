//! # phira-server —— 老板（组合根，§4.1 / §4.5）
//!
//! **唯一认识所有人的 crate**。接线在 Day-1 清单第 5 步：
//! 决定谁上架（RoomsV1）+ 注入外部依赖（HTTP/随机）+ 柜台开业（Bus → Server）。
//! 换实现 = 组合根换工厂（§3.2：灰度已降级为运维选项，项目内零灰度代码）。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use phira_api::{ApiClient, RandomSource, RoomConfig, RoomDeps, RoomFactory};
use phira_core::{Bus, Config, EventSink, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, Server, SessionSink};

/// 老板接线（§4.5）。组合根接线长是角色使然——换实现只动这里（§3.2）。
#[allow(clippy::too_many_lines)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let config = Config::load()?;
    eprintln!("[boot] api_base = {}", config.api_base);

    // 老板接线：决定谁上架 + 注入外部依赖（§4.9-6）
    // 单一 HTTP 实例，auth 与 chart/record 共享（评审 §8 五-1）
    let http = Arc::new(phira_server::http::HttpApiClient::new_with_timeout(
        config.api_base.clone(),
        config.http_timeout,
    ));

    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn ApiClient>,
        rng: Arc::new(phira_server::http::ThreadRngSource) as Arc<dyn RandomSource>,
    };

    // 第一个货物（工厂，持有 deps）；换实现 = 组合根换工厂
    let rooms = impl_rooms_v1::RoomsV1::new(config.rooms.clone(), deps);

    // 柜台开业：当前生效配置 = 组合根注入的初始配置（§4.9-8 热更走 Bus::update_config）
    // with_api：A2 两段式——Played 的成绩回源在柜台（房外任务）进行，不阻塞房间 actor
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn RoomFactory>,
        Arc::new(config.rooms.clone()) as Arc<RoomConfig>,
    )
    .with_api(Arc::clone(&http) as Arc<dyn ApiClient>);

    // 用户生命周期（§4.9-3）：单一生产者任务 + 注册表（重连窗口 = yml `reconnect_window`；
    // C-03/ADR-0012：对局中断线用 `playing_reconnect_window`，命令化查询 GetClientState 分级）
    let (lifecycle_task, registry, fact_tx) = LifecycleTask::new(
        bus.clone(),
        config.reconnect_window,
        Duration::from_millis(50),
    );
    let lifecycle_task =
        lifecycle_task.with_playing_reconnect_window(config.playing_reconnect_window);
    tokio::spawn(lifecycle_task.run());

    // 事件投递（§6.6 表 2）：user → 会话写通道 + 房间列表观察者（§7.3 组合）
    let sink = Arc::new(SessionSink::new());
    let room_list = Arc::new(phira_server::server::RoomListSink::new(
        config.hidden_room_prefixes.clone(),
    ));
    let composite = Arc::new(phira_server::server::CompositeSink::new(vec![
        Arc::clone(&sink) as Arc<dyn EventSink>,
        Arc::clone(&room_list) as Arc<dyn EventSink>,
    ]));
    bus.attach_sink(composite as Arc<dyn EventSink>);

    // 鉴权（回源 /me，§6.5-14）
    let auth: Arc<dyn phira_api::AuthHandler> =
        Arc::new(phira_server::http::HttpAuth::new_with_timeout(
            config.api_base.clone(),
            config.http_timeout,
        ));

    // 配置热重载（§4.9-8）：文件轮询 → update_config；路径 = R0SEMI_MP_CONFIG 或默认
    let config_path = std::env::var("R0SEMI_MP_CONFIG")
        .unwrap_or_else(|_| phira_core::DEFAULT_CONFIG_PATH.to_owned());
    bus.watch_config(
        std::path::PathBuf::from(config_path),
        config.config_poll_interval,
    );

    // 管理面持久化（组合根 storage，docs/admin-api.md §持久化）：
    // - 启动加载生效配置原文（config.current.json）→ 覆盖初始配置；
    // - 上一份（config.last.json）回填 AdminConfigState->重启后仍可 rollback；
    // - ban/audit 由 BanObserver/AuditLog 带文件构造时自动加载（同目录）。
    let persist_dir = std::path::PathBuf::from(&config.persist_dir);
    phira_server::storage::ensure_dir(&persist_dir);
    let config_store = phira_server::storage::ConfigStore::new(&persist_dir);
    let (saved_config, saved_last) = config_store.load();
    if let Some(cfg) = saved_config {
        match phira_server::storage::config_from_json(&cfg) {
            Ok(rc) => bus.update_config(Arc::new(rc)).await,
            Err(msg) => eprintln!("[boot] persist config.current ignored: {msg}"),
        }
    }
    let admin_config = phira_server::admin::AdminConfigState::new();
    if let Some(last) = saved_last {
        match phira_server::storage::config_from_json(&last) {
            Ok(rc) => admin_config.stash(Arc::new(rc)),
            Err(msg) => eprintln!("[boot] persist config.last ignored: {msg}"),
        }
    }

    let ctx = ConnContext {
        bus,
        auth,
        registry,
        fact_tx,
        sink,
        // 连接准入（§10.4）：未鉴权连接上限 + 每 IP 限额
        admission: Arc::new(phira_server::server::ConnectionAdmission::default()),
        // PROXY protocol（§前置层：反代后真实 IP；yml `proxy_protocol`，默认关）
        proxy_protocol: config.proxy_protocol,
        // 鉴权阶段超时（C-01，yml `auth_timeout`）：版本字节确认后→鉴权完成之间
        auth_timeout: config.auth_timeout,
        // 进服欢迎语（yml welcome_message，None = 不发）
        welcome_message: config.welcome_message.clone(),
        // 管理面（阶段 2）：Bearer token（None = 禁用）+ 写操作审计环（持久化归档）
        admin_token: config.admin_token.clone(),
        admin_audit: phira_server::admin::AuditLog::new_with_file(&persist_dir),
        admin_config,
        admin_ban_observer: phira_server::server::BanObserver::new_with_file(&persist_dir),
        admin_anticheat: phira_server::server::AntiCheatObserver::new(),
        config_store: Arc::new(config_store),
        room_list,
    };
    let server = Server::new(
        config.listen,
        ctx,
        config.maintenance_notice.clone(),
        config.maintenance_grace,
        config.http_port,
    )
    .await?;
    // systemd 就绪通知（§部署）：bind 成功即"准备好接受连接"（配合 Type=notify）
    #[cfg(target_os = "linux")]
    {
        // sd-notify 0.4.5 API：notify(unset_env, &[NotifyState])
        if let Err(e) = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
            tracing::warn!("sd_notify failed: {e}");
        }
    }
    server.run().await?;
    Ok(())
}

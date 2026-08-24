//! # phira-server —— 老板（组合根，§4.1 / §4.5）
//!
//! **唯一认识所有人的 crate**。接线在 Day-1 清单第 5 步：
//! 决定谁上架（RoomsV1）+ 注入外部依赖（HTTP/随机）+ 柜台开业（Bus → Server）。
//! 换实现 = 组合根换工厂（§3.2：灰度已降级为运维选项，项目内零灰度代码）。

use std::sync::Arc;

use anyhow::Result;
use phira_api::{ApiClient, RandomSource, RoomConfig, RoomDeps, RoomFactory};
use phira_core::{Bus, Config, EventSink, lifecycle::LifecycleTask};
use phira_server::server::{ConnContext, Server, SessionSink};

/// 老板接线（§4.5）。
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
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn RoomFactory>,
        Arc::new(config.rooms.clone()) as Arc<RoomConfig>,
    );

    // 用户生命周期（§4.9-3）：单一生产者任务 + 注册表（重连窗口 = yml `reconnect_window`，§6.5-21）
    let (lifecycle_task, registry, fact_tx) =
        LifecycleTask::new(bus.clone(), config.reconnect_window);
    tokio::spawn(lifecycle_task.run());

    // 事件投递（§6.6 表 2）：user → 会话写通道
    let sink = Arc::new(SessionSink::new());
    bus.attach_sink(Arc::clone(&sink) as Arc<dyn EventSink>);

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

    let ctx = ConnContext {
        bus,
        auth,
        registry,
        fact_tx,
        sink,
    };
    Server::new(
        config.listen,
        ctx,
        config.maintenance_notice.clone(),
        config.maintenance_grace,
    )
    .await?
    .run()
    .await?;
    Ok(())
}

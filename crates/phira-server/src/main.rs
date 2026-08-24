//! # phira-server —— 老板（组合根，§4.1 / §4.5）
//!
//! **唯一认识所有人的 crate**。接线在 Day-1 清单第 5 步：
//! 决定谁上架（RoomsV1）+ 注入外部依赖（HTTP/随机）+ 柜台开业（Bus → Server）。
//! 换实现 = 组合根换工厂（§3.2：灰度已降级为运维选项，项目内零灰度代码）。

mod http;
mod server;

use std::sync::Arc;

use anyhow::Result;
use phira_api::{ApiClient, RandomSource, RoomConfig, RoomDeps, RoomFactory};
use phira_core::{Bus, Config};

/// 老板接线（§4.5）。
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = Config::load()?;

    // 老板接线：决定谁上架 + 注入外部依赖（§4.9-6）
    // 单一 HTTP 实例，auth 与 chart/record 共享（评审 §8 五-1）
    let http = Arc::new(http::HttpApiClient::new(config.api_base.clone()));

    let deps = RoomDeps {
        api: Arc::clone(&http) as Arc<dyn ApiClient>,
        rng: Arc::new(http::ThreadRngSource) as Arc<dyn RandomSource>,
    };

    // 第一个货物（工厂，持有 deps）；换实现 = 组合根换工厂
    let rooms = impl_rooms_v1::RoomsV1::new(config.rooms.clone(), deps);

    // 柜台开业：当前生效配置 = 组合根注入的初始配置（§4.9-8 热更走 Bus::update_config）
    let bus = Bus::new(
        Arc::new(rooms) as Arc<dyn RoomFactory>,
        Arc::new(config.rooms.clone()) as Arc<RoomConfig>,
    );
    // TODO(阶段 2): 鉴权编排（token → AuthHandler → 用户注册表 → 会话替换，§4.9-3）
    // let auth: Arc<dyn AuthHandler> = Arc::new(http::HttpAuth::new(config.api_base.clone()));
    // TODO(阶段 5): bus.watch_config（文件轮询 → update_config，机制已就绪）

    server::Server::new(config.listen, bus).await?.run().await?;
    Ok(())
}

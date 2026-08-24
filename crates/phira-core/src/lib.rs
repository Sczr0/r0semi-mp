//! # phira-core —— 柜台（§2.4 / §4.1）
//!
//! 会话 + 总线 + 配置。只依赖 phira-api（§4.3-2），不认识任何 impl。
//! 并发模型（§4.9）：每房间一个 actor、命令串行、`&mut self` 无锁；
//! 断线事实由用户生命周期任务单一生产者按序派发。

#![forbid(unsafe_code)]

pub mod bus;
pub mod config;
pub mod convert;
pub mod lifecycle;
pub mod session;

pub use bus::{Bus, CommandStats, EventSink, Metrics};
pub use config::{Config, ConfigError, DEFAULT_CONFIG_PATH};
pub use session::{LifecycleFact, SessionHandle};

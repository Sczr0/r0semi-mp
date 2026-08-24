//! # phira-api —— 契约 crate（§4.1）
//!
//! 只有类型 + 薄缝 trait，不依赖任何内部 crate（§4.3-1）。
//! 零 tokio、零运行时，只允许 thiserror/half/async-trait 等轻量基础库（§4.8 红线）。
//!
//! 内容 = 两层契约（§2.3 原则 1）：
//! - **协议层**（ClientCommand/ServerCommand/Message）：协议直接投影，阶段 1 与编解码同步落地
//! - **内部契约层**（rooms.rs / auth.rs）：改写产物，按设计对待（评审、演进、版本，§5.6）
//!
//! 本 crate 是"可换架构"的形式化基础：core 与 impl 只认识这里，互不认识。

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod auth;
mod binary;
mod proto;
mod rooms;

pub use auth::*;
pub use binary::*;
pub use proto::*;
pub use rooms::*;

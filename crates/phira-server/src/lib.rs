//! # phira-server —— 组合根库面（§4.5）
//!
//! bin-only 之外的 lib 面：`server`（监听/连接处理）与 `stream`（协议帧层）可被
//! 集成测试直接驱动（§4.9-6 可测性精神：组合根不豁免测试）。
//!
//! `http`（生产 HTTP/鉴权实现）也在此——唯一认识所有内部 crate 的地方。

pub mod http;

// 测试专用入口（http.rs 内 doc(hidden)，此处仅 re-export 供集成测试）
pub use http::http_get_with_tls;
pub mod server;
pub mod stream;

//! # phira-contract —— 契约测试套件库（§4.1 / §5.3）
//!
//! 泛型契约测试对 trait 编写，任何 impl 只传构造器即可全量验证。
//! 只依赖 phira-api（§5.2 依赖方向矩阵）。
//!
//! **V2 想上线？先过同一套契约测试**（§5.3）。

#![forbid(unsafe_code)]

pub mod rooms;

pub use rooms::{FakeApi, SeqRng, room_contract_suite, suite_deps};

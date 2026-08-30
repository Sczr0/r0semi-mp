//! 解码器模糊 target（libFuzzer 覆盖率引导）。
//!
//! 威胁模型同 `crates/phira-api/tests/fuzz.rs`（黑客灌垃圾字节流，解码器在
//! **任何输入**下不 panic，Ok/Err 均可）。此 target 与 proptest 的区别：
//! 覆盖率引导自动钻新路径（proptest 固定种子不会为了新分支牺牲可复现性），
//! 两者互补——常驻 CI 用 proptest，本 target 走手动/CI workflow 深度验证。
//!
//! 单个输入同时喂全部解码面：corpus 共享，一次执行探索四个类型（§6.2）。
//! panic = libFuzzer 记录输入并终止（fuzz/artifacts/ 留存，即回归测试素材）。

#![no_main]

use libfuzzer_sys::fuzz_target;
use phira_api::{ClientCommand, Message, RoomId, ServerCommand, decode_packet};

fuzz_target!(|data: &[u8]| {
    let _ = decode_packet::<ClientCommand>(data);
    let _ = decode_packet::<ServerCommand>(data);
    let _ = decode_packet::<Message>(data);
    let _ = decode_packet::<RoomId>(data);
});
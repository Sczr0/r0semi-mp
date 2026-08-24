//! impl-rooms-v1 的契约测试接入（§5.3）。
//!
//! 任何 impl 只传构造器即可全量验证——V1 能过，V2 也必须能过。

use impl_rooms_v1::RoomsV1;
use phira_api::RoomConfig;
use phira_contract::{room_contract_suite, suite_deps};

#[tokio::test]
async fn rooms_v1_passes() {
    let deps = suite_deps();
    let rooms = RoomsV1::new(RoomConfig::default(), deps);
    room_contract_suite(&rooms).await;
}

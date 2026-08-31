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
// ———— impl-rooms-v1 专属补充验证（契约套件之外的依赖注入场景）————

use phira_contract::SeqRng;
use std::sync::Arc;

fn ctx(user_id: i32) -> phira_api::CmdCtx {
    phira_api::CmdCtx {
        room_id: phira_api::RoomId::new("r".to_owned()).unwrap(),
        origin: phira_api::Origin::Client { user_id },
    }
}

fn rid() -> phira_api::RoomId {
    phira_api::RoomId::new("r".to_owned()).unwrap()
}

/// 回源必败 API（模拟官方后端不可用）。
struct FailingApi;

#[async_trait::async_trait]
impl phira_api::ApiClient for FailingApi {
    async fn fetch_chart(&self, _id: i32) -> Result<phira_api::Chart, phira_api::ApiError> {
        Err(phira_api::ApiError::Internal {
            msg: "upstream down".into(),
        })
    }
    async fn fetch_record(&self, _id: i32) -> Result<phira_api::Record, phira_api::ApiError> {
        Err(phira_api::ApiError::Internal {
            msg: "upstream down".into(),
        })
    }
}

#[tokio::test]
async fn select_chart_api_failure_maps_to_internal_error() {
    let deps = phira_api::RoomDeps {
        api: Arc::new(FailingApi),
        rng: Arc::new(SeqRng::default()),
    };
    let rooms = impl_rooms_v1::RoomsV1::new(phira_api::RoomConfig::default(), deps);
    let factory: &dyn phira_api::RoomFactory = &rooms;
    let mut room = factory.create(rid());
    let (resp, _) = room
        .handle(
            ctx(1),
            phira_api::RoomCommand::CreateRoom {
                id: rid(),
                name: "user1".to_owned(),
            },
        )
        .await;
    assert!(matches!(resp, Some(phira_api::RoomResponse::Ok)));

    // 选图回源失败 → RoomError::Internal（客户端可见通用文案，不算业务错误）
    let (resp, _) = room
        .handle(ctx(1), phira_api::RoomCommand::SelectChart { id: 1 })
        .await;
    assert!(
        matches!(
            resp,
            Some(phira_api::RoomResponse::Failure(
                phira_api::RoomError::Internal { .. }
            ))
        ),
        "回源失败应映射为 Internal: {resp:?}"
    );
}

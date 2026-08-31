//! 错误文案本地化（B2 技术债，§1.2 P1 协议兼容）。
//!
//! 对照官方原版 `phira-mp-server/src/l10n.rs`（Fluent 三语 en-US/zh-CN/zh-TW，
//! `task_local LANGUAGE` per-user 作用域 + `tl!` 宏）：原版仅本地化 **6 条**
//! 入房/开局错误（`locales/*.ftl`），其余文案（`already in room` 等）同为英文硬编码。
//!
//! ## r0semi 的实现取舍（与原版同结果、更省资源）
//!
//! - **不用 Fluent 运行时**：6 条静态文案 ×3 语言 = 常量表即可（原则 5：等第二个
//!   复杂需求出现再引 Fluent——复数/参数化时才需要）。零新依赖，P0 内存不受影响。
//! - **EN 表对齐原版 ftl 的 Title Case 措辞**（非 impl 现行小写）：`lang=en`
//!   用户看到的文案与官服 en-US 逐字一致（ISSUE-0013 方案 A，2026-08 拍板）。
//!   不依赖"字节级不变"：Oracle 用自构造输入（见 docs/oracle.md），不经过本出口；
//!   现有断言不锁 EN 表值，均不受影响。
//! - **per-user 作用域 = 会话槽位**：原版用 `tokio task_local` 包裹命令处理；
//!   本项目命令经 bus 异步流转到 impl actor，无法传递 task_local。改为把
//!   [`Locale`] 存进 [`crate::server::SendSlot`](会话槽位)——随连接生灭，
//!   无影子表泄漏（C2 同款纪律）。
//! - **翻译点在 server（协议出口）**：impl 返回英文 msg + 结构化
//!   [`RoomErrorCode`]；`handle_frame` 出口按发起者语言将**有映射的错误码**替换为
//!   本地化文案。未映射的新错误优雅回落英文（对齐原版行为集，不丢失信息）。
//!
//! # 客户端可见性（P1 协议兼容）
//!
//! 原版对 zh 用户本就返回中文 → 本地化不是行为破坏，而是向原版语义对齐。

/// 支持的语言（对照原版 `LANGS: ["en-US", "zh-CN", "zh-TW"]`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Locale {
    /// 英文（默认回落）。
    #[default]
    EnUs,
    /// 简体中文。
    ZhCn,
    /// 繁体中文。
    ZhTw,
}

impl Locale {
    /// 从鉴权响应的 `language` 字符串解析（宽松匹配；未知值回落 en-US，
    /// 对齐原版 `.parse().map(Language).unwrap_or_default()` 的兜底语义）。
    #[must_use]
    pub fn from_lang_str(lang: &str) -> Self {
        let l = lang.trim();
        // 区分繁体：zh-TW / zh-HK / zh-Hant 前缀视为繁体
        if l.starts_with("zh") && (l.contains("TW") || l.contains("HK") || l.contains("Hant")) {
            return Self::ZhTw;
        }
        if l.starts_with("zh") {
            return Self::ZhCn;
        }
        Self::EnUs
    }

    /// 可本地化的错误 key（对齐原版 ftl key 名，便于 Oracle 字节对照溯源）。
    #[must_use]
    pub const fn text(self, key: Key) -> &'static str {
        const TABLES: [[&str; KEY_COUNT]; 3] = [EN, ZH_CN, ZH_TW];
        let locale_idx = match self {
            Self::EnUs => 0,
            Self::ZhCn => 1,
            Self::ZhTw => 2,
        };
        TABLES[locale_idx][key as usize]
    }
}

/// 可本地化错误的 key（枚举即文档：每条对应原版 ftl 或本项目新增）。
///
/// 对齐原版 6 条入房/开局文案；`TooManyRequests` 为本项目限速新增（ADR-0008/D1）。
#[derive(Debug, Clone, Copy)]
pub enum Key {
    /// CreateRoom：房间 ID 已被占用（原版 `create-id-occupied`）。
    CreateIdOccupied,
    /// JoinRoom：房间已锁定（原版 `join-room-locked`）。
    JoinRoomLocked,
    /// JoinRoom：游戏正在进行中（原版 `join-game-ongoing`）。
    JoinGameOngoing,
    /// JoinRoom：无观战权限（原版 `join-cant-monitor`）。
    JoinCantMonitor,
    /// JoinRoom：房间已满（原版 `join-room-full`）。
    JoinRoomFull,
    /// RequestStart：尚未选择谱面（原版 `start-no-chart-selected`）。
    StartNoChartSelected,
    /// 本项目限速（D1 / ADR-0008）。
    TooManyRequests,
}

const KEY_COUNT: usize = 7;

impl Key {
    /// 取该 key 在指定语言下的文案（[`Locale::text`] 的 key 侧封装）。
    #[must_use]
    pub const fn localized(self, locale: Locale) -> &'static str {
        locale.text(self)
    }
}

// 文案顺序严格对应 Key 枚举序；编译期断言见表数与 KEY_COUNT 一致（下方 tests）。
// EN 表为原版 en-US.ftl 的 Title Case 逐字措辞（ISSUE-0013 方案 A）；
// `TooManyRequests` 为 r0semi 新增 key，原版无对应 ftl，保留小写。
const EN: [&str; KEY_COUNT] = [
    "Room ID is occupied",
    "Room is locked",
    "Game is ongoing",
    "Permission denied. You can't monitor this room.",
    "Room is full",
    "No chart selected",
    "too many requests",
];

const ZH_CN: [&str; KEY_COUNT] = [
    "房间 ID 已被占用",
    "房间已锁定",
    "游戏正在进行中",
    "权限不足，不能旁观房间",
    "房间已满",
    "还没有选择谱面",
    "请求过于频繁",
];

const ZH_TW: [&str; KEY_COUNT] = [
    "房間 ID 已被佔用",
    "房間已鎖定",
    "遊戲正在進行中",
    "權限不足，不能旁觀房間",
    "房間已滿",
    "還沒有選擇譜面",
    "請求過於頻繁",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// 三张语言表的条目数与 Key 枚举一致（漏译编译期/测试期即断言）。
    #[test]
    fn tables_align_with_keys() {
        assert_eq!(EN.len(), KEY_COUNT);
        assert_eq!(ZH_CN.len(), KEY_COUNT);
        assert_eq!(ZH_TW.len(), KEY_COUNT);
        // 每个组合都非空
        for locale in [Locale::EnUs, Locale::ZhCn, Locale::ZhTw] {
            let keys = [
                Key::CreateIdOccupied,
                Key::JoinRoomLocked,
                Key::JoinGameOngoing,
                Key::JoinCantMonitor,
                Key::JoinRoomFull,
                Key::StartNoChartSelected,
                Key::TooManyRequests,
            ];
            for k in keys {
                assert!(!locale.text(k).is_empty());
            }
        }
    }

    /// 语言解析：zh 变体区分繁简，未知回落 en-US（对齐原版 unwrap_or_default）。
    #[test]
    fn from_lang_str_parses_and_falls_back() {
        assert_eq!(Locale::from_lang_str("zh"), Locale::ZhCn);
        assert_eq!(Locale::from_lang_str("zh-CN"), Locale::ZhCn);
        assert_eq!(Locale::from_lang_str("zh-TW"), Locale::ZhTw);
        assert_eq!(Locale::from_lang_str("zh-HK"), Locale::ZhTw);
        assert_eq!(Locale::from_lang_str("zh-Hant"), Locale::ZhTw);
        assert_eq!(Locale::from_lang_str("en-US"), Locale::EnUs);
        assert_eq!(Locale::from_lang_str(""), Locale::EnUs);
        assert_eq!(Locale::from_lang_str("fr"), Locale::EnUs);
    }
}

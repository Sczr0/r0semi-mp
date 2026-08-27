# ISSUE-0013：l10n EN 表 Title Case 决策点——镜像 impl 措辞 vs 原版 Fluent 措辞

- 状态：**待解决（低危，产品决策，owner 定夺）**
- 发现日期：2026-08
- 发现方式：client-behavior-review.md §7 三语校对（r0semi l10n.rs ↔ 原版 server ftl ↔ 客户端 multiplayer.ftl 逐条比对）
- 严重级：低（纯文案，无功能影响）
- 相关章节：ARCHITECTURE.md §6.4（规则 14 鉴权身份含 language）；`crates/phira-server/src/l10n.rs`；client-behavior-review.md §7

## 问题陈述

B2（i18n 按用户 language 本地化）落地后，EN 表存在两套措辞的取舍：

- **当前**：r0semi EN 表刻意镜像 impl 现行英文（如 `room id occupied`、`game is ongoing`、`no monitor permission`），
  以保证"本地化前后字节不变"（l10n.rs:12-14 的声明）；
- **原版**：官方服务端 Fluent en-US 为 Title Case 措辞（`Room ID is occupied`、`Game is ongoing`、`No chart selected`）。

## 证据

| key | 原版 en-US | r0semi EN | 一致? |
|---|---|---|---|
| create-id-occupied | Room ID is occupied | room id occupied | ❌ 大小写 |
| join-game-ongoing | Game is ongoing | game is ongoing | ❌ 大小写 |
| join-room-full | Room is full | room is full | ❌ 大小写 |
| join-room-locked | Room is locked | room is locked | ❌ 大小写 |
| join-cant-monitor | Permission denied. You can't monitor this room. | no monitor permission | ❌ 整句不同 |
| start-no-chart-selected | No chart selected | no chart selected | ❌ 大小写 |

zh-CN/zh-TW 已与官方逐字一致（2026-08 校验通过）；**仅 EN 表待定**。

## 影响评估

- 切到原版措辞：更像官服（真客户端透出错误文案时与官服一致），代价 = 自家 Oracle 字节基线/测试索引用例同步；
- 维持现状：零改动成本，但 EN 用户看到的是"自造措辞"。
- 真客户端把 auth/操作错误字符串透出到 UI（panel.rs show_error）→ 文案用户可见，值得定夺。

## 候选解决方案

- **方案 A（推荐走）**：切到原版 Title Case——B2 的初衷就是"对照原版 Fluent 语义"，EN 顺手对齐；
  需同步更新 phira-api 相关测试断言与 Oracle 基线。
- **方案 B**：维持现状并删掉 l10n.rs 里"刻意镜像"的声明（不再承诺字节不变）。

## 验收标准

- owner 拍板采用 A 或 B；
- 若 A：EN 六键与原版逐字一致 + 断言/Oracle 同步 + 全测试绿。

## 关联

- client-behavior-review.md §7（本文档只登记不动码，它明确说了"建议记入 issue 由 owner 定夺"）
- B2 i18n 本地化（已解决 2026-08）
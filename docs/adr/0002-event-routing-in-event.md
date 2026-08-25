# ADR-0002：事件寻址——事件自带路由，core 不持影子状态

- 日期：2026-08
- 状态：已接受
- 相关章节：ARCHITECTURE.md §4.4、§4.9-5、§6.6

## 背景

core 要"广播"却不知道发给谁：领域事件投递给房内成员+观察者，转发指令（RelayTouches/Judges）只给 monitor。若 core 维护影子成员表，必然与 impl 的实际状态漂移。原版是"广播时自己遍历 `users()+monitors()`"（core 复制房间状态）。

## 决策

`RoomEvent` 携带 `room_id + targets`（§4.4 分类学）：
- 领域事件：投递目标恒为房内 All（已核实无全服广播）——由 core 路由表反解
- 转发指令（RelayTouches/RelayJudges）：`targets = Specific(monitor_ids)`——由 impl 计算（角色在 actor 内）
- core 信号（RoomClosed）：仅 core

路由表只存 `user → room_id` 元数据（id 而已，不复制任何房间状态）；core 只执行投递。

## 后果

- 正面：core 零房间状态复制，影子状态漂移问题从根上消除；impl 换实现不碰 core；targets 成为事件契约的一部分（有版本、可演进）。
- 负面：`targets` 是**改写产物**（协议中不存在的概念），必须按设计对待——纳入评审、契约测试断言投递目标（§6.6 表 2）。

## 替代方案

- core 持影子成员表——被拒：必然漂移。
- 事件不带 targets、core 广播时遍历所有连接再过滤——被拒：core 需要知道"谁在哪个房间"，即复制状态。

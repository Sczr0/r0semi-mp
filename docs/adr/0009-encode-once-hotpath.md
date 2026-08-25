# ADR-0009：热路径编码一次共享（方案 2——缓存组件化，非完整方案 A）

- 日期：2026-08
- 状态：已接受
- 相关章节：ARCHITECTURE.md §6.5-17（方案 A）、§4.8-2（Bytes 选型）、ISSUE-0003

## 背景

§6.5-17 方案 A 承诺"core 编码一次为 Bytes 共享给所有 monitor，每接收者 0 次编码"——但 ISSUE-0003 发现实际是每接收者各自 `event_to_server` + `encode_packet`（帧数据 `Arc` 浅共享已兑现，转换+编码重复 M 遍）。根因：EventSink 的"每用户一次调用"形状 + RoomListSink 需要原始事件 + 编码在最下游写任务。

## 决策

**方案 2（缓存组件化，非完整方案 A）**：

1. **`Outbound` 消息类型**：发送通道消息 = `Command(ServerCommand)`（写任务编码）| `Encoded(Arc<Vec<u8>>)`（已编码载荷直写）。`Outbound` 实现 `BinaryData`——Encoded 分支经 `BinaryWriter::write_raw` 直写缓存字节，Stream 通用编码路径不变（客户端模式无感）。
2. **`EncodeCache`（独立组件）**：热路径（Touches/Judges）按**帧 Arc 指针**缓存编码载荷（容量 64，满则清）。同一帧的多个 monitor 投递命中同一缓存——**每命令 1 解 + 1 编，每接收者 0 次**（方案 A 的核心承诺兑现于热路径）。
3. **转换层不做去重**（event_to_server 仍每接收者一次）——转换是轻量结构组装（match + 克隆 Arc），编码是重量序列化；只去重编码已兑现文档承诺的核心。
4. **EventSink 签名不变**、RoomListSink 无感、impl 不碰编码（§4.3-3 红线保持）。

## 后果

- 正面：热路径（live 房间观战转播）每命令 1 编、每接收者 0 次——文档方案 A 核心兑现；改动集中在 SessionSink 投递层 + 写路径，契约 crate（phira-api）仅加 `BinaryWriter::write_raw`（编解码工具，非协议语义）；写任务对 Encoded 直写共享 `Arc<Vec<u8>>`（一次 memcpy 到写缓冲，无序列化）。
- 负面：**非热路径**（Chat/状态变更等）仍每接收者编码（频率低，秒级，可接受）；缓存淘汰是"满则清"（简单，每帧最多命中一次，清后下一帧重编一次）；EncodeCache 容量/淘汰策略是常量。

## 泛化触发条件（何时升级到完整方案 A / 方案 3）

按文档原则 5（抽象时机）：**第二个"大扇出广播"场景出现时才泛化**——如全服公告（几千人）、观战人数爆炸、跨房间事件转发。届时把 EncodeCache 从 SessionSink **提升到 bus 层**（缓存 key 从"帧 Arc 指针"改为"事件批次"），EventSink 加批次方法——**是提升不是重构**（EncodeCache 已是独立组件，数据结构可复用）。此触发条件不满足前，方案 2 是正确抽象层级。

## 替代方案

- 完整方案 A（方案 3：所有事件编码一次 + EventSink 批次重构）——被拒：为未出现的"大扇出"提前抽象（原则 5）；非热路径编码频率低，收益边际。
- 转换去重（bus 层按事件分组 event_to_server 一次）——暂缓：收益 < 编码去重（转换轻量）；未来与缓存提升一并做。

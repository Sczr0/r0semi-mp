# ADR-0001：并发模型——每房间一个 actor

- 日期：2026-08
- 状态：已接受
- 相关章节：ARCHITECTURE.md §4.9、§1.5、§4.7

## 背景

`&self + Send + Sync` 的形状对实现形态（单例+锁 / actor）是决定性猜测。原版（读码核实）是"会话独立 task + 房间级 RwLock + Weak 引用图"，dangle 检测靠 `Weak::upgrade` 失败；社区重写中 TS 版用单线程事件循环、gooophira 用全局 `ServerState.Mu` 串行化所有命令。三种前例的串行点分别落在"全局 / 每连接 / 锁内"，各有代价（全局锁一个房间 HTTP 卡全服；每连接线程内存重；Weak 图竞态温床）。

## 决策

每房间一个 actor 任务 + 有界 mpsc channel，命令 FIFO 串行进入，`&mut self` 独占状态无锁；**时间与连接事实也命令化**（`Tick`/`UserDisconnected`/`UserDangleExpired`），由用户生命周期任务单一生产者按序派发。core 的房间表是 `HashMap<RoomId, Sender<Envelope>>`，actor 跑在自己的任务里（core 持有的只是 channel sender，销毁即 drop sender）。

## 后果

- 正面：串行点 = 房间这个语义边界——正确性（房间内必须有序）与性能（房间间并行）同时成立；零锁零 CAS；命令串行使契约测试可穷举；单一生产者消除系统命令乱序。
- 负面：相对原版的"锁外 HTTP"是**行为回退**——`Played` 回源校验期间该房命令全部排队（8 条成绩串行回源时尾玩家等 8×RTT，§4.9-2 缓解：热路径可丢 + 每连接限速 + 结算突发可预期）。

## 替代方案

- 全局共享锁串起所有房间——被拒：一个房间的 HTTP 回源卡全服。
- 单例 Handler + 组合根枚举分发回避 dyn——被拒：actor 模型下 core 只能持有 channel sender，`Box<dyn RoomActor>` 成为必然容器，枚举分发无法覆盖。
- 原版"会话独立 task + 房间 RwLock + Weak 图"——被拒：Weak 生命周期纠缠是竞态温床（§4.6-3）。

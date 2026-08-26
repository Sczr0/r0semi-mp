# 真客户端行为一致性：已证实怪癖清单与验证体系规划

> 2026-08 通过对 **Phira 客户端开源源码**（TeamFlos/phira 主仓 + 本地 `phira-mp-client` crate，
> 即真客户端 `phira/Cargo.toml:54` 集成的同一库）的源码审计，
> 验证了竞品 `protocol_hack` 层声称的"客户端怪癖"，并据此提出 r0semi 独有的一致性验证路径。
> 背景认识论问题：**字节级 Oracle 验证的是"编码正确"，而客户端怪癖是"行为正确"——两者正交。**
> 但 Phira 开源使行为正确从"神秘的兼容性玄学"降维为"可审计的源码事实"。

## 三层证据等级

| 层级 | 来源 | 可信度 | 例子 |
|---|---|---|---|
| ① **客户端源码** | TeamFlos/phira + phira-mp-client | **可证明**（人人可复现） | 本文档验证的怪癖 |
| ② **生产观察** | gooophira/jphira 的 hack 注释 | 合理假设，指向真实现象，但参数是经验值 | "延迟 10ms" |
| ③ **字节对比** | r0semi Oracle | 只覆盖编码层，不覆盖行为 | 64/64 一致 |

其他服务端没做过字节对比 ≠ 其行为经验无价值——它们的价值恰在字节对比照不到的那层（客户端状态机），
只是**它们自己也说不清原理**，只能留 `"2ms 应该够"` 这类经验值。持源码可把经验值重新推导/校准。

## 已证实的怪癖（全部有 exact 源码定位）

### 怪癖一：时序窗口能让真客户端 panic（裸 unwrap）

`phira-mp-client/src/lib.rs` 的 `process()`（line 422）处理房间事件用裸 `.unwrap()`：

```rust
// lib.rs:455-466（Message::LockRoom/CycleRoom/LeaveRoom）
state.room.write().await.as_mut().unwrap().locked = lock;
state.room.write().await.as_mut().unwrap().cycle = cycle;
// lib.rs:477（ChangeState）
let state = guard.as_mut().unwrap();
```

而客户端本地房间状态的建立时机存在**竞态窗口**：

```rust
// lib.rs:294（create_room）：先等响应回来，再本地"伪造"房间状态
self.rcall(ClientCommand::CreateRoom{..}).await?;
*self.state.room.write().await = Some(ClientRoomState { locked:false, cycle:false, is_host:true, ... });
```

**窗口**：服务器处理完 CreateRoom 后即可推事件，但客户端要等响应返回才在本地构造 `state.room`。
窗口内到达 `LockRoom`/`ChangeState`/`LeaveRoom` → 踩裸 unwrap → **真客户端 panic**。
这就是 gooophira "延迟 10ms 再发后续消息"的机制根源——不是玄学，是在给客户端本地状态构造让路。

### 怪癖二："观战者幻觉"的机制根源（静默丢弃）

```rust
// lib.rs:493：客户端的 live 标志只由 monitor 加入设置
ServerCommand::OnJoinRoom(user) => if let Some(room) = state.room.write().await.as_mut() {
    room.live |= user.monitor;
}
```

更关键的是处理方式不对称：

```rust
// Message::LeaveRoom（lib.rs 附近）：裸 unwrap → 窗口期会 panic
// OnJoinRoom：if-let → 窗口期**静默丢弃**
```

**建房瞬间的加入事件会被静默吞掉**——房主客户端用户列表从此永久缺员（直到下次全量刷新）。
gooophira `forceSyncInfo`"补发假观战者加入/离开来修复客户端状态"正是对它的精确补偿。
（注：该怪癖描述中"触发回放录制"在当前客户端 master v0.8.2 已**找不到对应实现**——见"怪癖漂移"。）

### 附：重连恢复机制（r0semi 已做对）

```rust
// lib.rs:271（authenticate）覆盖整个本地房间状态
let (me, room) = self.rcall(Authenticate{..}).await?;
*self.state.room.write().await = room;   // 快照覆盖
```

客户端重连后的状态恢复完全依赖 Authenticate 响应里的 `Option<ClientRoomState>` 快照——
r0semi 的 GetClientState 路径与之一致，无需 fixClientRoomState 类补偿（前提：快照准确）。

## 怪癖漂移实例（为何不能信任传闻）

```
gooophira protocol_hack.go 注释：
"建房后服务端要追加『观战者已就位』的幻觉以【触发回放录制】"

当前客户端 master（v0.8.2，2026-08-17）：
grep -rn ".phirarec|recorder" → 零结果
grep -rn "live_players" phira/src → 零引用（库定义了但 UI 层无消费）
```

**jphira/gooophira 时代补偿的"回放录制触发"，在当前客户端代码里已不存在。**
传闻瞄准的是某历史客户端版本；客户端在演化，怪癖会过期。因此"信任黑盒"必然失真，
必须**以源码为准并定期复核**。

## 结论与路线图：r0semi 的独特机会

r0semi 的 interop 测试用的 `phira-mp-client` **就是真客户端集成的同一个 crate**——这让它可以做一件
其他四家都做不到的事：

> **把"怪癖传闻"升级为"针对真客户端库的可执行断言"**——写一致性测试直接实例化
> `phira_mp_client::Client` 连上自己的服务器，在对抗性但真实的事件序列下断言：
> 客户端库不 panic、本地状态与服务器一致。

这使 protocol_hack 不再是"抄 Go 版黑魔法"，而变成 r0semi 式合同条款：
"任何满足这些序列性质的服务器实现，都不会崩掉真客户端"。

### 一致性验证体系设计（"永续方案"）

```text
1. 锁版本   → interop 测试钉住具体 commit："client@51b05cb 全绿"
2. 建断言库 → 对抗性序列 × phira_mp_client::Client
             "建房瞬间并发 LockRoom → 断言不 panic 且状态一致"
3. 审计UI层 → phira/src/mp/panel.rs（825行）+ song.rs 假设清单化，并入服务器行为规格
4. 装漂移哨兵 → CI 定期 diff 上游 mp 相关文件，变更即重跑全部兼容性测试并更新怪癖文档
5. 保三足鼎立 → Oracle(字节) + Conformance(行为) + Contract(自身)，缺一不可
```

### 明确边界（不能"彻底根治"的部分）

1. **版本碎片化**：验证的是 master；玩家跑应用商店/APK 发布包，跨版本分布。需按活跃版本矩阵逐版核验。
2. **UI/场景层未审完**：panel.rs 已见 `blocking_is_host().unwrap()` 同级地雷；完整审计工作量可控但非零。
3. **源码给方向、参数靠实测**：竞态窗口可证明，但"窗口多宽"取决于低端手机处理速度——"10ms 够不够"
   源码回答不了，需真机联调。
4. **协议外的兼容面不在开源范围**：上游 REST API（/me、/chart、/record）服务端闭源，
   其行为变更会让鉴权/取谱面断掉，与 MP 协议无关（r0semi 手写 HTTP 客户端只认 200 的债务在此放大）。
5. **野生改版客户端**：私有服社区常见改地址的补丁客户端，其行为差异任何开源仓库都无法预期，只能线上遥测兜底。

## 一句话结论

> 客户端开源把"不兼容"从无解猜谜变成了**有界工作量**：
> 协议层可接近根治（双端源码 + 三层验证体系，r0semi 已具备全部原料）；
> 时序参数层靠源码给边界 + 实验给数字；
> 分发现实层永远只能靠监控与遥测缓解。
> 而"用 CI 自动维持、随客户端演进自动更新的兼容性保障体系"——是五家中没有任何一家拥有（或意识到可以拥有）的能力。

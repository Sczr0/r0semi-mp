# 部署 / 迁移 / 重建清单

服务器**无状态 + 单二进制 + 可移植**——任何机子半小时起一套，换机（小厂→腾讯云）走同一份清单。

## 1. 获取二进制

**推荐：GitHub Actions release job**（CI 已配置，main 分支 push 自动产出）：

```bash
# Actions 页面 → 最新 workflow → r0semi-mp-server-linux artifact → 下载
# 或本机构建（需 Rust 1.98，rust-toolchain.toml 钉死）：
cargo build --release -p phira-server
# 产物：target/release/r0semi-mp-server（Linux）/ r0semi-mp-server.exe（Windows）
```

## 2. 服务器配置（server_config.yml，与二进制同目录）

完整样例见仓库根 `server_config.example.yml`（每项含默认值注释，2026-08 与代码对齐）：

```yaml
# 可选——全部字段都有默认值；只写要覆盖的
listen: "0.0.0.0:12346"
api_base: "https://phira.5wyxi.com"   # 回源官方（阶段 4 TLS 已解锁）
monitors: [2]                          # 观战者白名单（§6.5-4）
reconnect_window: 10                   # 断线重连窗口（秒；非对局，默认 10）
playing_reconnect_window: 60          # 对局中断线重连窗口（秒；C-03/ADR-0012，默认 60）
auth_timeout: 10                       # 鉴权阶段超时（秒；C-01，默认 10）
http_timeout: 5                        # 回源 HTTP 超时（秒）
maintenance_grace: 10                  # 停机宽限窗口（秒）
config_poll_interval: 2                # 配置文件轮询（秒）
maintenance_notice: "服务器维护中，房间即将关闭，请稍后再来"
persist_dir: "./data"                 # 管理面持久化目录（bans/audit/config 快照，自动创建）
admin_token: "changeme"               # 管理面 Bearer token（不配 = 管理面整体禁用）
http_port: 8080                        # 管理 HTTP 端口（/healthz + /rooms + /admin/*）
# welcome_message: "欢迎语"           # 进服欢迎语（不配 = 不发）
# hidden_room_prefixes: ["solo"]      # 私密房间前缀（/rooms 不展示）
# proxy_protocol: false               # PROXY protocol（反代真实 IP）
```

> 环境变量覆盖文件：`R0SEMI_MP_PORT` / `R0SEMI_MP_API_BASE` / `R0SEMI_MP_CONFIG`（文件路径）
> / `R0SEMI_MP_ADMIN_TOKEN` / `R0SEMI_MP_PERSIST_DIR`

## 3. systemd 服务（Linux）

```ini
# /etc/systemd/system/r0semi-mp.service
[Unit]
Description=r0semi-mp server
After=network-online.target

[Service]
Type=simple                  # 不等待 READY 通知：部署稳定优先（Type=notify 需二进制含 sd-notify，
                             # 否则 systemd 等 90s 超时触发 on-failure 重启循环——2026-08 生产踩坑）
WorkingDirectory=/opt/r0semi-mp
ExecStart=/opt/r0semi-mp/r0semi-mp-server
Restart=on-failure
RestartSec=3
# 优雅停机（§11）：SIGTERM → 维护广播 → 宽限窗口 → 退出
KillSignal=SIGTERM
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable --now r0semi-mp
journalctl -u r0semi-mp -f
```

## 4. 安全组 / 防火墙

- **只放行** `12346/tcp`（或 yml 自定义端口）
- SSH 限制来源 IP
- 其余端口全关

## 5. DNS（域名 r0semi.net）

```
A    mp.r0semi.net     → 服务器公网 IP
SRV  _phira._tcp.r0semi.net → mp.r0semi.net 12346   （可选，客户端填 r0semi.net 免端口）
```

客户端两种填法全通：`mp.r0semi.net:12346`（直连）或 `r0semi.net`（SRV）。

> 国内机 + 域名：**ICP 备案**是前置（用户已备案）。香港/海外机免备案。

## 6. 上线验证

```bash
# 1. 服务在跑
curl -s telnet://<IP>:12346 || true   # 或 nc <IP> 12346
# 2. 日志无异常
journalctl -u r0semi-mp | tail
# 3. 协议探活：发握手 0x01 + Ping 帧 → 收 Pong
# 4. 真客户端：Phira App → 填 mp.r0semi.net:12346 → 真 token 登录 → 开房联机
```

## 7. 迁移 / 重建清单（换机全流程，约 30 分钟）

| 步骤 | 操作 |
|---|---|
| 1 | 新机（任意云商）：装系统（Debian/Ubuntu 纯净版，**不要宝塔面板**） |
| 2 | 下载 CI release 二进制 → 放到 `/opt/r0semi-mp/` |
| 3 | 写 `server_config.yml`（同上模板） |
| 4 | 配 systemd 服务 |
| 5 | 安全组放行 12346 |
| 6 | 改 DNS：A 记录指向新 IP（SRV 不用动，target 不变） |
| 7 | 玩家等 DNS 生效（几分钟）重连——旧房已随旧机消失，重新开房即可 |

**无数据库迁移、无数据备份**——服务器内存态，停机即丢房（§11 设计如此），迁移损失 = 正在进行的房间。

## 8. 连接准入（§10.4，已落地）

| 防护 | 行为 |
|---|---|
| 握手超时 | connect 后 5s 不发版本字节 → 断开 |
| 未鉴权连接上限 | 全局 100 |
| 每 IP 限额 | 每 IP 5 个未鉴权连接 |
| 鉴权后帧上限 | 4KiB → 2MiB（鉴权通过放开） |

**部署安全姿势**：云商 DDoS 防护（基础版即可）+ 安全组收紧 + 上述协议层准入 = 自用规模足够。

## 9. 常见问题

- **连不上**：安全组没放行 / 服务器没起 / DNS 未生效 / 备案未过（大陆机）
- **鉴权失败**：回源官方 API 不通（官方挂了则鉴权/选图/结算全断——所有 phira-mp 服务器共同依赖）
- **内存**：~12MB（debug 实测）/ 预算 7-15MB（release）——1G 机子绰绰有余
- **"房间 ID 已被占用"但明明没人用**：CreateRoom 无幂等键（协议级限制，ISSUE-0010）——建房请求
  响应丢失后客户端同 id 重试必得此错。**指引：建房 id 建议带唯一后缀**（如 `xxx-8f3k`），
  撞 id 时直接换一个新 id 而不是原样重试；孤儿房风险见 `docs/issues/0010`。

## 10. Docker 部署（D-01）

仓库根 `Dockerfile`（multi-stage，clux/muslrust:1.98.0 产 musl 静态二进制，与 CI release 同工具链）
+ `docker-compose.yml`（一条命令起服）：

```bash
docker compose up -d --build        # 一条命令起服
```

**验收**（游戏端口 12346 + 管理 HTTP 8080）：

```bash
curl -s http://127.0.0.1:8080/healthz    # 期望 HTTP 200（healthz JSON）
# 游戏端口握手（发版本字节 0x01 + Ping 帧 `01 00` → 收 Pong `01 00`）：
python -c "import socket,time;s=socket.create_connection(('127.0.0.1',12346));s.sendall(b'\x01');time.sleep(.2);s.sendall(b'\x01\x00');print(s.recv(64).hex())"
```

覆盖配置：`cp server_config.example.yml server_config.yml` 修改后，取消 compose 中
`./server_config.yml:/app/server_config.yml:ro` 挂载注释；管理面数据持久化在 named volume
`r0semi-mp-data`（bans/audit/config 快照）。STOPSIGNAL SIGTERM → §11 优雅停机。

> **静态验证（无 Docker 环境时）**：Dockerfile 构建命令与 CI release job 完全一致
> （`cargo build --profile release-dist --target x86_64-unknown-linux-musl --bin r0semi-mp-server`，
> 产物路径 `target/x86_64-unknown-linux-musl/release-dist/r0semi-mp-server`，见 `.github/workflows/ci.yml`）；
> 镜像内默认 `server_config.yml`（12346 + http_port 8080 + persist /app/data）与 §2 字段集合一致
> （`crates/phira-core/tests/example_yml.rs` 钉死解析）。`/healthz` 由 HTTP 端口提供
> （`crates/phira-server/tests/healthz.rs` 覆盖），无需回源 API。

# syntax=docker/dockerfile:1
# r0semi-mp-server 运行时镜像（D-01）
#
# 策略：multi-stage——builder 用 clux/muslrust（自带 musl 工具链，ring 无系统依赖问题；
# tag 钉死 `1.98.0-stable`——裸 `1.98.0` 在 Docker Hub 不存在，2026-08 CI 实测修正）
# 产 musl **静态**二进制；运行时镜像只带二进制 + 配置。
# 注：CI release job 是 runner 直接构建（dtolnay 装 target + apt musl-tools，ci.yml），
# 本容器独立但同 rust 1.98.0 + release-dist profile + musl target（产物等价）。
# TLS 回源用 webpki-roots 内嵌根证书（§10.3），无需系统 CA；alpine 提供 busybox
# wget 供 /healthz 容器探针。
FROM clux/muslrust:1.98.0-stable AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# 与 CI release 一致：release-dist profile（strip + fat LTO）+ musl target
RUN cargo build --profile release-dist --target x86_64-unknown-linux-musl --bin r0semi-mp-server

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release-dist/r0semi-mp-server /app/r0semi-mp-server
# 样例配置入库（D-02）：用户可据此生成 server_config.yml 挂载覆盖
COPY server_config.example.yml /app/server_config.example.yml
# 开箱默认配置：管理 HTTP（/healthz + /rooms + /admin/*）端口 8080，数据持久化 /app/data。
# 生产覆盖方式：`./server_config.yml:/app/server_config.yml:ro` 挂载（见 docker-compose.yml）。
COPY <<'EOF' /app/server_config.yml
listen: "0.0.0.0:12346"
http_port: 8080
persist_dir: "/app/data"
EOF
# 优雅停机（§11）：SIGTERM → 维护广播 → 宽限窗口 → 退出
STOPSIGNAL SIGTERM
EXPOSE 12346 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
CMD ["/app/r0semi-mp-server"]

#!/usr/bin/env bash
# r0semi-mp 一键部署（Debian/Ubuntu，root 直接执行）
# phira 服务端口 = 3939（按部署需求写死）
# 用法：curl 下载后 `bash deploy.sh`
set -euo pipefail

PORT=3939

echo "===== r0semi-mp 部署（端口 $PORT）====="

# 1. 建目录 + 下载 nightly 二进制（GitHub 公开 Release，匿名可下）
mkdir -p /opt/r0semi-mp
cd /opt/r0semi-mp
if [ ! -x ./r0semi-mp-server ]; then
  echo "[1/3] 下载二进制..."
  curl -fL -o r0semi-mp-server \
    https://github.com/Sczr0/r0semi-mp/releases/download/nightly/r0semi-mp-server
  chmod +x r0semi-mp-server
fi
echo "[1/3] 二进制就绪: $(ls -la r0semi-mp-server | awk '{print $5}') bytes"

# 2. 配置（监听 $PORT）
cat > server_config.yml <<YML
listen: "0.0.0.0:$PORT"
YML
echo "[2/3] 配置就绪: listen 0.0.0.0:$PORT"

# 3. systemd 服务（Type=simple：不等待 READY 通知——Type=notify 需二进制含 sd-notify，
#    否则 systemd 等 90s 超时 → on-failure 重启循环，2026-08 生产踩坑；SIGTERM 优雅停机 §11）
cat > /etc/systemd/system/r0semi-mp.service <<'UNIT'
[Unit]
Description=r0semi-mp server (phira multi-room)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/r0semi-mp
ExecStart=/opt/r0semi-mp/r0semi-mp-server
Restart=on-failure
RestartSec=3
KillSignal=SIGTERM
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now r0semi-mp
sleep 2

echo "[3/3] 状态确认:"
systemctl status r0semi-mp --no-pager | head -10 || true
echo "--- 监听检查 ---"
ss -tlnp | grep "$PORT" && echo "==> $PORT 监听中 ✓" || echo "!! $PORT 未监听（看上方状态）"

echo ""
echo "===== 部署完成 ====="
echo "下一步：NAT 面板加端口映射：公网 $PORT → 内网 $PORT"
echo "DNS：A re0.r0semi.net → 本机公网IP；SRV _phira._tcp.r0semi.net → re0.r0semi.net:$PORT"

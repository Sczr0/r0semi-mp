#!/usr/bin/env bash
# r0semi-mp 一键部署（Debian/Ubuntu，root 直接执行）
# 用法：root 下整段粘贴，或保存为 deploy.sh 后 `bash deploy.sh`
set -euo pipefail

echo "===== r0semi-mp 部署 ====="

# 1. 建目录 + 下载 nightly 二进制（GitHub 公开 Release，匿名可下）
mkdir -p /opt/r0semi-mp
cd /opt/r0semi-mp
if [ ! -x ./r0semi-mp-server ]; then
  echo "[1/4] 下载二进制..."
  curl -fL -o r0semi-mp-server \
    https://github.com/Sczr0/r0semi-mp/releases/download/nightly/r0semi-mp-server
  chmod +x r0semi-mp-server
fi
echo "[1/4] 二进制就绪: $(ls -la r0semi-mp-server | awk '{print $5}') bytes"

# 2. 可选配置（默认值即可；需要自定义就取消注释改）
# cat > server_config.yml <<'YML'
# listen: "0.0.0.0:12346"
# monitors: [2]
# YML

# 3. systemd 服务（Type=notify：bind 成功即报就绪；SIGTERM 优雅停机 §11）
echo "[2/4] 安装 systemd 服务..."
cat > /etc/systemd/system/r0semi-mp.service <<'UNIT'
[Unit]
Description=r0semi-mp server (phira multi-room)
After=network-online.target

[Service]
Type=notify
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

echo "[3/4] 启动服务..."
systemctl enable --now r0semi-mp
sleep 2

echo "[4/4] 状态确认:"
systemctl status r0semi-mp --no-pager | head -10 || true
echo "--- 监听端口 ---"
ss -tlnp | grep 12346 || echo "!! 12346 未监听（服务可能没起来，看上方状态）"

echo ""
echo "===== 部署完成 ====="
echo "下一步：NAT 面板加端口映射：公网端口(如47301) → 内网 12346"
echo "然后配 DNS：A re0.r0semi.net → 本机公网IP；SRV _phira._tcp.r0semi.net → re0.r0semi.net:<映射端口>"

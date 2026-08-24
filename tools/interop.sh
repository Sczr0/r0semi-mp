#!/usr/bin/env bash
# 互通测试（§9 双实现互连互通）：原版 phira-mp-client ↔ 本服务器
# 前置：r0semi-mp 服务器已构建（target/debug/r0semi-mp-server.exe）
# 用法：bash tools/interop.sh
set -e
cd "$(dirname "$0")/.."

PORT=${INTEROP_PORT:-12346}
MOCK_PORT=${INTEROP_MOCK_PORT:-19000}

echo "[interop] 1/3 启动 mock API (:$MOCK_PORT)"
python /tmp/mock_api.py > /tmp/mock.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "[interop] 2/3 启动本服务器 (:$PORT, 回源 mock)"
R0SEMI_MP_PORT=$PORT R0SEMI_MP_API_BASE=http://127.0.0.1:$MOCK_PORT \
  ./target/debug/r0semi-mp-server.exe > /tmp/interop_server.log 2>&1 &
SERVER_PID=$!
sleep 2

cleanup() {
  kill $SERVER_PID $MOCK_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "[interop] 3/3 跑原版客户端库流程"
cd ../r0semi-mp-interop
cargo run --quiet

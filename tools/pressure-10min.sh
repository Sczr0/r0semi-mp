#!/usr/bin/env bash
# 持续压测脚本（默认 10 分钟）：起 server（若无）→ 内存采样 → flooder 接力攻击 → 汇总
#
# 用法：
#   bash tools/pressure-10min.sh                 # 10 分钟（3 模式接力）
#   bash tools/pressure-10min.sh --duration 600  # 自定义秒数
#   bash tools/pressure-10min.sh --mode random   # 单模式
#
# 输出：
#   /tmp/pressure-10min/ 下：flooder 各模式结果 + server 内存采样 + 汇总报告

set -euo pipefail

DURATION=600            # 默认 10 分钟
MODE="multi"            # multi = 三模式接力；或 single 模式名
TARGET="127.0.0.1:12346"
OUT=/tmp/pressure-10min
SERVER_LOG=/tmp/server-press.log

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration) DURATION="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --target) TARGET="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 2 ;;
    esac
done

mkdir -p "$OUT"
echo "== 持续压测开始（${DURATION}s）=="
date

# 1. 确保 server 在跑（若没监听则启动）
if ! netstat -ano 2>/dev/null | grep -q ":$TARGET" ; then
    echo "[server] 未运行，启动 r0semi-mp-server ..."
    cd "$(dirname "$0")/.."
    cargo build -p phira-server --bin r0semi-mp-server > /dev/null 2>&1
    ./target/debug/r0semi-mp-server.exe > "$SERVER_LOG" 2>&1 &
    sleep 3
fi
echo "[server] $(netstat -ano 2>/dev/null | grep -E "12346" | grep LISTEN | head -1)"

# 2. 后台内存采样（每 10s 记录 WorkingSet，Windows）
SERVER_PID=$(netstat -ano 2>/dev/null | grep 12346 | grep LISTEN | awk '{print $NF}' | head -1)
(
    END=$(( $(date +%s) + DURATION + 30 ))
    while [ "$(date +%s)" -lt "$END" ]; do
        if [ -n "$SERVER_PID" ]; then
            WS=$(powershell -Command "[math]::Round((Get-Process -Id $SERVER_PID).WorkingSet64/1MB,1)" 2>/dev/null | tr -d '\r')
            echo "$(date +%s) $WS MB" >> "$OUT/mem.log"
        fi
        sleep 10
    done
) &
MEM_MONITOR=$!

# 3. flooder 攻击（multi = random + proto + reconnect 接力；single = 指定模式）
run_mode() {
    local mode="$1" secs="$2"
    echo ""
    echo "========== [$mode] ${secs}s =========="
    cargo run -p phira-server --bin flooder -- \
        --mode "$mode" --duration "$secs" --connections 50 --target "$TARGET" \
        2>/dev/null | grep -v Compiling | tee "$OUT/$mode.txt"
}

if [ "$MODE" = "multi" ]; then
    # 按比例分配：random 60% / proto 25% / reconnect 15%
    R=$((DURATION * 60 / 100)); P=$((DURATION * 25 / 100)); C=$((DURATION - R - P))
    run_mode random "$R"
    run_mode proto "$P"
    run_mode reconnect "$C"
else
    run_mode "$MODE" "$DURATION"
fi

# 4. 汇总
wait "$MEM_MONITOR" 2>/dev/null || true
echo ""
echo "=========================================="
echo "== 压测汇总（$DURATION s）=="
echo "=========================================="
echo "--- flooder 各模式 ---"
for f in "$OUT"/random.txt "$OUT"/proto.txt "$OUT"/reconnect.txt; do
    [ -f "$f" ] && grep -E "总发送|连接尝试" "$f" | sed "s/^/$(basename $f .txt): /"
done
echo ""
echo "--- server 存活 ---"
PANICS=$(grep -c "panic" "$SERVER_LOG" 2>/dev/null || echo 0)
CONNS=$(grep -c "connection from" "$SERVER_LOG" 2>/dev/null || echo 0)
echo "panic: $PANICS"
echo "累计连接: $CONNS"
echo ""
echo "--- server 内存（WorkingSet，10s 采样）---"
if [ -s "$OUT/mem.log" ]; then
    awk '{if($2>max){max=$2}} END {print "峰值: " max " MB"}' "$OUT/mem.log"
    awk 'NR==1{min=$2} {if($2<min){min=$2}} END {print "最低: " min " MB"}' "$OUT/mem.log"
    echo "样本数: $(wc -l < "$OUT/mem.log")"
    tail -3 "$OUT/mem.log"
else
    echo "（无采样——Windows 环境变量问题？）"
fi
echo ""
date
echo "== 压测结束 =="

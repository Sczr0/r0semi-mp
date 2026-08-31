#!/usr/bin/env python3
"""客户端漂移哨兵（client-conformance.md 五步规划步骤 4）。

上游 `phira-mp`（TeamFlos/phira-mp）是**动态仓库**——conformance 测试钉住的是
`crates/phira-server/Cargo.toml` 的 `rev`（当前 cc822df）。上游 master 前进后：
- 新行为可能让 conformance 测试失真（验证的是旧 commit，玩家跑的是新客户端）；
- 怪癖文档（client-behavior-review.md / client-conformance.md）可能过时。

本脚本：读 Cargo.lock 的钉住 commit → `git ls-remote` 上游 main 分支 → 漂移则警告：
"重跑全部兼容性测试（cargo test --test conformance）+ 更新怪癖文档"。

退出码：默认 0（咨询性，离线/网络失败不炸）；`--strict` 时漂移返回 1（供 CI 用）。
"""

import argparse
import pathlib
import re
import subprocess
import sys

UPSTREAM = "https://github.com/TeamFlos/phira-mp.git"
CARGO_LOCK = pathlib.Path(__file__).resolve().parent.parent / "Cargo.lock"
PKG_NAME = "phira-mp-client"


def pinned_sha() -> str | None:
    """从 Cargo.lock 提取 phira-mp-client 的钉住完整 commit sha。"""
    in_pkg = False
    for line in CARGO_LOCK.read_text(encoding="utf-8").splitlines():
        if line.startswith(f'name = "{PKG_NAME}"'):
            in_pkg = True
        elif in_pkg and line.startswith("name = "):
            return None  # 包条目结束仍未找到 source（异常）
        elif in_pkg and line.startswith("source = "):
            m = re.search(r"#([0-9a-f]{40})\"$", line.strip())
            return m.group(1) if m else None
    return None


def upstream_head() -> str | None:
    """上游 main 分支的当前 HEAD（git ls-remote；网络失败返回 None）。

    上游默认分支为 `main`（2026-08 实测 ls-remote --heads：main/ci/update-rust）。
    """
    try:
        out = subprocess.run(
            ["git", "ls-remote", UPSTREAM, "refs/heads/main"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout
    except (subprocess.SubprocessError, FileNotFoundError):
        return None
    sha = out.split()[0] if out.strip() else None
    return sha


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict", action="store_true", help="漂移时返回非零（CI 用）；默认咨询性返回 0"
    )
    args = parser.parse_args()

    pinned = pinned_sha()
    if not pinned:
        print(f"[FAIL] Cargo.lock 找不到 {PKG_NAME} 的 git 钉住 commit")
        return 1

    head = upstream_head()
    if not head:
        print(f"[SKIP] 无法访问 {UPSTREAM}（离线/网络失败）——跳过漂移检查")
        return 0

    if head == pinned:
        print(f"[OK] 上游 {UPSTREAM} main 分支仍为钉住 commit {pinned[:12]}——无漂移")
        return 0

    print(f"[DRIFT] 上游 main 分支已前进：钉住 {pinned[:12]} → 当前 {head[:12]}")
    print("  动作：")
    print("   1) 升级 crates/phira-server/Cargo.toml rev 至新 commit")
    print("   2) 重跑全部兼容性测试：cargo test --workspace --test conformance")
    print("   3) 按新客户端行为审计 client-behavior-review.md / client-conformance.md 怪癖清单")
    return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())

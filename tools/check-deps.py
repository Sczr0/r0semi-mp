#!/usr/bin/env python3
"""依赖方向检查脚本（§5.2 / §5.4 第三道闸门）。

读取 `cargo metadata` 的真实依赖图，与白名单比对：
- normal 边必须命中 ALLOW（§4.3 依赖方向矩阵）
- dev 边仅允许 impl-* → phira-contract 这条链（接入契约测试），
  或该边本身在 ALLOW 中（冗余无害，防 CI 误红）
- build 边一律禁止
- 新增内部 crate 但未登记 → 直接失败（§5.2：先更新本表再合并）

违规 → 退出码 1。
"""

import json
import subprocess
import sys

# 内部 crate 白名单：normal 依赖边（§4.3 / §5.2）
ALLOW = {
    "phira-api": [],                     # 不依赖任何内部 crate
    "phira-core": ["phira-api"],
    "phira-contract": ["phira-api"],     # 契约测试套件库（只依赖 api）
    "impl-rooms-v1": ["phira-api"],      # + dev: [phira-contract]（接入测试）
    # "impl-mod-memory": ["phira-api"],  # 阶段 4 再开（crate 尚未创建，创建时启用本行）
    "phira-server": ["phira-api", "phira-core", "impl-rooms-v1"],
}

# dev 边额外白名单：仅允许 impl-* → phira-contract（接入契约测试）
DEV_EXTRA = {
    "impl-rooms-v1": ["phira-contract"],
}


def main() -> int:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1"],
            text=True,
        )
    )
    member_ids = set(metadata["workspace_members"])
    members = {p["name"] for p in metadata["packages"] if p["id"] in member_ids}

    # 新 crate 出现但未登记 → 直接失败
    unregistered = members - set(ALLOW)
    if unregistered:
        print(f"[FAIL] 未登记的内部 crate: {sorted(unregistered)}")
        print("       新增 crate 时，先更新 tools/check-deps.py 的 ALLOW 再合并")
        return 1

    errors = []
    for pkg in metadata["packages"]:
        if pkg["id"] not in member_ids:
            continue
        for dep in pkg["dependencies"]:
            if dep["name"] not in members:
                continue  # 只看内部 crate 之间的边
            kind = dep.get("kind")  # None=normal, "dev", "build"
            allowed = dep["name"] in ALLOW.get(pkg["name"], [])
            dev_ok = kind == "dev" and dep["name"] in DEV_EXTRA.get(pkg["name"], [])
            if kind == "build":
                errors.append(f"build 边不允许: {pkg['name']} -> {dep['name']}")
            elif not (allowed or dev_ok):
                errors.append(f"违规依赖边 ({kind or 'normal'}): {pkg['name']} -> {dep['name']}")

    if errors:
        print("[FAIL] 依赖方向违反白名单：")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"[OK] 依赖方向全部符合白名单（{len(members)} 个 crate）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

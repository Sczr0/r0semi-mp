#!/usr/bin/env python3
"""ADR 完整性检查（ISSUE-0002 修复：增量纪律）。

检查 docs/adr/ 下编号 0001..N **连续存在且非空**：
- 新增决策必须落 ADR（编号取当前最大值 + 1）
- 编号不允许跳号（跳号 = 决策丢失）
- 文件名格式 `0001-*.md`

配合 ARCHITECTURE.md §5.4/§5.6：破坏性契约变更必须走 ADR + api 主版本。
"""

import pathlib
import re
import sys

ADR_DIR = pathlib.Path(__file__).resolve().parent.parent / "docs" / "adr"


def main() -> int:
    if not ADR_DIR.is_dir():
        print(f"[FAIL] docs/adr/ 目录不存在（应存放 ADR 决策记录）")
        return 1

    files = sorted(ADR_DIR.glob("*.md"))
    if not files:
        print("[FAIL] docs/adr/ 为空——决策必须落 ADR（从 0001 开始编号）")
        return 1

    nums = []
    for f in files:
        m = re.match(r"^(\d{4})-", f.name)
        if not m:
            print(f"[FAIL] 文件名不符编号格式: {f.name}（应为 0001-*.md）")
            return 1
        nums.append(int(m.group(1)))
        if f.stat().st_size == 0:
            print(f"[FAIL] 空文件: {f.name}")
            return 1

    expected = list(range(1, len(nums) + 1))
    if sorted(nums) != expected:
        print(f"[FAIL] ADR 编号不连续: {sorted(nums)}，期望 {expected}")
        print(f"       新增决策取编号 {max(expected) + 1}（或先补齐跳号）")
        return 1

    print(f"[OK] ADR 完整（{len(nums)} 条，编号连续）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

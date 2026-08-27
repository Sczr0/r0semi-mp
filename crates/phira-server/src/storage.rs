//! 管理面持久化（ban 名单 / 审计归档 / config 快照）——组合根边角事务。
//!
//! 纪律（docs/admin-api.md §持久化）：
//! - **只持久化管理事实**（决策与记录），不持久化状态（房间/会话内存态模型不变）；
//! - 组合根独占：phira-api/core/impl/契约零感知，无新 `RoomCommand`、无新 `Moderator`；
//! - 零新依赖：纯 `std::fs` + `serde_json`（http.rs 已有）；写文件**同步在既有命令
//!   路径**（管理操作低频 <1ms），无后台任务/定时器（§4.6 时间事实命令化不违背）；
//! - fail soft：写失败记日志、内存态继续；读损坏回退空态/默认（名单只是反作弊工具）。
//!
//! 文件布局（`persist_dir` 配置项，默认 `./data`）：
//! ```text
//! data/
//! ├── audit.jsonl          # 审计归档（追加；一个 AuditEntry 一行）
//! ├── bans.json            # 封禁名单（全量重写；原子 tmp+rename）
//! ├── config.current.json  # 生效配置原文（POST /admin/config 请求体）
//! └── config.last.json     # 上一份（rollback 源；回切一次后删除）
//! ```

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use phira_api::RoomConfig;
use tracing::{error, warn};

/// 审计归档回填上限（与内存环容量一致：启动时文件尾部至多回填 256 行）。
pub const AUDIT_BACKFILL_MAX: usize = 256;

/// 确保持久化目录存在（失败仅告警——fail soft，管理面不因磁盘问题崩）。
pub fn ensure_dir(dir: &Path) {
    if let Err(e) = fs::create_dir_all(dir) {
        error!(
            "persist dir {dir:?} unavailable: {e}（管理持久化禁用，影响：审计不归档/名单不落盘/config 不回滚跨重启）"
        );
    }
}

/// 原子写（tmp + rename）：半写文件永不落地。
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

// ---------- audit.jsonl ----------

/// 追加一行到 JSONL（返回失败供调用方告警；行尾含 `\n`）。
///
/// # Errors
///
/// 打开/写入/`fsync` 失败时返回 `std::io::Error`。
pub fn audit_append(path: &Path, line: &str) -> std::io::Result<()> {
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    f.sync_all()?; // 归档语义：断电容忍下仍尽量持久（管理事实不丢）
    Ok(())
}

/// 读文件**尾部至多 `max` 行**（启动回填用；损坏行跳过，不打断回填）。
pub fn audit_read_tail(path: &Path, max: usize) -> Vec<String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut lines = text.lines().collect::<Vec<_>>();
    if lines.len() > max {
        lines.drain(..lines.len() - max);
    }
    lines.into_iter().map(str::to_owned).collect()
}

// ---------- bans.json ----------

/// 写名单（全量；原子）。`[7, 42]`。
///
/// # Errors
///
/// 序列化 / `tmp+rename` 写盘失败时返回 `std::io::Error`。
pub fn bans_write(path: &Path, list: &[i32]) -> std::io::Result<()> {
    let text = serde_json::to_string(list)
        .map_err(|e| std::io::Error::other(format!("serialize bans: {e}")))?;
    atomic_write(path, text.as_bytes())
}

/// 读名单（损坏/缺失 → 空 + 告警；fail soft）。
pub fn bans_read(path: &Path) -> Vec<i32> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    match serde_json::from_str::<Vec<i32>>(&text) {
        Ok(list) => list,
        Err(e) => {
            warn!("bans file {path:?} corrupt ({e})——按空名单启动（fail soft）");
            Vec::new()
        }
    }
}

// ---------- config.current.json / config.last.json ----------

/// config 快照存取：current = 生效原文，last = 上一份（rollback 源）。
#[derive(Default)]
pub struct ConfigStore {
    current: Option<PathBuf>,
    last: Option<PathBuf>,
}

impl ConfigStore {
    /// 启用持久化（dir = `persist_dir`；构造即确保目录存在）。
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        ensure_dir(dir);
        Self {
            current: Some(dir.join("config.current.json")),
            last: Some(dir.join("config.last.json")),
        }
    }

    /// 禁用（测试默认可选项：所有方法 no-op Ok/None）。
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// POST /admin/config 成功：旧 current → last；新原文 → current。
    /// 失败仅记日志（调用方 keep 内存态），不中断主流程。
    pub fn record_success(&self, body: &serde_json::Value) {
        let (Some(current), Some(last)) = (&self.current, &self.last) else {
            return;
        };
        // 落盘 = `rooms` 子对象（`{"monitors": [...]}`，与 config_from_json 同形）；
        // 兼容扁平体（未来协议若改扁平）。
        let rooms = body.get("rooms").unwrap_or(body);
        let Ok(new_text) = serde_json::to_string(rooms) else {
            return;
        };
        // 旧 current → last（rollback 源）；缺失 = 首次写，无 last
        if let Ok(old) = fs::read_to_string(current)
            && let Err(e) = atomic_write(last, old.as_bytes())
        {
            warn!("persist config.last {last:?}: {e}");
        }
        if let Err(e) = atomic_write(current, new_text.as_bytes()) {
            warn!("persist config.current {current:?}: {e}");
        }
    }

    /// rollback 成功：last → current；删除 last（对应内存 take_last 取走即清空）。
    pub fn record_rollback(&self) {
        let (Some(current), Some(last)) = (&self.current, &self.last) else {
            return;
        };
        match fs::read_to_string(last) {
            Ok(prev) => {
                if let Err(e) = atomic_write(current, prev.as_bytes()) {
                    warn!("persist config.current {current:?} on rollback: {e}");
                }
                if let Err(e) = fs::remove_file(last) {
                    warn!("persist config.last remove: {e}");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // 内存有 last（take_last Some）但文件缺失 = 文件层落后（如加固前写入）；
                // 告警并跳过——内存层是可信源，滚动不影响已生效配置
                warn!("rollback: config.last {last:?} missing（文件层落后于内存层，跳过落盘）");
            }
            Err(e) => warn!("rollback: read config.last {last:?}: {e}"),
        }
    }

    /// 启动加载：(生效配置原文, 上一份配置原文)，均缺省 None。
    #[must_use]
    pub fn load(&self) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
        let read = |path: &Option<PathBuf>| -> Option<serde_json::Value> {
            let path = path.as_ref()?;
            let text = fs::read_to_string(path).ok()?;
            match serde_json::from_str(&text) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("persist config {path:?} corrupt ({e})——按缺省处理");
                    None
                }
            }
        };
        (read(&self.current), read(&self.last))
    }
}

/// 配置原文 → `RoomConfig`（`monitors` 白名单；其余字段未来演进由原文自然携带）。
///
/// # Errors
///
/// 永不失败（monitors 缺失/非法字段降级为空白名单）——返回类型保留给未来字段校验。
pub fn config_from_json(value: &serde_json::Value) -> Result<RoomConfig, &'static str> {
    let monitors = value
        .get("monitors")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_i64().and_then(|i| i32::try_from(i).ok()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(RoomConfig { monitors })
}

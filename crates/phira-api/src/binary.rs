//! 二进制编解码（协议 §6.2）。
//!
//! 原版 `phira-mp-common` 的 `bin.rs` 移植（Apache-2.0，TeamFlos），按本架构轻量化：
//! - 去 `anyhow`/`byteorder`/`tap` → 手写小端读取 + `thiserror` 错误
//! - 去 `uuid`/`chrono`（协议命令未使用，§4.3 依赖红线）
//! - 增强：ULEB128 溢出 / 数组长度 / take 越界防护（合法输入行为与原版完全一致，
//!   非法输入从 UB/OOM 变为明确报错）
//!
//! 红线程（§4.3-1 / §4.8）：零 tokio、零运行时，只依赖 std + thiserror + half。

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use thiserror::Error;

/// 协议解码错误。
///
/// 覆盖 §6.2 编解码的失败路径；帧层（phira-server）把长度/大小类错误拦截在包外，
/// 本类型只处理包内解码失败。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    /// 意外 EOF（数据截断，包被截短）。
    #[error("unexpected EOF")]
    Eof,
    /// 非法枚举 tag（未知命令编号）。
    #[error("invalid enum tag: {0}")]
    InvalidTag(u8),
    /// 字符串超长（长度字段超出类型约束，§6.2 Varchar）。
    #[error("string too long: {len} > {max}")]
    StringTooLong {
        /// 类型允许的最大字节数。
        max: usize,
        /// 实际长度字段值（字节数）。
        len: usize,
    },
    /// 非法房间 id（字符约束 `[A-Za-z0-9_-]` 失败，§6.2）。
    #[error("invalid room id: {0}")]
    InvalidRoomId(String),
    /// ULEB128 超过 64 位（防移位溢出；合法协议值远小于此）。
    #[error("ULEB128 overflow")]
    UlebOverflow,
    /// 数组长度超过剩余字节数（防分配攻击：恶意长度字段导致的巨量预分配）。
    #[error("array length {len} exceeds remaining {remaining} bytes")]
    ArrayTooLarge {
        /// 长度字段值（元素数量）。
        len: u64,
        /// 当前游标后剩余的字节数。
        remaining: u64,
    },
}

/// 协议编解码结果。
pub type ProtoResult<T> = std::result::Result<T, DecodeError>;

/// 可二进制序列化的类型（协议 §6.2）。
///
/// 原版 `phira-mp-common` 同名 trait；整数小端、容器长度 ULEB128、`Option` = bool + 值、
/// `Result` = bool + Ok/Err。实现者必须保证读写对称（契约测试 roundtrip 断言）。
pub trait BinaryData: Sized {
    /// 从 reader 读取一个值。
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self>;
    /// 把 `self` 写入 writer。
    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()>;
}

/// 只读字节游标（原版 `BinaryReader`）。
pub struct BinaryReader<'a>(&'a [u8], usize);

impl<'a> BinaryReader<'a> {
    /// 从切片起始位置创建游标。
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self(data, 0)
    }

    /// 读取长度前缀数组（§6.2：数量 ULEB128 + 逐元素）。
    ///
    /// # Errors
    ///
    /// 长度字段超过剩余字节数 → [`DecodeError::ArrayTooLarge`]（防分配攻击）；
    /// 元素读取失败 → 对应 [`DecodeError`]。
    pub fn array<T: BinaryData>(&mut self) -> ProtoResult<Vec<T>> {
        let len = self.uleb()?;
        // 元素至少 1 字节（枚举 tag 或单字节标量），长度超剩余字节必失败——提前拒绝防巨量预分配
        let remaining = (self.0.len() - self.1) as u64;
        if len > remaining {
            return Err(DecodeError::ArrayTooLarge { len, remaining });
        }
        (0..len).map(|_| self.read()).collect()
    }

    /// 读取单字节。
    ///
    /// # Errors
    ///
    /// 游标越界 → [`DecodeError::Eof`]。
    pub fn byte(&mut self) -> ProtoResult<u8> {
        let b = self.0.get(self.1).ok_or(DecodeError::Eof)?;
        self.1 += 1;
        Ok(*b)
    }

    /// 取 `n` 字节子切片（不复制）。
    ///
    /// # Errors
    ///
    /// 越界 → [`DecodeError::Eof`]（含 `n` 溢出 usize 的防护）。
    pub fn take(&mut self, n: usize) -> ProtoResult<&'a [u8]> {
        let end = self.1.checked_add(n).ok_or(DecodeError::Eof)?;
        let slice = self.0.get(self.1..end).ok_or(DecodeError::Eof)?;
        self.1 = end;
        Ok(slice)
    }

    /// 读取一个 `BinaryData` 值（类型推断用）。
    ///
    /// # Errors
    ///
    /// 该类型的读取失败路径。
    pub fn read<T: BinaryData>(&mut self) -> ProtoResult<T> {
        T::read_binary(self)
    }

    /// 读取 ULEB128 变长整数（§6.2）。
    ///
    /// # Errors
    ///
    /// 超过 64 位 → [`DecodeError::UlebOverflow`]（防移位溢出）。
    pub fn uleb(&mut self) -> ProtoResult<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.read::<u8>()?;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(DecodeError::UlebOverflow);
            }
        }
    }
}

/// 追加写游标（原版 `BinaryWriter`，直接写 `Vec<u8>`）。
pub struct BinaryWriter<'a>(&'a mut Vec<u8>);

impl<'a> BinaryWriter<'a> {
    /// 从可变缓冲创建游标。
    #[must_use]
    pub fn new(vec: &'a mut Vec<u8>) -> Self {
        Self(vec)
    }

    /// 写入长度前缀数组（§6.2）。
    ///
    /// # Errors
    ///
    /// 元素写入失败路径（内存写入一般不失败）。
    pub fn array<T: BinaryData>(&mut self, v: &[T]) -> ProtoResult<()> {
        self.uleb(v.len() as u64)?;
        for element in v {
            element.write_binary(self)?;
        }
        Ok(())
    }

    /// 按引用写入一个 `BinaryData` 值。
    ///
    /// # Errors
    ///
    /// 该类型的写入失败路径。
    #[inline]
    pub fn write<T: BinaryData>(&mut self, v: &T) -> ProtoResult<()> {
        v.write_binary(self)
    }

    /// 按值写入一个 `BinaryData` 值。
    ///
    /// # Errors
    ///
    /// 该类型的写入失败路径。
    #[inline]
    pub fn write_val<T: BinaryData>(&mut self, v: T) -> ProtoResult<()> {
        v.write_binary(self)
    }

    /// 写入原始字节（不经 BinaryData 编码；ISSUE-0003 方案 2：`Outbound::Encoded`
    /// 共享编码载荷直写——一次编码、多接收者复用）。
    ///
    /// # Errors
    ///
    /// 内存写入失败（理论上不发生）。
    pub fn write_raw(&mut self, bytes: &[u8]) -> ProtoResult<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }

    /// 写入 ULEB128 变长整数（§6.2）。
    ///
    /// # Errors
    ///
    /// 内存写入失败路径（理论上不发生）。
    pub fn uleb(&mut self, mut v: u64) -> ProtoResult<()> {
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            self.write_val(byte)?;
            if v == 0 {
                return Ok(());
            }
        }
    }
}

/// 编码一个完整包载荷（不含长度前缀；帧层负责加 ULEB128 长度头，§6.1）。
///
/// # Panics
///
/// 内存写入失败（`Vec` 扩容 OOM）时 panic——写路径理论上不可失败。
pub fn encode_packet(payload: &impl BinaryData, vec: &mut Vec<u8>) {
    BinaryWriter::new(vec)
        .write(payload)
        .expect("writing to in-memory buffer cannot fail");
}

/// 解码一个完整包载荷（§6.1 帧层剥掉长度头后调用）。
///
/// # Errors
///
/// 载荷格式非法 → [`DecodeError`]。
pub fn decode_packet<T>(data: &[u8]) -> ProtoResult<T>
where
    T: BinaryData,
{
    BinaryReader::new(data).read()
}

// —— 基础类型实现（§6.2：整数小端、bool=1 字节） ——

impl BinaryData for () {
    fn read_binary(_r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(())
    }

    fn write_binary(&self, _w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        Ok(())
    }
}

impl BinaryData for i8 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(r.byte()? as i8)
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        // to_le_bytes 位保留转换，避免 `as u8` 的符号转换 lint
        w.0.push(self.to_le_bytes()[0]);
        Ok(())
    }
}

impl BinaryData for u8 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        r.byte()
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.push(*self);
        Ok(())
    }
}

impl BinaryData for u16 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(u16::from_le_bytes(r.take(2)?.try_into().expect("take(2)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for u32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(u32::from_le_bytes(r.take(4)?.try_into().expect("take(4)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for u64 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(u64::from_le_bytes(r.take(8)?.try_into().expect("take(8)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for i32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(i32::from_le_bytes(r.take(4)?.try_into().expect("take(4)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for i64 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(i64::from_le_bytes(r.take(8)?.try_into().expect("take(8)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for bool {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(r.byte()? == 1)
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write_val(*self as u8)
    }
}

impl BinaryData for f32 {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(f32::from_le_bytes(r.take(4)?.try_into().expect("take(4)")))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.0.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl BinaryData for String {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        let len = r.uleb()? as usize;
        Ok(String::from_utf8_lossy(r.take(len)?).into_owned())
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.uleb(self.len() as u64)?;
        w.0.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl<A: BinaryData, B: BinaryData> BinaryData for (A, B) {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok((r.read()?, r.read()?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.write(&self.0)?;
        w.write(&self.1)?;
        Ok(())
    }
}

impl<T: BinaryData> BinaryData for Option<T> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(if r.read::<bool>()? {
            Some(r.read()?)
        } else {
            None
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Some(val) => {
                w.write_val(true)?;
                w.write(val)?;
            }
            None => {
                w.write_val(false)?;
            }
        }
        Ok(())
    }
}

impl<A: BinaryData, B: BinaryData> BinaryData for Result<A, B> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(if r.read::<bool>()? {
            Ok(r.read()?)
        } else {
            Err(r.read()?)
        })
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        match self {
            Ok(val) => {
                w.write_val(true)?;
                w.write(val)?;
            }
            Err(err) => {
                w.write_val(false)?;
                w.write(err)?;
            }
        }
        Ok(())
    }
}

impl<T: BinaryData> BinaryData for Vec<T> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        r.array()
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.array(self)
    }
}

/// 哈希映射（协议 §6.3：`ClientRoomState.users`）。
impl<K: BinaryData + Eq + Hash, V: BinaryData> BinaryData for HashMap<K, V> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        let len = r.uleb()?;
        let remaining = (r.0.len() - r.1) as u64;
        if len > remaining {
            return Err(DecodeError::ArrayTooLarge { len, remaining });
        }
        (0..len).map(|_| r.read::<(K, V)>()).collect()
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        w.uleb(self.len() as u64)?;
        for (k, v) in self {
            k.write_binary(w)?;
            v.write_binary(w)?;
        }
        Ok(())
    }
}

/// `Arc` 透明包装：读写内部值（协议 §6.3：`Touches.frames` 等热路径共享，§6.5-17）。
impl<T: BinaryData> BinaryData for Arc<T> {
    fn read_binary(r: &mut BinaryReader<'_>) -> ProtoResult<Self> {
        Ok(Arc::new(r.read()?))
    }

    fn write_binary(&self, w: &mut BinaryWriter<'_>) -> ProtoResult<()> {
        self.as_ref().write_binary(w)
    }
}

use std::io::{self, Read, Write};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_WORDS: usize = PAGE_SIZE / 8;
pub const SNAPSHOT_BITS: u32 = 19;
pub const SNAPSHOT_MASK: u64 = (1u64 << SNAPSHOT_BITS) - 1;
pub const MAX_SNAPSHOT_ID: u32 = (1u32 << SNAPSHOT_BITS) - 1;
pub const DEFAULT_DELTA_THRESHOLD: u16 = 256;

pub const MAGIC_LOG: [u8; 8] = *b"BXDBLOG\0";
pub const MAGIC_IDX: [u8; 8] = *b"BXDBIDX\0";
pub const FORMAT_VERSION: u8 = 0x01;
pub const HEADER_SIZE: usize = 16;
pub const LOG_RECORD_BASE_SIZE: usize = 22;

pub const CHUNK_FULL: u8 = 0;
pub const CHUNK_DELTA: u8 = 1;
pub const CHUNK_ZERO: u8 = 2;

#[inline]
pub fn encode_key(pa: u64, snapshot_id: u32) -> u64 {
    (pa << SNAPSHOT_BITS) | (snapshot_id as u64 & SNAPSHOT_MASK)
}

#[inline]
pub fn pa_of(key: u64) -> u64 {
    key >> SNAPSHOT_BITS
}

#[inline]
pub fn snapshot_of(key: u64) -> u32 {
    (key & SNAPSHOT_MASK) as u32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkKind {
    Full,
    Delta,
    Zero,
}

impl ChunkKind {
    pub fn as_u8(self) -> u8 {
        match self {
            ChunkKind::Full => CHUNK_FULL,
            ChunkKind::Delta => CHUNK_DELTA,
            ChunkKind::Zero => CHUNK_ZERO,
        }
    }

    pub fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            CHUNK_FULL => Ok(ChunkKind::Full),
            CHUNK_DELTA => Ok(ChunkKind::Delta),
            CHUNK_ZERO => Ok(ChunkKind::Zero),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid chunk type {other}"),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkRecord {
    pub key: u64,
    pub kind: ChunkKind,
    pub worker_id: u8,
    pub offset: u64,
    pub len: u32,
    pub base_key: u64,
}

impl ChunkRecord {
    pub fn new_full(key: u64, worker_id: u8, offset: u64, len: u32) -> Self {
        Self { key, kind: ChunkKind::Full, worker_id, offset, len, base_key: 0 }
    }

    pub fn new_delta(
        key: u64,
        worker_id: u8,
        offset: u64,
        len: u32,
        base_key: u64,
    ) -> Self {
        Self { key, kind: ChunkKind::Delta, worker_id, offset, len, base_key }
    }

    pub fn new_zero(key: u64) -> Self {
        Self { key, kind: ChunkKind::Zero, worker_id: 0, offset: 0, len: 0, base_key: 0 }
    }

    pub fn encoded_len(&self) -> usize {
        if self.kind == ChunkKind::Delta {
            LOG_RECORD_BASE_SIZE + 8
        } else {
            LOG_RECORD_BASE_SIZE
        }
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut buf = [0u8; 30];
        buf[0..8].copy_from_slice(&self.key.to_le_bytes());
        buf[8] = self.kind.as_u8();
        buf[9] = self.worker_id;
        buf[10..18].copy_from_slice(&self.offset.to_le_bytes());
        buf[18..22].copy_from_slice(&self.len.to_le_bytes());
        if self.kind == ChunkKind::Delta {
            buf[22..30].copy_from_slice(&self.base_key.to_le_bytes());
            w.write_all(&buf[..30])
        } else {
            w.write_all(&buf[..22])
        }
    }

    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Option<Self>> {
        let mut head = [0u8; LOG_RECORD_BASE_SIZE];
        match r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let key = u64::from_le_bytes(head[0..8].try_into().unwrap());
        let kind = ChunkKind::from_u8(head[8])?;
        let worker_id = head[9];
        let offset = u64::from_le_bytes(head[10..18].try_into().unwrap());
        let len = u32::from_le_bytes(head[18..22].try_into().unwrap());
        let base_key = if kind == ChunkKind::Delta {
            let mut extra = [0u8; 8];
            r.read_exact(&mut extra)?;
            u64::from_le_bytes(extra)
        } else {
            0
        };
        Ok(Some(Self { key, kind, worker_id, offset, len, base_key }))
    }

    pub fn write_fixed<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut buf = [0u8; FIXED_RECORD_SIZE];
        self.encode_fixed(&mut buf);
        w.write_all(&buf)
    }

    pub fn encode_fixed(&self, buf: &mut [u8; FIXED_RECORD_SIZE]) {
        buf.fill(0);
        buf[0..8].copy_from_slice(&self.key.to_le_bytes());
        buf[8..16].copy_from_slice(&self.base_key.to_le_bytes());
        buf[16..24].copy_from_slice(&self.offset.to_le_bytes());
        buf[24..28].copy_from_slice(&self.len.to_le_bytes());
        buf[28] = self.kind.as_u8();
        buf[29] = self.worker_id;
    }

    pub fn decode_fixed(buf: &[u8; FIXED_RECORD_SIZE]) -> io::Result<Self> {
        let key = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let base_key = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let offset = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let len = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let kind = ChunkKind::from_u8(buf[28])?;
        let worker_id = buf[29];
        Ok(Self { key, kind, worker_id, offset, len, base_key })
    }
}

pub const FIXED_RECORD_SIZE: usize = 32;

pub fn encode_delta_patch(patch: &[(u16, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(patch.len() * 10);
    for &(idx, val) in patch {
        out.extend_from_slice(&idx.to_le_bytes());
        out.extend_from_slice(&val.to_le_bytes());
    }
    out
}

pub fn apply_delta_patch(base: &[u8; PAGE_SIZE], blob: &[u8], out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    if blob.len() % 10 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "delta blob length not multiple of 10",
        ));
    }
    out.copy_from_slice(base);
    apply_delta_xor(blob, out)
}

/// Apply delta XOR words in-place. Assumes `out` already contains the base page.
pub fn apply_delta_patch_in_place(blob: &[u8], out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    if blob.len() % 10 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "delta blob length not multiple of 10",
        ));
    }
    apply_delta_xor(blob, out)
}

fn apply_delta_xor(blob: &[u8], out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    for chunk in blob.chunks_exact(10) {
        let idx = u16::from_le_bytes([chunk[0], chunk[1]]) as usize;
        let xor = u64::from_le_bytes(chunk[2..10].try_into().unwrap());
        if idx >= PAGE_WORDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "delta word index out of range",
            ));
        }
        let dst = &mut out[idx * 8..(idx + 1) * 8];
        let cur = u64::from_le_bytes(dst.try_into().unwrap());
        dst.copy_from_slice(&(cur ^ xor).to_le_bytes());
    }
    Ok(())
}

pub fn compute_xor_patch(page: &[u8; PAGE_SIZE], base: &[u8; PAGE_SIZE]) -> Vec<(u16, u64)> {
    let mut out = Vec::with_capacity(32);
    for i in 0..PAGE_WORDS {
        let pw = u64::from_le_bytes(page[i * 8..(i + 1) * 8].try_into().unwrap());
        let bw = u64::from_le_bytes(base[i * 8..(i + 1) * 8].try_into().unwrap());
        let x = pw ^ bw;
        if x != 0 {
            out.push((i as u16, x));
        }
    }
    out
}

pub fn is_all_zero(page: &[u8; PAGE_SIZE]) -> bool {
    page.iter().all(|&b| b == 0)
}

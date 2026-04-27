use rustc_hash::{FxHashMap, FxHashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, MAGIC_IDX, MAGIC_LOG, snapshot_of,
};
use crate::format::{read_and_verify_header, write_index_header, write_log_header};

/// Purge all records and their blobs whose snapshot_id is strictly greater
/// than `snapshot_threshold`.
///
/// Accepts both B-tree (`index.bxdb`) and append-only log (`chunks.log`)
/// formats as input, and preserves any existing format in the output.
/// If both formats are present, both are rewritten so they stay in sync.
///
/// Because snapshot ids grow monotonically, blob data within each worker file
/// is laid out in snapshot order. This function exploits that property by
/// truncating worker blob files at the highest offset still referenced by a
/// kept record, avoiding a full blob rewrite.
///
/// Returns the number of records removed.
pub fn purge(dir: &Path, snapshot_threshold: u32) -> io::Result<usize> {
    let idx_path = dir.join("index.bxdb");
    let log_path = dir.join("chunks.log");

    let has_idx = idx_path.exists();
    let has_log = log_path.exists();

    if !has_idx && !has_log {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither index.bxdb nor chunks.log found",
        ));
    }

    // Read from the btree index if available (faster), otherwise from the log.
    // Both formats represent the same record set, so we only need to read one.
    let mut records = if has_idx {
        read_index(&idx_path)?
    } else {
        read_log(&log_path)?
    };

    let before = records.len();
    records.retain(|r| snapshot_of(r.key) <= snapshot_threshold);
    let removed = before - records.len();

    if removed == 0 {
        return Ok(0);
    }

    let max_snap = records
        .iter()
        .map(|r| snapshot_of(r.key))
        .max()
        .unwrap_or(0);

    truncate_blobs(dir, &records)?;
    cleanup_unused_blob_files(dir, &records)?;

    if has_idx {
        write_index_file(dir, &records, max_snap)?;
    }
    if has_log {
        write_log_file(dir, &records, max_snap)?;
    }

    Ok(removed)
}

fn read_index(idx_path: &Path) -> io::Result<Vec<ChunkRecord>> {
    let file = File::open(idx_path)?;
    let meta = file.metadata()?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_IDX)?;

    let body_len = meta.len().saturating_sub(HEADER_SIZE as u64);
    if body_len % FIXED_RECORD_SIZE as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index.bxdb body size not aligned to record size",
        ));
    }
    let count = (body_len / FIXED_RECORD_SIZE as u64) as usize;
    let mut records = Vec::with_capacity(count);
    let mut buf = [0u8; FIXED_RECORD_SIZE];
    for _ in 0..count {
        r.read_exact(&mut buf)?;
        records.push(ChunkRecord::decode_fixed(&buf)?);
    }
    Ok(records)
}

fn read_log(log_path: &Path) -> io::Result<Vec<ChunkRecord>> {
    let file = File::open(log_path)?;
    let meta = file.metadata()?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;

    let capacity =
        (meta.len().saturating_sub(HEADER_SIZE as u64) / size_of::<u64>() as u64) as usize;
    let mut records: Vec<ChunkRecord> = Vec::with_capacity(capacity.min(1 << 24));
    while let Some(rec) = ChunkRecord::read_from(&mut r)? {
        records.push(rec);
    }

    records.sort_by_key(|r| r.key);
    records.dedup_by_key(|r| r.key);

    Ok(records)
}

/// Truncate each worker blob file so that only blobs referenced by kept
/// records remain.  Because save_pages calls append blobs in monotonically
/// increasing snapshot order, the kept blobs (snap <= threshold) are a
/// prefix of each worker file and the deleted blobs (snap > threshold) a
/// suffix.
fn truncate_blobs(dir: &Path, records: &[ChunkRecord]) -> io::Result<()> {
    let mut worker_max_end: FxHashMap<u8, u64> = FxHashMap::default();

    for rec in records {
        if rec.kind == ChunkKind::Zero {
            continue;
        }
        let end = rec.offset + rec.len as u64;
        worker_max_end
            .entry(rec.worker_id)
            .and_modify(|e| *e = (*e).max(end))
            .or_insert(end);
    }

    let blobs_dir = dir.join("blobs");
    for (worker_id, keep_len) in worker_max_end {
        let path = blobs_dir.join(format!("worker_{worker_id}.blob"));
        let f = OpenOptions::new().write(true).open(&path)?;
        f.set_len(keep_len)?;
    }

    Ok(())
}

fn write_log_file(dir: &Path, records: &[ChunkRecord], max_snap: u32) -> io::Result<()> {
    let log_path = dir.join("chunks.log");
    let tmp_path = log_path.with_extension("log.tmp");

    // Group records by snapshot_id so the output log preserves the
    // monotonic snapshot invariant (same order save_pages produces).
    // Records enter key-sorted from read_index / read_log, so each
    // group is already sorted by key — no per-group sort needed.
    let mut by_snap: FxHashMap<u32, Vec<ChunkRecord>> = FxHashMap::default();
    for rec in records {
        by_snap.entry(snapshot_of(rec.key)).or_default().push(*rec);
    }
    let mut snaps: Vec<u32> = by_snap.keys().copied().collect();
    snaps.sort_unstable();

    {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = BufWriter::new(f);
        write_log_header(&mut w, max_snap)?;
        for snap in &snaps {
            for rec in by_snap.get(snap).unwrap() {
                rec.write_to(&mut w)?;
            }
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }

    std::fs::rename(&tmp_path, &log_path)?;
    Ok(())
}

fn write_index_file(dir: &Path, records: &[ChunkRecord], max_snap: u32) -> io::Result<()> {
    let idx_path = dir.join("index.bxdb");
    let tmp_path = idx_path.with_extension("bxdb.tmp");

    {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = BufWriter::new(f);
        write_index_header(&mut w, max_snap)?;
        let mut buf = [0u8; FIXED_RECORD_SIZE];
        for rec in records {
            rec.encode_fixed(&mut buf);
            w.write_all(&buf)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }

    std::fs::rename(&tmp_path, &idx_path)?;
    Ok(())
}

fn cleanup_unused_blob_files(dir: &Path, records: &[ChunkRecord]) -> io::Result<()> {
    let used: FxHashSet<u8> = records.iter().map(|r| r.worker_id).collect();
    let blobs_dir = dir.join("blobs");
    if !blobs_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&blobs_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(rest) = name_str.strip_prefix("worker_") {
            if let Some(num_str) = rest.strip_suffix(".blob") {
                if let Ok(wid) = num_str.parse::<u8>() {
                    if !used.contains(&wid) {
                        fs::remove_file(entry.path())?;
                    }
                }
            }
        }
    }
    Ok(())
}

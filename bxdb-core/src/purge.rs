use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, MAGIC_IDX, MAGIC_LOG,
    snapshot_of,
};
use crate::format::{read_and_verify_header, write_index_header};

/// Purge all records and their blobs whose snapshot_id is strictly greater
/// than `snapshot_threshold`.
///
/// Accepts both B-tree (`index.bxdb`) and append-only log (`chunks.log`)
/// formats as input. Always writes `index.bxdb` as output (the canonical read
/// format). If only `chunks.log` exists, the log is converted to B-tree in the
/// process and the log is removed.
///
/// Returns the number of records removed.
pub fn purge(dir: &Path, snapshot_threshold: u32) -> io::Result<usize> {
    let idx_path = dir.join("index.bxdb");
    let log_path = dir.join("chunks.log");

    let mut records = if idx_path.exists() {
        read_index(&idx_path)?
    } else if log_path.exists() {
        read_log(&log_path)?
    } else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither index.bxdb nor chunks.log found",
        ));
    };

    let before = records.len();
    records.retain(|r| snapshot_of(r.key) <= snapshot_threshold);
    let removed = before - records.len();

    if removed == 0 {
        return Ok(0);
    }

    compact_blobs(dir, &mut records)?;
    write_index_file(dir, &records)?;
    cleanup_unused_blob_files(dir, &records)?;

    // Remove chunks.log now that index.bxdb is canonical.
    let log_path = dir.join("chunks.log");
    if log_path.exists() {
        fs::remove_file(&log_path)?;
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

    let capacity = (meta.len().saturating_sub(HEADER_SIZE as u64)
        / size_of::<u64>() as u64) as usize;
    let mut records: Vec<ChunkRecord> = Vec::with_capacity(capacity.min(1 << 24));
    while let Some(rec) = ChunkRecord::read_from(&mut r)? {
        records.push(rec);
    }

    // Log records may be unsorted and contain duplicates for the same key
    // across multiple save_pages calls. Sort by key and deduplicate so the
    // output index has exactly one record per key. Stable sort preserves
    // temporal order for same-key records: the first occurrence (lowest
    // snapshot_id) survives dedup.
    records.sort_by_key(|r| r.key);
    records.dedup_by_key(|r| r.key);

    Ok(records)
}

fn compact_blobs(dir: &Path, records: &mut [ChunkRecord]) -> io::Result<()> {
    // Group non-zero records by worker_id, keyed by old offset.
    // BTreeMap sorts by offset so we write blobs sequentially in the new file.
    let mut by_worker: BTreeMap<u8, BTreeMap<u64, Vec<usize>>> = BTreeMap::new();

    for (i, rec) in records.iter().enumerate() {
        if rec.kind == ChunkKind::Zero {
            continue;
        }
        by_worker
            .entry(rec.worker_id)
            .or_default()
            .entry(rec.offset)
            .or_default()
            .push(i);
    }

    let blobs_dir = dir.join("blobs");

    for (&worker_id, offset_map) in &by_worker {
        let old_path = blobs_dir.join(format!("worker_{worker_id}.blob"));
        let old = File::open(&old_path)?;
        let tmp_path = blobs_dir.join(format!(".worker_{worker_id}.blob.tmp"));
        let new_path = blobs_dir.join(format!("worker_{worker_id}.blob"));

        let mut w = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut new_offset: u64 = 0;

        for (&old_offset, indices) in offset_map {
            let first_idx = indices[0];
            let blob_len = records[first_idx].len as usize;
            let mut blob = vec![0u8; blob_len];
            old.read_exact_at(&mut blob, old_offset)?;
            w.write_all(&blob)?;
            for &idx in indices {
                records[idx].offset = new_offset;
            }
            new_offset += blob_len as u64;
        }

        w.sync_all()?;
        std::fs::rename(&tmp_path, &new_path)?;
    }

    Ok(())
}

fn write_index_file(dir: &Path, records: &[ChunkRecord]) -> io::Result<()> {
    let idx_path = dir.join("index.bxdb");
    let tmp_path = idx_path.with_extension("bxdb.tmp");

    {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = std::io::BufWriter::new(f);
        write_index_header(&mut w)?;
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
    let used: HashSet<u8> = records.iter().map(|r| r.worker_id).collect();
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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use crate::btree::BlobReaders;
use crate::chunk::{ChunkKind, ChunkRecord, MAGIC_LOG, PAGE_SIZE, pa_of};
use crate::format::read_and_verify_header;

thread_local! {
    static ZSTD_DEC: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new({
            let mut d = zstd::bulk::Decompressor::new().expect("zstd decompressor init");
            d.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(12))
                .expect("zstd window log");
            d
        });
}

#[derive(Debug)]
pub struct CheckReport {
    pub total_records: usize,
    pub full_count: usize,
    pub delta_count: usize,
    pub zero_count: usize,
    pub errors: Vec<String>,
    pub blob_file_sizes: Vec<(u8, u64)>,
}

impl CheckReport {
    pub fn ok(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn check(dir: &Path) -> io::Result<CheckReport> {
    let log_path = dir.join("chunks.log");
    let blobs_dir = dir.join("blobs");

    let file = File::open(&log_path)?;
    let mut r = BufReader::new(file);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;

    let mut blob_sizes: BTreeMap<u8, u64> = BTreeMap::new();
    if blobs_dir.exists() {
        for entry in std::fs::read_dir(&blobs_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(suffix) = name_str.strip_prefix("worker_") {
                if let Some(id_str) = suffix.strip_suffix(".blob") {
                    if let Ok(id) = id_str.parse::<u8>() {
                        blob_sizes.insert(id, entry.metadata()?.len());
                    }
                }
            }
        }
    }

    let blob_readers = BlobReaders::new(dir)?;

    let mut report = CheckReport {
        total_records: 0,
        full_count: 0,
        delta_count: 0,
        zero_count: 0,
        errors: Vec::new(),
        blob_file_sizes: blob_sizes.iter().map(|(&k, &v)| (k, v)).collect(),
    };

    let mut full_index: BTreeMap<u64, ChunkRecord> = BTreeMap::new();
    let mut all_records: Vec<ChunkRecord> = Vec::new();

    while let Some(rec) = ChunkRecord::read_from(&mut r)? {
        if !blob_sizes.contains_key(&rec.worker_id) {
            report.errors.push(format!(
                "record pa={pa} snap={snap}: worker_id {wid} has no blob file",
                pa = pa_of(rec.key),
                snap = crate::chunk::snapshot_of(rec.key),
                wid = rec.worker_id,
            ));
        }
        let blob_len = blob_sizes.get(&rec.worker_id).copied().unwrap_or(0);

        match rec.kind {
            ChunkKind::Full => {
                check_full(&mut report, &blob_readers, &rec, blob_len)?;
                full_index.insert(rec.key, rec);
                report.full_count += 1;
            }
            ChunkKind::Delta => {
                check_delta(&mut report, &blob_readers, &rec, blob_len)?;
                report.delta_count += 1;
            }
            ChunkKind::Zero => {
                report.zero_count += 1;
                full_index.insert(rec.key, rec);
            }
        }
        report.total_records += 1;
        all_records.push(rec);
    }

    // Verify Delta base_key references.
    for rec in all_records {
        if rec.kind != ChunkKind::Delta {
            continue;
        }
        if rec.base_key == 0 {
            report.errors.push(format!(
                "Delta pa={pa} snap={snap}: base_key is zero",
                pa = pa_of(rec.key),
                snap = crate::chunk::snapshot_of(rec.key),
            ));
            continue;
        }
        match full_index.get(&rec.base_key) {
            None => {
                report.errors.push(format!(
                    "Delta pa={pa} snap={snap}: base_key={bk} (pa={bkpa} snap={bksnap}) not found in index",
                    pa = pa_of(rec.key),
                    snap = crate::chunk::snapshot_of(rec.key),
                    bk = rec.base_key,
                    bkpa = pa_of(rec.base_key),
                    bksnap = crate::chunk::snapshot_of(rec.base_key),
                ));
            }
            Some(base) if base.kind != ChunkKind::Full => {
                report.errors.push(format!(
                    "Delta pa={pa} snap={snap}: base_key={bk} points to {base_kind:?} (not Full)",
                    pa = pa_of(rec.key),
                    snap = crate::chunk::snapshot_of(rec.key),
                    bk = rec.base_key,
                    base_kind = base.kind,
                ));
            }
            Some(_) => {}
        }
    }

    Ok(report)
}

fn check_full(
    report: &mut CheckReport,
    blob_readers: &BlobReaders,
    rec: &ChunkRecord,
    blob_len: u64,
) -> io::Result<()> {
    if rec.len == 0 {
        return Ok(());
    }
    let end = rec.offset.saturating_add(rec.len as u64);
    if end > blob_len {
        report.errors.push(format!(
            "Full  pa={pa} snap={snap} worker={wid} offset={off} len={len}: end {end} > blob file size {blob_len}",
            pa = pa_of(rec.key),
            snap = crate::chunk::snapshot_of(rec.key),
            wid = rec.worker_id,
            off = rec.offset,
            len = rec.len,
        ));
        return Ok(());
    }
    let mmap = blob_readers.mmap(rec.worker_id);
    let start = rec.offset as usize;
    let end = start + rec.len as usize;
    let blob = mmap.as_bytes().get(start..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "blob read out of range")
    })?;
    match ZSTD_DEC.with(|d| d.borrow_mut().decompress(blob, PAGE_SIZE + 1)) {
        Ok(out) if out.len() == PAGE_SIZE => {}
        Ok(out) => {
            let pa = pa_of(rec.key);
            let snap = crate::chunk::snapshot_of(rec.key);
            let wid = rec.worker_id;
            let actual = out.len();
            report.errors.push(format!(
                "Full  pa={pa} snap={snap} worker={wid}: decompressed {actual} bytes (expected 4096)",
            ));
        }
        Err(e) => {
            report.errors.push(format!(
                "Full  pa={pa} snap={snap} worker={wid}: zstd decompress failed: {e}",
                pa = pa_of(rec.key),
                snap = crate::chunk::snapshot_of(rec.key),
                wid = rec.worker_id,
            ));
        }
    }
    Ok(())
}

fn check_delta(
    report: &mut CheckReport,
    blob_readers: &BlobReaders,
    rec: &ChunkRecord,
    blob_len: u64,
) -> io::Result<()> {
    if rec.len == 0 {
        return Ok(());
    }
    let end = rec.offset.saturating_add(rec.len as u64);
    if end > blob_len {
        report.errors.push(format!(
            "Delta pa={pa} snap={snap} worker={wid} offset={off} len={len}: end {end} > blob file size {blob_len}",
            pa = pa_of(rec.key),
            snap = crate::chunk::snapshot_of(rec.key),
            wid = rec.worker_id,
            off = rec.offset,
            len = rec.len,
        ));
        return Ok(());
    }
    if rec.len as usize % 10 != 0 {
        report.errors.push(format!(
            "Delta pa={pa} snap={snap} worker={wid}: blob length {len} not a multiple of 10",
            pa = pa_of(rec.key),
            snap = crate::chunk::snapshot_of(rec.key),
            wid = rec.worker_id,
            len = rec.len,
        ));
        return Ok(());
    }
    let mmap = blob_readers.mmap(rec.worker_id);
    let start = rec.offset as usize;
    let end = start + rec.len as usize;
    let delta_blob = mmap.as_bytes().get(start..end).ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "delta blob out of range")
    })?;
    for chunk in delta_blob.chunks_exact(10) {
        let idx = u16::from_le_bytes([chunk[0], chunk[1]]) as usize;
        if idx >= crate::chunk::PAGE_WORDS {
            report.errors.push(format!(
                "Delta pa={pa} snap={snap} worker={wid}: word index {idx} out of range (max {max})",
                pa = pa_of(rec.key),
                snap = crate::chunk::snapshot_of(rec.key),
                wid = rec.worker_id,
                idx = idx,
                max = crate::chunk::PAGE_WORDS - 1,
            ));
            break;
        }
    }
    Ok(())
}

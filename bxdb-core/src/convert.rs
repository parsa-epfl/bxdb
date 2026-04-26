use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::chunk::{
    ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, LOG_RECORD_BASE_SIZE, MAGIC_IDX, MAGIC_LOG,
};
use crate::format::{read_and_verify_header, write_index_header, write_log_header};

pub fn to_btree(dir: &Path) -> io::Result<()> {
    let log_path = dir.join("chunks.log");
    let idx_path = dir.join("index.bxdb");

    let log = File::open(&log_path)?;
    let meta = log.metadata()?;
    let mut r = BufReader::new(log);
    read_and_verify_header(&mut r, &MAGIC_LOG)?;

    let capacity = (meta.len().saturating_sub(HEADER_SIZE as u64)
        / LOG_RECORD_BASE_SIZE as u64) as usize;
    let mut records: Vec<ChunkRecord> = Vec::with_capacity(capacity);
    while let Some(rec) = ChunkRecord::read_from(&mut r)? {
        records.push(rec);
    }
    records.sort_by_key(|r| r.key);
    records.dedup_by_key(|r| r.key);

    let tmp = idx_path.with_extension("bxdb.tmp");
    {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(f);
        write_index_header(&mut w)?;
        let mut buf = [0u8; FIXED_RECORD_SIZE];
        for rec in &records {
            rec.encode_fixed(&mut buf);
            w.write_all(&buf)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    std::fs::rename(&tmp, &idx_path)?;
    Ok(())
}

pub fn to_log(dir: &Path) -> io::Result<()> {
    let idx_path = dir.join("index.bxdb");
    let log_path = dir.join("chunks.log");

    let idx = File::open(&idx_path)?;
    let meta = idx.metadata()?;
    let mut r = BufReader::new(idx);
    read_and_verify_header(&mut r, &MAGIC_IDX)?;

    let capacity = (meta.len().saturating_sub(HEADER_SIZE as u64)
        / FIXED_RECORD_SIZE as u64) as usize;
    let mut records: Vec<ChunkRecord> = Vec::with_capacity(capacity);
    let mut buf = [0u8; FIXED_RECORD_SIZE];
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => records.push(ChunkRecord::decode_fixed(&buf)?),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    let tmp = log_path.with_extension("log.tmp");
    {
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(f);
        write_log_header(&mut w)?;
        for rec in &records {
            rec.write_to(&mut w)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    std::fs::rename(&tmp, &log_path)?;
    Ok(())
}

use std::fs::File;
use std::io::Read;
use std::path::Path;

use bxdb::btree::BtreeDb;

const PAGE_SIZE: usize = 4096;

struct RawFile {
    addrs: Vec<u64>,
    data: Vec<u8>,
}

fn load_raw_file(path: &Path) -> std::io::Result<RawFile> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    if buf.len() < 8 { return Ok(RawFile{addrs:vec![], data:vec![]}); }
    let n = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
    let addr_end = 8 + n * 8;
    let mut addrs = Vec::with_capacity(n);
    for i in 0..n { addrs.push(u64::from_le_bytes(buf[8+i*8..8+(i+1)*8].try_into().unwrap())); }
    let data_off = addr_end.next_multiple_of(PAGE_SIZE);
    Ok(RawFile { addrs, data: if data_off < buf.len() { buf[data_off..].to_vec() } else { vec![] } })
}

/// files: latest to oldest (files[0] = snap N, files[N] = snap 0)
fn raw_backward_search(files: &[RawFile], addr: u64) -> Option<&[u8]> {
    for f in files {
        if let Ok(idx) = f.addrs.binary_search(&addr) {
            let off = idx * PAGE_SIZE;
            if off + PAGE_SIZE <= f.data.len() {
                return Some(&f.data[off..off+PAGE_SIZE]);
            }
        }
    }
    None
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: compare-bxdb-raw-backward <db_dir> <raw_dir> <snap_id>");
        std::process::exit(1);
    }
    let db_dir = &args[1];
    let raw_dir = &args[2];
    let snap_id: u32 = args[3].parse().unwrap();

    eprintln!("Opening bxdb at {}", db_dir);
    let db = BtreeDb::open(db_dir).expect("failed");

    eprintln!("Loading raw files (latest to oldest)");
    let mut raw_files = Vec::new();
    for sid in (0..=snap_id).rev() {
        let path = format!("{}/{}", raw_dir, sid);
        raw_files.push(load_raw_file(Path::new(&path)).expect("failed raw"));
        eprintln!("  file {}: {} pages", sid, raw_files.last().unwrap().addrs.len());
    }

    let total_pages = 16_777_216u64;
    let sample_count = 10000u64;
    let mut mismatches = 0u64;
    let mut lcg: u64 = 0x_deadbeef;
    for i in 0..sample_count {
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let pa = lcg % total_pages;
        let addr = pa * PAGE_SIZE as u64;

        let mut bxdb_buf = [0u8; PAGE_SIZE];
        let bxdb_found = db.load_page(&mut bxdb_buf, pa, snap_id).expect("bxdb error");
        let raw_pg = raw_backward_search(&raw_files, addr);

        let bxdb_data: &[u8] = if bxdb_found { &bxdb_buf } else { &[0u8; PAGE_SIZE] };
        let raw_data = raw_pg.unwrap_or(&[0u8; PAGE_SIZE]);

        if bxdb_data != raw_data {
            mismatches += 1;
            eprintln!(
                "MISMATCH snap={} pa={} addr=0x{:x} bxdb_found={} raw_found={}",
                snap_id, pa, addr,
                bxdb_found,
                raw_pg.is_some()
            );
            eprintln!("  bxdb[0:8]={:02x?}", &bxdb_data[0..8.min(bxdb_data.len())]);
            eprintln!("  raw [0:8]={:02x?}", &raw_data[0..8.min(raw_data.len())]);
            let mut diff_at = 0u64;
            while diff_at < PAGE_SIZE as u64 && bxdb_data[diff_at as usize] == raw_data[diff_at as usize] {
                diff_at += 1;
            }
            eprintln!("  first_diff_byte={} bxdb=0x{:02x} raw=0x{:02x}",
                      diff_at,
                      if (diff_at as usize) < bxdb_data.len() { bxdb_data[diff_at as usize] } else { 0 },
                      if (diff_at as usize) < raw_data.len() { raw_data[diff_at as usize] } else { 0 });
            if mismatches >= 5 { break; }
        }

        if i > 0 && i % 2000 == 0 {
            eprintln!("  sampled {} pages, {} mismatches", i, mismatches);
        }
    }

    eprintln!("Done snap={}: {} random pages sampled, {} mismatches",
              snap_id, sample_count, mismatches);
}

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use bxdb::btree::BtreeDb;

const PAGE_SIZE: usize = 4096;

fn read_raw_file(path: &Path) -> io::Result<(Vec<u64>, Vec<u8>)> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;

    if buf.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file too small"));
    }

    let n = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
    if n == 0 {
        return Ok((vec![], vec![]));
    }

    let addr_size = 8 + n * 8;
    if buf.len() < addr_size {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated address array"));
    }

    let mut addrs = Vec::with_capacity(n);
    for i in 0..n {
        let off = 8 + i * 8;
        addrs.push(u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
    }

    let data_offset = addr_size.next_multiple_of(PAGE_SIZE);
    let data = buf[data_offset..].to_vec();

    Ok((addrs, data))
}

fn raw_page_data<'a>(addrs: &[u64], data: &'a [u8], addr: u64) -> Option<&'a [u8]> {
    match addrs.binary_search(&addr) {
        Ok(idx) => {
            let off = idx * PAGE_SIZE;
            if off + PAGE_SIZE <= data.len() {
                Some(&data[off..off + PAGE_SIZE])
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

fn hex8(b: &[u8]) -> String {
    b.iter().take(8).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join("")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: compare_bxdb_raw <db_dir> <raw_test_dir> <snap_id>");
        std::process::exit(1);
    }
    let db_dir = &args[1];
    let raw_dir = &args[2];
    let snap_id: u32 = args[3].parse().unwrap();

    eprintln!("Opening bxdb at {}", db_dir);
    let db = BtreeDb::open(db_dir).expect("failed to open bxdb");

    let raw_path = format!("{}/{}", raw_dir, snap_id);
    eprintln!("Reading raw file {}", raw_path);
    let (addrs, data) = read_raw_file(Path::new(&raw_path))
        .expect("failed to read raw file");

    eprintln!("Raw file has {} pages", addrs.len());

    let mut mismatch_count = 0u64;
    let mut zero_addrs_count = 0u64;

    for (i, &addr) in addrs.iter().enumerate() {
        let pa_index = addr / PAGE_SIZE as u64;
        let raw_data = raw_page_data(&addrs, &data, addr).unwrap();

        let mut bxdb_page = [0u8; PAGE_SIZE];
        match db.load_page(&mut bxdb_page, pa_index, snap_id) {
            Ok(true) => {
                if bxdb_page.as_slice() != raw_data {
                    mismatch_count += 1;
                    eprintln!(
                        "MISMATCH snap={} addr=0x{:x} pa={} ",
                        snap_id, addr, pa_index
                    );
                    eprintln!("  bxdb[0:8]={}", hex8(&bxdb_page));
                    eprintln!("  raw [0:8]={}", hex8(raw_data));

                    let mut first_diff = 0;
                    while first_diff < PAGE_SIZE && bxdb_page[first_diff] == raw_data[first_diff] {
                        first_diff += 1;
                    }
                    eprintln!(
                        "  first_diff_off={} bxdb=0x{:02x} raw=0x{:02x}",
                        first_diff,
                        if first_diff < PAGE_SIZE { bxdb_page[first_diff] } else { 0 },
                        if first_diff < PAGE_SIZE { raw_data[first_diff] } else { 0 },
                    );
                    if mismatch_count >= 10 {
                        eprintln!("Too many mismatches, stopping.");
                        std::process::exit(1);
                    }
                }
            }
            Ok(false) => {
                mismatch_count += 1;
                let all_zero = raw_data.iter().all(|&b| b == 0);
                if all_zero {
                    zero_addrs_count += 1;
                    // This is expected: bxdb doesn't have this page, raw has zeros
                } else {
                    eprintln!(
                        "MISSING-FROM-BXDB snap={} addr=0x{:x} pa={} raw[0:8]={}",
                        snap_id, addr, pa_index, hex8(raw_data)
                    );
                    if mismatch_count >= 10 {
                        eprintln!("Too many mismatches, stopping.");
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!("ERROR reading pa={} snap={}: {}", pa_index, snap_id, e);
                std::process::exit(1);
            }
        }

        if i > 0 && i % 100000 == 0 {
            eprintln!("  Compared {}/{} pages...", i, addrs.len());
        }
    }

    eprintln!(
        "Done. snap={}: {} pages in raw file, {} mismatches, {} zero-pages-not-in-bxdb",
        snap_id, addrs.len(), mismatch_count, zero_addrs_count
    );
}

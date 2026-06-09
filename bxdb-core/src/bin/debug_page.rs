use std::fs::File;
use std::io::Read;
use std::path::Path;
use bxdb::btree::PageStore;

const PAGE_SIZE: u64 = 4096;

fn find_in_raw(raw_dir: &str, max_snap: u32, addr: u64) {
    for sid in 0..=max_snap {
        let path = format!("{}/{}", raw_dir, sid);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        if buf.len() < 8 { continue; }
        let n = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
        for i in 0..n {
            let off = 8 + i * 8;
            let a = u64::from_le_bytes(buf[off..off+8].try_into().unwrap());
            if a == addr {
                let data_off = (8 + n * 8).next_multiple_of(PAGE_SIZE as usize);
                let pg_off = data_off + i * PAGE_SIZE as usize;
                let data = &buf[pg_off..pg_off+PAGE_SIZE as usize];
                let all_zero = data.iter().all(|&b| b == 0);
                println!("  RAW file {}: addr=0x{:x} zero={} first8={:02x?}",
                         sid, addr, all_zero, &data[0..8.min(data.len())]);
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: debug-page <db_dir> <raw_dir> <pa> [max_snap]");
        return;
    }
    let db_dir = &args[1];
    let raw_dir = &args[2];
    let pa: u64 = args[3].parse().unwrap();
    let max_snap: u32 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(103);

    let addr = pa * PAGE_SIZE;
    println!("PA={} addr=0x{:x}", pa, addr);

    println!("\n--- Raw files containing this page ---");
    find_in_raw(raw_dir, max_snap, addr);

    println!("\n--- BXDB records for this PA ---");
    let store = PageStore::open(Path::new(db_dir)).expect("open bxdb");
    for snap in 0..=max_snap {
        let key = bxdb::chunk::encode_key(pa, snap);
        match store.exact_lookup(key) {
            Ok(Some(rec)) => {
                println!("  snap={} key=0x{:x} kind={:?} base_key=0x{:x} worker={} offset={} len={}",
                         snap, key, rec.kind, rec.base_key, rec.worker_id, rec.offset, rec.len);
            }
            _ => {}
        }
    }
}

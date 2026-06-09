use std::fs::File;
use std::io::Read;
use std::path::Path;

use bxdb::btree::PageStore;
use bxdb::chunk::{self, PAGE_SIZE, apply_delta_patch, ChunkKind};

fn read_raw_page(raw_dir: &str, snap: u32, addr: u64) -> Option<[u8; PAGE_SIZE]> {
    let path = format!("{}/{}", raw_dir, snap);
    let mut f = File::open(&path).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    if buf.len() < 8 { return None; }
    let n = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
    for i in 0..n {
        let off = 8 + i * 8;
        let a = u64::from_le_bytes(buf[off..off+8].try_into().unwrap());
        if a == addr {
            let data_off = (8 + n * 8).next_multiple_of(PAGE_SIZE);
            let pg_off = data_off + i * PAGE_SIZE;
            let mut page = [0u8; PAGE_SIZE];
            page.copy_from_slice(&buf[pg_off..pg_off+PAGE_SIZE]);
            return Some(page);
        }
    }
    None
}

fn bxdb_resolve(store: &PageStore, pa: u64, snap: u32) -> Option<[u8; PAGE_SIZE]> {
    let key = chunk::encode_key(pa, snap);
    let rec = store.exact_lookup(key).ok()??;
    let mut out = [0u8; PAGE_SIZE];
    match rec.kind {
        ChunkKind::Full => {
            store.decompress_blob_into(&rec, &mut out).ok()?;
        }
        ChunkKind::Zero => {
            // out already zero
        }
        ChunkKind::Delta => {
            let base_rec = store.exact_lookup(rec.base_key).ok()??;
            let mut base = [0u8; PAGE_SIZE];
            store.decompress_blob_into(&base_rec, &mut base).ok()?;
            let mmap = store.blob_readers.mmap(rec.worker_id);
            let start = rec.offset as usize;
            let end = start + rec.len as usize;
            let delta_blob = mmap.as_bytes().get(start..end)?;
            apply_delta_patch(&base, delta_blob, &mut out).ok()?;
        }
    }
    Some(out)
}

fn first8(p: &[u8]) -> String {
    format!("{:02x?}", &p[0..8])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: debug-page-resolve <db_dir> <raw_dir> <pa> <snap>");
        return;
    }
    let db_dir = &args[1];
    let raw_dir = &args[2];
    let pa: u64 = args[3].parse().unwrap();
    let snap: u32 = args[4].parse().unwrap();

    let addr = pa * PAGE_SIZE as u64;
    let store = PageStore::open(Path::new(db_dir)).expect("open bxdb");

    println!("PA={} snap={} addr=0x{:x}", pa, snap, addr);

    // Show all records for this PA up to snap
    println!("\nRecords up to snap {}:", snap);
    for s in 0..=snap {
        let key = chunk::encode_key(pa, s);
        if let Ok(Some(rec)) = store.exact_lookup(key) {
            println!("  s={} key=0x{:x} kind={:?} base=0x{:x} w={} off={} len={}",
                     s, key, rec.kind, rec.base_key, rec.worker_id, rec.offset, rec.len);
        }
    }

    // Resolve via bxdb at this snap
    println!("\nBXDB resolution at snap {}:", snap);
    match bxdb_resolve(&store, pa, snap) {
        Some(p) => println!("  bxdb[0:8]={}", first8(&p)),
        None => println!("  NOT FOUND"),
    }

    // Read from raw file
    println!("\nRaw file {}:", snap);
    match read_raw_page(raw_dir, snap, addr) {
        Some(p) => println!("  raw[0:8]={}", first8(&p)),
        None => println!("  NOT IN FILE"),
    }

    // For comparison, also do backward search from snap down to 0
    println!("\nRaw backward search from snap {}:", snap);
    let mut found = false;
    for s in (0..=snap).rev() {
        match read_raw_page(raw_dir, s, addr) {
            Some(p) => {
                println!("  found in file {}: [0:8]={}", s, first8(&p));
                found = true;
                break;
            }
            None => {}
        }
    }
    if !found {
        println!("  NOT FOUND (all-zero)");
    }
}

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use bxdb;
use bxdb::cache::{self, SharedCache, CACHE_PAGE_SIZE, TOTAL_BYTES};

fn usage(prog: &str) {
    eprintln!("usage: {prog} <create|inspect|delete> <db-dir>");
    eprintln!("  create   Create the shared-memory page cache for a database");
    eprintln!("  inspect  Show cache metadata and slot contents");
    eprintln!("  delete   Remove the shared-memory cache files");
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        usage(&args[0]);
        return ExitCode::from(2);
    }
    let cmd = args[1].as_str();
    let dir = PathBuf::from(&args[2]);

    let result = match cmd {
        "create" => cmd_create(&dir),
        "inspect" => cmd_inspect(&dir),
        "delete" => cmd_delete(&dir),
        _ => {
            usage(&args[0]);
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bxdb-cache {cmd}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn cmd_create(dir: &Path) -> std::io::Result<()> {
    let canonical = canonicalize(dir);
    let cache_path = bxdb::shm_cache_path(&canonical)?;
    println!("creating cache at {}", cache_path.display());
    SharedCache::create(&cache_path, &canonical)?;
    println!("done ({} MiB)", TOTAL_BYTES / (1024 * 1024));
    Ok(())
}

fn cmd_inspect(dir: &Path) -> std::io::Result<()> {
    let canonical = canonicalize(dir);
    let cache_path = bxdb::shm_cache_path(&canonical)?;
    let cache = SharedCache::open(&cache_path, &canonical)?;

    println!("cache file    : {}", cache_path.display());
    println!("file size     : {} MiB", TOTAL_BYTES / (1024 * 1024));
    println!("page size     : {} KiB", CACHE_PAGE_SIZE / 1024);
    println!("total slots   : {}", cache::TOTAL_SLOTS);
    println!(
        "metadata      : {} MiB",
        cache::METADATA_BYTES / (1024 * 1024)
    );
    println!(
        "data region   : {} MiB",
        cache::DATA_BYTES / (1024 * 1024)
    );
    println!("identity      : {:?}", cache.identity_path());

    let mut occupied = 0usize;
    for slot in 0..cache::TOTAL_SLOTS {
        if cache.slot_key(slot).is_some() {
            occupied += 1;
        }
    }
    println!("occupied      : {occupied} / {}", cache::TOTAL_SLOTS);
    println!();

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    // Emit a hexdump of every occupied slot as "bxdb-cache inspect | hexdump -C".
    for slot in 0..cache::TOTAL_SLOTS {
        let Some(key) = cache.slot_key(slot) else {
            continue;
        };
        let ts = cache.slot_timestamp(slot);
        let mut page = [0u8; CACHE_PAGE_SIZE];
        cache.read_slot_page(slot, &mut page);

        writeln!(
            out,
            "--- slot {slot:6}  key={key:016x}  lru_ts={ts:3} ---"
        )?;
        hex_slice(&page, &mut out)?;
    }

    Ok(())
}

fn cmd_delete(dir: &Path) -> std::io::Result<()> {
    let canonical = canonicalize(dir);
    let cache_path = bxdb::shm_cache_path(&canonical)?;
    let parent = cache_path.parent().unwrap();

    match std::fs::remove_file(&cache_path) {
        Ok(()) => println!("removed {}", cache_path.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("cache file not found: {}", cache_path.display());
        }
        Err(e) => return Err(e),
    }

    // Remove the hash directory if it is now empty.
    let _ = std::fs::remove_dir(parent);

    Ok(())
}

/// Canonicalise or return the raw path on failure.
fn canonicalize(dir: &Path) -> PathBuf {
    dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf())
}

/// Write `buf` as a byte-dump:  offset | hex16 | ascii.
fn hex_slice(buf: &[u8], w: &mut impl Write) -> std::io::Result<()> {
    for (i, chunk) in buf.chunks(16).enumerate() {
        write!(w, "{:07x}  ", i * 16)?;
        for (j, &b) in chunk.iter().enumerate() {
            write!(w, "{b:02x}")?;
            if j == 7 {
                write!(w, " ")?;
            }
        }
        let pad = if chunk.len() < 16 { 16 - chunk.len() } else { 0 };
        for _ in 0..pad {
            write!(w, "  ")?;
        }
        if chunk.len() <= 7 {
            write!(w, " ")?;
        }
        write!(w, " |")?;
        for &b in chunk {
            let c = if b.is_ascii_graphic() || b == b' ' { b } else { b'.' };
            write!(w, "{}", c as char)?;
        }
        writeln!(w, "|")?;
    }
    Ok(())
}

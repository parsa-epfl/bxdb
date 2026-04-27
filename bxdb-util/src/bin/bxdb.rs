use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use bxdb::cache::{self, CACHE_PAGE_SIZE, SharedCache, TOTAL_BYTES};

/// bxdb — database utility tool
#[derive(Parser)]
#[command(name = "bxdb", about, version, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Convert between database formats
    Convert {
        #[command(subcommand)]
        cmd: ConvertCmd,
    },
    /// Purge records above a snapshot threshold
    Purge {
        /// Delete records with snapshot_id > this value
        threshold: u32,
        /// Database directory (must contain index.bxdb and/or chunks.log)
        dir: PathBuf,
    },
    /// Manage the shared-memory page cache
    Cache {
        #[command(subcommand)]
        cmd: CacheCmd,
    },
}

#[derive(Subcommand)]
enum ConvertCmd {
    /// Convert from append-only log (chunks.log) to B-tree (index.bxdb)
    ToBtree {
        /// Database directory
        dir: PathBuf,
    },
    /// Convert from B-tree (index.bxdb) to append-only log (chunks.log)
    ToLog {
        /// Database directory
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum CacheCmd {
    /// Create the shared-memory page cache for a database
    Create {
        /// Database directory
        dir: PathBuf,
    },
    /// Show cache metadata and slot contents
    Inspect {
        /// Database directory
        dir: PathBuf,
    },
    /// Remove the shared-memory cache files
    Delete {
        /// Database directory
        dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Cmd::Convert { cmd } => match cmd {
            ConvertCmd::ToBtree { dir } => {
                if let Err(e) = bxdb::convert::to_btree(&dir) {
                    eprintln!("bxdb convert to-btree: {e}");
                    return ExitCode::FAILURE;
                }
            }
            ConvertCmd::ToLog { dir } => {
                if let Err(e) = bxdb::convert::to_log(&dir) {
                    eprintln!("bxdb convert to-log: {e}");
                    return ExitCode::FAILURE;
                }
            }
        },
        Cmd::Purge { threshold, dir } => match bxdb::purge::purge(&dir, threshold) {
            Ok(removed) => {
                println!("removed {removed} records with snapshot_id > {threshold}");
            }
            Err(e) => {
                eprintln!("bxdb purge: {e}");
                return ExitCode::FAILURE;
            }
        },
        Cmd::Cache { cmd } => {
            if let Err(e) = run_cache(cmd) {
                eprintln!("bxdb cache: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    ExitCode::SUCCESS
}

fn run_cache(cmd: CacheCmd) -> std::io::Result<()> {
    match cmd {
        CacheCmd::Create { dir } => cmd_cache_create(&dir),
        CacheCmd::Inspect { dir } => cmd_cache_inspect(&dir),
        CacheCmd::Delete { dir } => cmd_cache_delete(&dir),
    }
}

fn cmd_cache_create(dir: &Path) -> std::io::Result<()> {
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let cache_path = bxdb::shm_cache_path(&canonical)?;
    println!("creating cache at {}", cache_path.display());
    SharedCache::create(&cache_path, &canonical)?;
    println!("done ({} MiB)", TOTAL_BYTES / (1024 * 1024));
    Ok(())
}

fn cmd_cache_inspect(dir: &Path) -> std::io::Result<()> {
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
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
    println!("data region   : {} MiB", cache::DATA_BYTES / (1024 * 1024));
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

    for slot in 0..cache::TOTAL_SLOTS {
        let Some(key) = cache.slot_key(slot) else {
            continue;
        };
        let ts = cache.slot_timestamp(slot);
        let mut page = [0u8; CACHE_PAGE_SIZE];
        cache.read_slot_page(slot, &mut page);

        writeln!(out, "--- slot {slot:6}  key={key:016x}  lru_ts={ts:3} ---")?;
        hex_slice(&page, &mut out)?;
    }

    Ok(())
}

fn cmd_cache_delete(dir: &Path) -> std::io::Result<()> {
    let canonical = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let cache_path = bxdb::shm_cache_path(&canonical)?;
    let parent = cache_path.parent().unwrap();

    match std::fs::remove_file(&cache_path) {
        Ok(()) => println!("removed {}", cache_path.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("cache file not found: {}", cache_path.display());
        }
        Err(e) => return Err(e),
    }

    let _ = std::fs::remove_dir(parent);

    Ok(())
}

fn hex_slice(buf: &[u8], w: &mut impl Write) -> std::io::Result<()> {
    for (i, chunk) in buf.chunks(16).enumerate() {
        write!(w, "{:07x}  ", i * 16)?;
        for (j, &b) in chunk.iter().enumerate() {
            write!(w, "{b:02x}")?;
            if j == 7 {
                write!(w, " ")?;
            }
        }
        let pad = if chunk.len() < 16 {
            16 - chunk.len()
        } else {
            0
        };
        for _ in 0..pad {
            write!(w, "  ")?;
        }
        if chunk.len() <= 7 {
            write!(w, " ")?;
        }
        write!(w, " |")?;
        for &b in chunk {
            let c = if b.is_ascii_graphic() || b == b' ' {
                b
            } else {
                b'.'
            };
            write!(w, "{}", c as char)?;
        }
        writeln!(w, "|")?;
    }
    Ok(())
}

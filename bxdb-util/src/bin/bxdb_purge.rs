use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        usage(&args[0]);
        return ExitCode::from(2);
    }
    let threshold: u32 = match args[1].parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("bxdb-purge: invalid snapshot threshold: {}", args[1]);
            usage(&args[0]);
            return ExitCode::from(2);
        }
    };
    let dir = PathBuf::from(&args[2]);

    match bxdb::purge::purge(&dir, threshold) {
        Ok(removed) => {
            println!("removed {removed} records with snapshot_id > {threshold}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("bxdb-purge: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage(prog: &str) {
    eprintln!("usage: {prog} <snapshot-threshold> <dir>");
    eprintln!();
    eprintln!("  snapshot-threshold  Delete records with snapshot_id > this value");
    eprintln!("  dir                 Database directory (must contain index.bxdb)");
}

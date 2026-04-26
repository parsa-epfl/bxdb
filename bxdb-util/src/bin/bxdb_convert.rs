use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        usage(&args[0]);
        return ExitCode::from(2);
    }
    let cmd = args[1].as_str();
    let dir = PathBuf::from(&args[2]);
    let result = match cmd {
        "to-btree" => bxdb::convert::to_btree(&dir),
        "to-log" => bxdb::convert::to_log(&dir),
        _ => {
            usage(&args[0]);
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bxdb-convert: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage(prog: &str) {
    eprintln!("usage: {prog} <to-btree|to-log> <dir>");
}

use dftobin::DftoBin;
use std::{env, process};

fn main() {
    // Keep the binary entry point small: all fallible work happens in `run`,
    // and the CLI maps errors to stderr plus a non-zero exit code.
    if let Err(err) = run() {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run() -> dftobin::Result<()> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "dftobin".to_owned());

    // Required input: a Parquet file path. The optional second positional
    // argument controls the output root directory.
    let Some(parquet_file) = args.next() else {
        eprintln!("usage: {program} <input.parquet> [output_dir]");
        process::exit(2);
    };
    let output_dir = args.next();

    // Reject extra positional arguments so accidental shell expansion or typos
    // do not silently change the conversion target.
    if args.next().is_some() {
        eprintln!("usage: {program} <input.parquet> [output_dir]");
        process::exit(2);
    }

    // The library owns Parquet loading and binary layout. The CLI only chooses
    // whether to use the default output root or the user-provided one.
    let converter = DftoBin::new(&parquet_file)?;
    match output_dir {
        Some(output_dir) => converter.to_bin_at(output_dir),
        None => converter.to_bin(),
    }
}

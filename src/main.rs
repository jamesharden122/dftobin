use dftobin::DftoBin;
use std::{env, process};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run() -> dftobin::Result<()> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "dftobin".to_owned());
    let Some(parquet_file) = args.next() else {
        eprintln!("usage: {program} <input.parquet> [output_dir]");
        process::exit(2);
    };
    let output_dir = args.next();

    if args.next().is_some() {
        eprintln!("usage: {program} <input.parquet> [output_dir]");
        process::exit(2);
    }

    let converter = DftoBin::new(&parquet_file)?;
    match output_dir {
        Some(output_dir) => converter.to_bin_at(output_dir),
        None => converter.to_bin(),
    }
}

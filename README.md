# dftobin

`dftobin` converts a Parquet file into a directory of column-oriented binary files.
It is a small Rust crate with:

- a library API for converting Parquet rows into per-column `.bin` outputs
- a binary entry point for command-line use

## What it does

Given an input Parquet file, the converter:

- reads the file with the `parquet` crate
- creates an output directory named after the input file stem
- writes one binary file per Parquet column
- encodes values in big-endian order

The generated files are named:

```text
<output_dir>/<input_stem>/<column_name>_<PHYSICAL_TYPE>.bin
```

Examples:

- `prices.parquet` -> `./prices/close_DOUBLE.bin`
- `prices.parquet` with `output_dir=./data` -> `./data/prices/close_DOUBLE.bin`

## CLI usage

```text
cargo run -- <input.parquet> [output_dir]
```

Examples:

```text
cargo run -- ./data/prices.parquet
cargo run -- ./data/prices.parquet ./out
```

If `output_dir` is omitted, output is written under the current directory.

## Binary layout

The encoder writes each column value as follows:

- numeric values are written in big-endian byte order
- `Str`, `Bytes`, and `Decimal` values are written as a big-endian `u64` length prefix followed by the raw bytes
- `Null` values are mapped to type-specific sentinels
- nested values such as groups, lists, and maps are rejected

Null sentinels:

- `BOOLEAN` -> `0`
- `INT32` -> `i32::MIN`
- `INT64` -> `i64::MIN`
- `INT96` -> 12 zero bytes
- `FLOAT` -> `NaN`
- `DOUBLE` -> `NaN`
- `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` -> zero-length payload

## Error handling

The library returns `dftobin::Result<T>` and reports:

- I/O errors
- Parquet parse errors
- missing columns
- invalid or nested column types
- schema errors
- empty files

## Project layout

- `src/main.rs` - CLI entry point
- `src/lib.rs` - converter implementation and tests

## Development

Build:

```text
cargo build
```

Run:

```text
cargo run -- <input.parquet> [output_dir]
```

Test:

```text
cargo test
```


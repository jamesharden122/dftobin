use parquet::{
    basic::Type as PhysicalType,
    data_type::ByteArray,
    errors::ParquetError,
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
    },
    record::{Field, Row},
};
use std::{
    fmt,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::PathBuf,
};

/// Error type shared by the library API and the CLI.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Parquet(ParquetError),

    MissingColumn(String),

    InvalidColumnType {
        column: String,
        expected: &'static str,
        found: String,
    },

    InvalidValue {
        column: String,
        message: String,
    },

    Schema(String),

    EmptyFile,
}

/// Crate-local result alias so callers can use one error surface.
pub type Result<T> = std::result::Result<T, Error>;

/// In-memory representation of a Parquet file ready to be written as column bins.
///
/// The converter currently materializes rows up front. That keeps the per-column
/// write pass simple, because each output file can be produced by scanning the
/// same row buffer for one schema column at a time.
#[derive(Debug)]
pub struct DftoBin {
    pub file: File,
    pub file_name: String,
    pub metadata: ParquetMetaData,
    pub data: Vec<Row>,
}

impl DftoBin {
    /// Open a Parquet file, capture its metadata, and load all rows into memory.
    pub fn new(file_name: &str) -> Result<Self> {
        let file = File::open(file_name)?;
        let reader = SerializedFileReader::new(file.try_clone()?)?;

        let metadata = reader.metadata().to_owned();
        let iter = reader.get_row_iter(None)?;
        let mut data: Vec<Row> = Vec::new();

        // Row materialization makes later column-wise output deterministic and
        // avoids reopening the Parquet reader once per column.
        for row_result in iter {
            data.push(row_result?);
        }

        let df_to_bin = Self {
            file,
            file_name: file_name.to_owned(),
            metadata,
            data,
        };
        Ok(df_to_bin)
    }

    /// Write output under the current directory.
    pub fn to_bin(&self) -> Result<()> {
        self.to_bin_at(".")
    }

    /// Write one `.bin` file per Parquet schema column under `output_root/<input_stem>/`.
    pub fn to_bin_at(&self, output_root: impl Into<PathBuf>) -> Result<()> {
        let schema = self.metadata.file_metadata().schema_descr();
        let output_leaf: PathBuf = PathBuf::from(&self.file_name)
            .file_stem()
            .ok_or_else(|| Error::Schema(format!("invalid file name: {}", self.file_name)))?
            .into();
        let output_dir = output_root.into().join(output_leaf);
        fs::create_dir_all(&output_dir)?;

        for i in 0..schema.num_columns() {
            let col = schema.column(i);
            let column_name = col.name();

            // The physical type is embedded in the file name so downstream
            // readers can choose the correct fixed-width decoder.
            let file = File::create(
                output_dir.join(format!("{column_name}_{}.bin", col.physical_type())),
            )?;
            let mut writer = BufWriter::new(file);

            for row in &self.data {
                // Parquet row fields should match the schema order; a mismatch
                // means the input cannot be safely decoded by ordinal position.
                let Some((_name, field)) = row.get_column_iter().nth(i) else {
                    return Err(Error::Schema(format!(
                        "row has fewer fields than schema; missing column index {i} ({column_name})"
                    )));
                };

                write_field_be(&mut writer, column_name, col.physical_type(), field)?;
            }

            writer.flush()?;
        }

        Ok(())
    }
}

fn write_field_be<W: Write>(
    writer: &mut W,
    column: &str,
    physical_type: PhysicalType,
    field: &Field,
) -> Result<()> {
    // Fixed-width values are written in big-endian order to make the binary
    // format explicit and stable across platforms.
    match field {
        Field::Null => write_null_be(writer, physical_type)?,
        Field::Bool(value) => writer.write_all(&[u8::from(*value)])?,
        Field::Byte(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Short(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Int(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Long(value) => writer.write_all(&value.to_be_bytes())?,
        Field::UByte(value) => writer.write_all(&value.to_be_bytes())?,
        Field::UShort(value) => writer.write_all(&value.to_be_bytes())?,
        Field::UInt(value) => writer.write_all(&value.to_be_bytes())?,
        Field::ULong(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Float16(value) => writer.write_all(&value.to_bits().to_be_bytes())?,
        Field::Float(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Double(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Decimal(value) => write_len_prefixed_bytes(writer, value.data())?,
        Field::Str(value) => write_len_prefixed_bytes(writer, value.as_bytes())?,
        Field::Bytes(value) => write_byte_array(writer, value)?,
        Field::Date(value) => writer.write_all(&value.to_be_bytes())?,
        Field::TimeMillis(value) => writer.write_all(&value.to_be_bytes())?,
        Field::TimeMicros(value) => writer.write_all(&value.to_be_bytes())?,
        Field::TimestampMillis(value) => writer.write_all(&value.to_be_bytes())?,
        Field::TimestampMicros(value) => writer.write_all(&value.to_be_bytes())?,
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            // Nested Parquet values need an explicit schema-aware layout, so the
            // flat binary writer rejects them instead of guessing.
            return Err(Error::InvalidColumnType {
                column: column.to_owned(),
                expected: "flat primitive parquet column",
                found: format!("{field:?}"),
            });
        }
    }

    Ok(())
}

fn write_null_be<W: Write>(writer: &mut W, physical_type: PhysicalType) -> Result<()> {
    // Nulls are represented with type-specific sentinels so column lengths stay
    // aligned with row counts even when source data has missing values.
    match physical_type {
        PhysicalType::BOOLEAN => writer.write_all(&[0])?,
        PhysicalType::INT32 => writer.write_all(&i32::MIN.to_be_bytes())?,
        PhysicalType::INT64 => writer.write_all(&i64::MIN.to_be_bytes())?,
        PhysicalType::INT96 => writer.write_all(&[0; 12])?,
        PhysicalType::FLOAT => writer.write_all(&f32::NAN.to_be_bytes())?,
        PhysicalType::DOUBLE => writer.write_all(&f64::NAN.to_be_bytes())?,
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            write_len_prefixed_bytes(writer, &[])?
        }
    }

    Ok(())
}

fn write_byte_array<W: Write>(writer: &mut W, value: &ByteArray) -> Result<()> {
    write_len_prefixed_bytes(writer, value.data())
}

fn write_len_prefixed_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    // Variable-width values use an explicit length prefix so adjacent rows can
    // be decoded from a single column file without a sidecar offset table.
    writer.write_all(&(bytes.len() as u64).to_be_bytes())?;
    writer.write_all(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_numeric_values_in_big_endian_order() {
        let mut out = Vec::new();

        write_field_be(
            &mut out,
            "signed",
            PhysicalType::INT32,
            &Field::Int(0x0102_0304),
        )
        .unwrap();
        write_field_be(
            &mut out,
            "long",
            PhysicalType::INT64,
            &Field::Long(0x0102_0304_0506_0708),
        )
        .unwrap();
        write_field_be(&mut out, "float", PhysicalType::FLOAT, &Field::Float(1.0)).unwrap();

        assert_eq!(
            out,
            [
                0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x3f, 0x80,
                0x00, 0x00,
            ]
        );
    }

    #[test]
    fn writes_variable_width_values_with_big_endian_length_prefix() {
        let mut out = Vec::new();

        write_field_be(
            &mut out,
            "name",
            PhysicalType::BYTE_ARRAY,
            &Field::Str("ab".to_owned()),
        )
        .unwrap();

        assert_eq!(out, [0, 0, 0, 0, 0, 0, 0, 2, b'a', b'b']);
    }

    #[test]
    fn writes_null_double_as_big_endian_nan() {
        let mut out = Vec::new();

        write_field_be(&mut out, "ret", PhysicalType::DOUBLE, &Field::Null).unwrap();
        let value = f64::from_be_bytes(out.try_into().unwrap());

        assert!(value.is_nan());
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<ParquetError> for Error {
    fn from(err: ParquetError) -> Self {
        Self::Parquet(err)
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Parquet(err) => write!(f, "Parquet error: {err}"),
            Self::MissingColumn(column) => {
                write!(f, "missing column: {column}")
            }
            Self::InvalidColumnType {
                column,
                expected,
                found,
            } => {
                write!(
                    f,
                    "invalid column type for `{column}`: expected {expected}, found {found}"
                )
            }
            Self::InvalidValue { column, message } => {
                write!(f, "invalid value in column `{column}`: {message}")
            }

            Self::Schema(message) => {
                write!(f, "schema error: {message}")
            }

            Self::EmptyFile => {
                write!(f, "empty parquet file")
            }
        }
    }
}

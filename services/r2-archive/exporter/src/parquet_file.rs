use std::io::{self, Write};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, ListArray, RecordBatch, StructArray, UInt64Array};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Field};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::event::EventType;

pub const PARQUET_COMPRESSION_LEVEL: i32 = 9;
pub const PARQUET_DICTIONARY_PAGE_SIZE_LIMIT: usize = 8 * 1024 * 1024;
pub const PARQUET_MAX_ROW_GROUP_ROWS: usize = 1_048_576;

#[derive(Debug, Eq, PartialEq)]
pub struct FileStats {
    pub row_count: u64,
    pub byte_size: u64,
    pub sha256: String,
    pub min_sequence: Option<u64>,
    pub max_sequence: Option<u64>,
}

pub struct StagedArtifact {
    file: NamedTempFile,
    pub stats: FileStats,
}

impl StagedArtifact {
    pub fn into_parts(self) -> (NamedTempFile, FileStats) {
        (self.file, self.stats)
    }

    #[cfg(test)]
    pub(crate) fn from_parts(file: NamedTempFile, stats: FileStats) -> Self {
        Self { file, stats }
    }
}

pub fn writer_properties(event: EventType) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder()
        // parquet-rs uses writer version 2.0 to select DataPageV2.
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(PARQUET_MAX_ROW_GROUP_ROWS))
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            PARQUET_COMPRESSION_LEVEL,
        )?))
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_dictionary_page_size_limit(PARQUET_DICTIONARY_PAGE_SIZE_LIMIT);
    for column in event.dictionary_columns() {
        builder = builder.set_column_dictionary_enabled(ColumnPath::from(*column), true);
    }
    Ok(builder.build())
}

pub fn cast_batch(event: EventType, batch: &RecordBatch) -> Result<RecordBatch> {
    let target_schema = event.schema();
    ensure!(
        batch.num_columns() == target_schema.fields().len(),
        "{} source has {} columns, expected {}",
        event,
        batch.num_columns(),
        target_schema.fields().len()
    );

    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, target_field) in target_schema.fields().iter().enumerate() {
        let source_field = batch.schema().field(index).clone();
        ensure!(
            source_field.name() == target_field.name(),
            "{} source column {} is {:?}, expected {:?}",
            event,
            index,
            source_field.name(),
            target_field.name()
        );
        let source = batch.column(index);
        let converted = if source.data_type() == target_field.data_type() {
            Arc::clone(source)
        } else {
            cast(source, target_field.data_type()).with_context(|| {
                format!(
                    "cast {}.{} from {:?} to {:?}",
                    event,
                    target_field.name(),
                    source.data_type(),
                    target_field.data_type()
                )
            })?
        };
        validate_nullability(converted.as_ref(), target_field, target_field.name())?;
        columns.push(converted);
    }
    RecordBatch::try_new(target_schema, columns).context("build typed export batch")
}

fn validate_nullability(array: &dyn Array, field: &Field, path: &str) -> Result<()> {
    if !field.is_nullable() && array.null_count() != 0 {
        bail!("non-null export column {path} contains nulls");
    }
    match field.data_type() {
        DataType::List(item_field) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .with_context(|| format!("{path} is not a ListArray"))?;
            validate_nullability(
                list.values().as_ref(),
                item_field,
                &format!("{path}.list.element"),
            )?;
        }
        DataType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .with_context(|| format!("{path} is not a StructArray"))?;
            for (index, child_field) in fields.iter().enumerate() {
                validate_nullability(
                    structure.column(index).as_ref(),
                    child_field,
                    &format!("{path}.{}", child_field.name()),
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn write_arrow_batches<I>(
    event: EventType,
    staged: NamedTempFile,
    batches: I,
) -> Result<StagedArtifact>
where
    I: IntoIterator<Item = std::result::Result<RecordBatch, ArrowError>>,
{
    let schema = event.schema();
    let output = HashingWriter::new(staged);
    let mut writer =
        ArrowWriter::try_new(output, Arc::clone(&schema), Some(writer_properties(event)?))
            .with_context(|| format!("create {event} Parquet writer"))?;
    let mut row_count = 0_u64;
    let mut min_sequence = None;
    let mut max_sequence = None;

    for batch in batches {
        let source = batch.with_context(|| format!("decode {event} Arrow batch"))?;
        let batch = cast_batch(event, &source)?;
        update_sequence_bounds(&batch, &mut row_count, &mut min_sequence, &mut max_sequence)?;
        writer
            .write(&batch)
            .with_context(|| format!("write {event} Parquet batch"))?;
    }

    let mut output = writer
        .into_inner()
        .with_context(|| format!("finish {event} Parquet file"))?;
    output
        .flush()
        .with_context(|| format!("flush {event} Parquet file"))?;
    let byte_size = output.byte_size;
    let sha256 = format!("{:x}", output.hasher.finalize());

    Ok(StagedArtifact {
        file: output.inner,
        stats: FileStats {
            row_count,
            byte_size,
            sha256,
            min_sequence,
            max_sequence,
        },
    })
}

fn update_sequence_bounds(
    batch: &RecordBatch,
    row_count: &mut u64,
    minimum: &mut Option<u64>,
    maximum: &mut Option<u64>,
) -> Result<()> {
    let sequence = batch
        .column_by_name("sequence")
        .context("typed export batch has no sequence")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("typed export sequence is not UInt64")?;
    ensure!(sequence.null_count() == 0, "sequence contains nulls");
    *row_count = row_count
        .checked_add(sequence.len() as u64)
        .context("export row count overflow")?;
    for &value in sequence.values() {
        *minimum = Some(minimum.map_or(value, |current| current.min(value)));
        *maximum = Some(maximum.map_or(value, |current| current.max(value)));
    }
    Ok(())
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    byte_size: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            byte_size: 0,
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(data)?;
        self.hasher.update(&data[..written]);
        self.byte_size = self
            .byte_size
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("archive byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.inner.write_all(data)?;
        self.hasher.update(data);
        self.byte_size = self
            .byte_size
            .checked_add(data.len() as u64)
            .ok_or_else(|| io::Error::other("archive byte count overflow"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow_array::builder::{
        Decimal128Builder, FixedSizeBinaryBuilder, ListBuilder, StructBuilder,
    };
    use arrow_array::{
        ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array,
    };
    use arrow_schema::{Fields, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::basic::{PageType, Type as PhysicalType};
    use tempfile::TempDir;

    use super::*;

    fn fixed32(value: u8, rows: usize) -> ArrayRef {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 32);
        for _ in 0..rows {
            builder.append_value([value; 32]).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn common_source(rows: usize) -> Vec<ArrayRef> {
        vec![
            Arc::new(
                TimestampNanosecondArray::from_iter_values(0..rows as i64).with_timezone("UTC"),
            ),
            Arc::new(UInt64Array::from_iter_values(10..10 + rows as u64)),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(0..rows as i64).with_timezone("UTC"),
            ),
            fixed32(b'm', rows),
        ]
    }

    fn source_book_batch() -> RecordBatch {
        let source_level_fields: Fields = vec![
            Field::new("price", DataType::Decimal128(9, 4), false),
            Field::new("size", DataType::Decimal128(18, 6), false),
        ]
        .into();
        let level_builder = || {
            StructBuilder::new(
                source_level_fields.clone(),
                vec![
                    Box::new(
                        Decimal128Builder::with_capacity(2)
                            .with_data_type(DataType::Decimal128(9, 4)),
                    ),
                    Box::new(
                        Decimal128Builder::with_capacity(2)
                            .with_data_type(DataType::Decimal128(18, 6)),
                    ),
                ],
            )
        };
        let mut bids = ListBuilder::new(level_builder());
        bids.values()
            .field_builder::<Decimal128Builder>(0)
            .unwrap()
            .append_value(4_800);
        bids.values()
            .field_builder::<Decimal128Builder>(1)
            .unwrap()
            .append_value(30_000_000);
        bids.values().append(true);
        bids.append(true);
        let mut asks = ListBuilder::new(level_builder());
        asks.append(true);

        let mut fields = vec![
            Field::new(
                "timestamp_received",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("sequence", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("market", DataType::FixedSizeBinary(32), false),
            Field::new("asset_id", DataType::FixedSizeBinary(32), false),
            Field::new("bids", bids.finish().data_type().clone(), false),
            Field::new("asks", asks.finish().data_type().clone(), false),
        ];
        let mut columns = common_source(1);
        columns.push(fixed32(b'a', 1));
        // Rebuild the list arrays after using their data types above.
        let mut bids = ListBuilder::new(level_builder());
        bids.values()
            .field_builder::<Decimal128Builder>(0)
            .unwrap()
            .append_value(4_800);
        bids.values()
            .field_builder::<Decimal128Builder>(1)
            .unwrap()
            .append_value(30_000_000);
        bids.values().append(true);
        bids.append(true);
        let mut asks = ListBuilder::new(level_builder());
        asks.append(true);
        columns.push(Arc::new(bids.finish()));
        columns.push(Arc::new(asks.finish()));
        RecordBatch::try_new(Arc::new(Schema::new(std::mem::take(&mut fields))), columns).unwrap()
    }

    fn price_change_batch(rows: usize) -> RecordBatch {
        let mut columns = common_source(rows);
        columns.extend([
            fixed32(b'a', rows),
            Arc::new(
                Decimal32Array::from_iter_values(std::iter::repeat_n(5_000, rows))
                    .with_precision_and_scale(9, 4)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal64Array::from_iter_values(std::iter::repeat_n(2_000_000, rows))
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|index| if index % 2 == 0 { "BUY" } else { "SELL" }),
            )),
            Arc::new(
                Decimal32Array::from_iter_values(std::iter::repeat_n(4_900, rows))
                    .with_precision_and_scale(9, 4)
                    .unwrap(),
            ),
            Arc::new(
                Decimal32Array::from_iter_values(std::iter::repeat_n(5_100, rows))
                    .with_precision_and_scale(9, 4)
                    .unwrap(),
            ),
        ]);
        RecordBatch::try_new(EventType::PriceChange.schema(), columns).unwrap()
    }

    fn trade_batch(rows: usize) -> RecordBatch {
        let mut columns = common_source(rows);
        columns.extend([
            fixed32(b'a', rows),
            Arc::new(
                Decimal32Array::from_iter_values(std::iter::repeat_n(5_000, rows))
                    .with_precision_and_scale(9, 4)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal64Array::from_iter_values(std::iter::repeat_n(2_000_000, rows))
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|index| if index % 2 == 0 { "BUY" } else { "SELL" }),
            )),
            Arc::new(UInt16Array::from_iter_values(std::iter::repeat_n(0, rows))),
            fixed32(b'x', rows),
        ]);
        RecordBatch::try_new(EventType::LastTradePrice.schema(), columns).unwrap()
    }

    #[test]
    fn nested_decimal_cast_is_narrow_and_lossless() {
        let typed = cast_batch(EventType::Book, &source_book_batch()).unwrap();
        assert_eq!(typed.schema(), EventType::Book.schema());
        assert_eq!(
            typed.column_by_name("bids").unwrap().data_type(),
            EventType::Book
                .schema()
                .field_with_name("bids")
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn empty_file_keeps_the_static_schema() {
        let directory = TempDir::new().unwrap();
        let staged = NamedTempFile::new_in(directory.path()).unwrap();
        let artifact = write_arrow_batches(
            EventType::MarketResolved,
            staged,
            Vec::<std::result::Result<RecordBatch, ArrowError>>::new(),
        )
        .unwrap();
        assert_eq!(artifact.stats.row_count, 0);
        assert_eq!(artifact.stats.min_sequence, None);
        assert_eq!(artifact.stats.max_sequence, None);
        assert!(artifact.stats.byte_size > 0);

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(artifact.file.path()).unwrap())
                .unwrap();
        assert_eq!(builder.schema(), &EventType::MarketResolved.schema());
        assert_eq!(builder.metadata().file_metadata().num_rows(), 0);
    }

    #[test]
    fn writer_policy_is_plain_except_for_selected_dictionaries() {
        let plain = writer_properties(EventType::PriceChange).unwrap();
        assert_eq!(plain.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(
            plain.max_row_group_row_count(),
            Some(PARQUET_MAX_ROW_GROUP_ROWS)
        );
        assert_eq!(
            plain.compression(&ColumnPath::from("sequence")),
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
        );
        assert_eq!(
            plain.encoding(&ColumnPath::from("sequence")),
            Some(Encoding::PLAIN)
        );
        assert!(!plain.dictionary_enabled(&ColumnPath::from("sequence")));
        assert!(plain.dictionary_enabled(&ColumnPath::from("side")));
        assert_eq!(
            plain.dictionary_page_size_limit(),
            PARQUET_DICTIONARY_PAGE_SIZE_LIMIT
        );

        let trade = writer_properties(EventType::LastTradePrice).unwrap();
        for column in ["price", "side", "fee_rate_bps"] {
            assert!(trade.dictionary_enabled(&ColumnPath::from(column)));
        }
        for column in ["sequence", "market", "asset_id", "size", "transaction_hash"] {
            assert!(!trade.dictionary_enabled(&ColumnPath::from(column)));
        }
    }

    #[test]
    fn file_metadata_uses_the_selected_value_encodings_and_zstd() {
        let directory = TempDir::new().unwrap();
        let staged = NamedTempFile::new_in(directory.path()).unwrap();
        let artifact = write_arrow_batches(
            EventType::PriceChange,
            staged,
            vec![Ok(price_change_batch(100))],
        )
        .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            File::open(artifact.file.path()).unwrap(),
            ArrowReaderOptions::new().with_encoding_stats_as_mask(false),
        )
        .unwrap();
        let row_group = builder.metadata().row_group(0);
        let metadata = (0..row_group.num_columns())
            .map(|index| {
                let column = row_group.column(index);
                (
                    column.column_path().string(),
                    (column.encodings().collect::<Vec<_>>(), column.compression()),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        for column in [
            "timestamp_received",
            "sequence",
            "timestamp",
            "market",
            "asset_id",
            "price",
            "size",
            "best_bid",
            "best_ask",
        ] {
            assert!(metadata[column].0.contains(&Encoding::PLAIN));
            assert!(!metadata[column].0.contains(&Encoding::RLE_DICTIONARY));
            assert!(matches!(metadata[column].1, Compression::ZSTD(_)));
        }
        assert!(metadata["side"].0.contains(&Encoding::RLE_DICTIONARY));
        assert!(matches!(metadata["side"].1, Compression::ZSTD(_)));
        for index in 0..row_group.num_columns() {
            let stats = row_group.column(index).page_encoding_stats().unwrap();
            assert!(
                stats
                    .iter()
                    .filter(|stat| stat.page_type != PageType::DICTIONARY_PAGE)
                    .all(|stat| stat.page_type == PageType::DATA_PAGE_V2),
                "unexpected page version for {}: {stats:?}",
                row_group.column(index).column_path()
            );
        }

        let staged = NamedTempFile::new_in(directory.path()).unwrap();
        let artifact = write_arrow_batches(
            EventType::LastTradePrice,
            staged,
            vec![Ok(trade_batch(100))],
        )
        .unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(artifact.file.path()).unwrap())
                .unwrap();
        let row_group = builder.metadata().row_group(0);
        let encodings = (0..row_group.num_columns())
            .map(|index| {
                let column = row_group.column(index);
                (
                    column.column_path().string(),
                    column.encodings().collect::<Vec<_>>(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        for column in ["price", "side", "fee_rate_bps"] {
            assert!(encodings[column].contains(&Encoding::RLE_DICTIONARY));
        }
        for column in ["sequence", "market", "asset_id", "size", "transaction_hash"] {
            assert!(!encodings[column].contains(&Encoding::RLE_DICTIONARY));
            assert!(encodings[column].contains(&Encoding::PLAIN));
        }
    }

    #[test]
    fn sequence_stats_and_physical_decimal_widths_are_recorded() {
        let directory = TempDir::new().unwrap();
        let staged = NamedTempFile::new_in(directory.path()).unwrap();
        let artifact =
            write_arrow_batches(EventType::Book, staged, vec![Ok(source_book_batch())]).unwrap();
        assert_eq!(artifact.stats.row_count, 1);
        assert_eq!(artifact.stats.min_sequence, Some(10));
        assert_eq!(artifact.stats.max_sequence, Some(10));
        assert_eq!(artifact.stats.sha256.len(), 64);
        assert_eq!(
            artifact.stats.byte_size,
            artifact.file.as_file().metadata().unwrap().len()
        );

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(artifact.file.path()).unwrap())
                .unwrap();
        let descriptor = builder.parquet_schema();
        let physical = (0..descriptor.num_columns())
            .map(|index| {
                let column = descriptor.column(index);
                (column.path().string(), column.physical_type())
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            physical.get("bids.list.element.price"),
            Some(&PhysicalType::INT32),
            "physical columns: {physical:?}"
        );
        assert_eq!(
            physical.get("bids.list.element.size"),
            Some(&PhysicalType::INT64),
            "physical columns: {physical:?}"
        );
    }

    #[test]
    fn nullable_and_column_name_contracts_are_enforced() {
        let mut columns = common_source(1);
        columns.push(fixed32(b'a', 1));
        columns.push(Arc::new(
            Decimal128Array::from(vec![Some(5_000)])
                .with_precision_and_scale(9, 4)
                .unwrap(),
        ));
        columns.push(Arc::new(
            Decimal128Array::from(vec![Some(2_000_000)])
                .with_precision_and_scale(18, 6)
                .unwrap(),
        ));
        columns.push(Arc::new(StringArray::from(vec![Option::<&str>::None])));
        columns.push(Arc::new(
            Decimal128Array::from(vec![None])
                .with_precision_and_scale(9, 4)
                .unwrap(),
        ));
        columns.push(Arc::new(
            Decimal128Array::from(vec![None])
                .with_precision_and_scale(9, 4)
                .unwrap(),
        ));
        let schema = EventType::PriceChange.schema();
        let source_fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let data_type = columns[index].data_type().clone();
                Field::new(field.name(), data_type, true)
            })
            .collect::<Vec<_>>();
        let source = RecordBatch::try_new(Arc::new(Schema::new(source_fields)), columns).unwrap();
        assert!(cast_batch(EventType::PriceChange, &source).is_err());
    }
}

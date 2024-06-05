use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field};
use bytes::Bytes;
use snafu::{ensure, ResultExt};

use crate::error::{IoSnafu, Result};
use crate::error::{MismatchedSchemaSnafu, UnexpectedSnafu, UnsupportedTypeVariantSnafu};
use crate::proto::stream::Kind;
use crate::proto::{ColumnEncoding, StripeFooter};
use crate::reader::decode::boolean_rle::BooleanIter;
use crate::reader::ChunkReader;
use crate::schema::DataType;
use crate::stripe::Stripe;

#[derive(Clone, Debug)]
pub struct Column {
    number_of_rows: u64,
    footer: Arc<StripeFooter>,
    name: String,
    /// ORC data type
    data_type: DataType,
    /// Arrow field the column will be decoded into
    field: Arc<Field>,
}

impl Column {
    pub fn new(
        name: &str,
        data_type: &DataType,
        footer: &Arc<StripeFooter>,
        number_of_rows: u64,
        field: Arc<Field>,
    ) -> Self {
        Self {
            number_of_rows,
            footer: footer.clone(),
            data_type: data_type.clone(),
            name: name.to_string(),
            field,
        }
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.data_type.column_index();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.data_type.column_index();
        self.footer.columns[column].clone()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_id(&self) -> u32 {
        self.data_type.column_index() as u32
    }

    pub fn field(&self) -> Arc<Field> {
        self.field.clone()
    }

    pub fn children(&self) -> Result<Vec<Column>> {
        let field_type = self.field.data_type();
        match &self.data_type {
            DataType::Boolean { .. }
            | DataType::Byte { .. }
            | DataType::Short { .. }
            | DataType::Int { .. }
            | DataType::Long { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::String { .. }
            | DataType::Varchar { .. }
            | DataType::Char { .. }
            | DataType::Binary { .. }
            | DataType::Decimal { .. }
            | DataType::Timestamp { .. }
            | DataType::TimestampWithLocalTimezone { .. }
            | DataType::Date { .. } => Ok(vec![]),
            DataType::Struct { children, .. } => match field_type {
                ArrowDataType::Struct(fields) => Ok(children
                    .iter()
                    .zip(fields.iter().cloned())
                    .map(|(col, field)| Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: col.name().to_string(),
                        data_type: col.data_type().clone(),
                        field,
                    })
                    .collect()),
                _ => MismatchedSchemaSnafu {
                    orc_type: self.data_type.clone(),
                    arrow_type: field_type.clone(),
                }
                .fail(),
            },
            DataType::List { child, .. } => match field_type {
                ArrowDataType::List(field) => Ok(vec![Column {
                    number_of_rows: self.number_of_rows,
                    footer: self.footer.clone(),
                    name: "item".to_string(),
                    data_type: *child.clone(),
                    field: field.clone(),
                }]),
                // TODO: add support for ArrowDataType::LargeList
                _ => MismatchedSchemaSnafu {
                    orc_type: self.data_type.clone(),
                    arrow_type: field_type.clone(),
                }
                .fail(),
            },
            DataType::Map { key, value, .. } => {
                let ArrowDataType::Map(entries, sorted) = field_type else {
                    MismatchedSchemaSnafu {
                        orc_type: self.data_type.clone(),
                        arrow_type: field_type.clone(),
                    }
                    .fail()?
                };
                ensure!(!sorted, UnsupportedTypeVariantSnafu { msg: "Sorted map" });
                let ArrowDataType::Struct(entries) = entries.data_type() else {
                    UnexpectedSnafu {
                        msg: "arrow Map with non-Struct entry type".to_owned(),
                    }
                    .fail()?
                };
                ensure!(
                    entries.len() == 2,
                    UnexpectedSnafu {
                        msg: format!(
                            "arrow Map with {} columns per entry (expected 2)",
                            entries.len()
                        )
                    }
                );
                let key_field = entries[0].clone();
                let value_field = entries[1].clone();
                Ok(vec![
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "key".to_string(),
                        data_type: *key.clone(),
                        field: key_field,
                    },
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "value".to_string(),
                        data_type: *value.clone(),
                        field: value_field,
                    },
                ])
            }
            DataType::Union { variants, .. } => match field_type {
                ArrowDataType::Union(fields, _) => {
                    // TODO: might need corrections
                    Ok(variants
                        .iter()
                        .enumerate()
                        .zip(fields.iter())
                        .map(|((index, data_type), (index2, field))| {
                            ensure!(
                                index == index2 as usize,
                                MismatchedSchemaSnafu {
                                    orc_type: self.data_type.clone(),
                                    arrow_type: field_type.clone(),
                                }
                            );
                            Ok(Column {
                                number_of_rows: self.number_of_rows,
                                footer: self.footer.clone(),
                                name: format!("{index}"),
                                data_type: data_type.clone(),
                                field: field.clone(),
                            })
                        })
                        .collect::<Result<_>>()?)
                }
                _ => MismatchedSchemaSnafu {
                    orc_type: self.data_type.clone(),
                    arrow_type: field_type.clone(),
                }
                .fail(),
            },
        }
    }

    pub fn read_stream<R: ChunkReader>(reader: &mut R, start: u64, length: u64) -> Result<Bytes> {
        reader.get_bytes(start, length).context(IoSnafu)
    }

    #[cfg(feature = "async")]
    pub async fn read_stream_async<R: crate::reader::AsyncChunkReader>(
        reader: &mut R,
        start: u64,
        length: u64,
    ) -> Result<Bytes> {
        reader.get_bytes(start, length).await.context(IoSnafu)
    }
}

/// Prefetch present stream for entire column in stripe.
///
/// Makes subsequent operations easier to handle.
pub fn get_present_vec(column: &Column, stripe: &Stripe) -> Result<Option<Vec<bool>>> {
    stripe
        .stream_map()
        .get_opt(column, Kind::Present)
        .map(|reader| BooleanIter::new(reader).collect::<Result<Vec<_>>>())
        .transpose()
}

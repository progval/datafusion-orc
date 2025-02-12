use std::{collections::HashMap, io::Read, sync::Arc};

use bytes::Bytes;
use prost::Message;
use snafu::{OptionExt, ResultExt};

use crate::{
    arrow_reader::column::Column,
    error::{self, IoSnafu},
    error::{InvalidColumnSnafu, Result},
    proto::{self, stream::Kind, StripeFooter},
    reader::{
        decompress::{Compression, Decompressor},
        metadata::FileMetadata,
        ChunkReader,
    },
    schema::RootDataType,
    statistics::ColumnStatistics,
};

/// Stripe metadata parsed from the file tail metadata sections.
/// Does not contain the actual stripe bytes, as those are decoded
/// when they are required.
#[derive(Debug, Clone)]
pub struct StripeMetadata {
    /// Statistics of columns across this specific stripe
    column_statistics: Vec<ColumnStatistics>,
    /// Byte offset of start of stripe from start of file
    offset: u64,
    /// Byte length of index section
    index_length: u64,
    /// Byte length of data section
    data_length: u64,
    /// Byte length of footer section
    footer_length: u64,
    /// Number of rows in the stripe
    number_of_rows: u64,
}

impl StripeMetadata {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn index_length(&self) -> u64 {
        self.index_length
    }

    pub fn data_length(&self) -> u64 {
        self.data_length
    }

    pub fn footer_length(&self) -> u64 {
        self.footer_length
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn column_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn footer_offset(&self) -> u64 {
        self.offset + self.index_length + self.data_length
    }
}

impl TryFrom<(&proto::StripeInformation, &proto::StripeStatistics)> for StripeMetadata {
    type Error = error::OrcError;

    fn try_from(value: (&proto::StripeInformation, &proto::StripeStatistics)) -> Result<Self> {
        let column_statistics = value
            .1
            .col_stats
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            column_statistics,
            offset: value.0.offset(),
            index_length: value.0.index_length(),
            data_length: value.0.data_length(),
            footer_length: value.0.footer_length(),
            number_of_rows: value.0.number_of_rows(),
        })
    }
}

#[derive(Debug)]
pub struct Stripe {
    pub(crate) footer: Arc<StripeFooter>,
    pub(crate) columns: Vec<Column>,
    pub(crate) stripe_offset: usize,
    /// <(ColumnId, Kind), Bytes>
    pub(crate) stream_map: Arc<StreamMap>,
    pub(crate) number_of_rows: usize,
}

impl Stripe {
    pub fn new<R: ChunkReader>(
        reader: &mut R,
        file_metadata: &Arc<FileMetadata>,
        projected_data_type: &RootDataType,
        stripe: usize,
        info: &StripeMetadata,
    ) -> Result<Self> {
        let compression = file_metadata.compression();

        let footer = reader
            .get_bytes(info.footer_offset(), info.footer_length())
            .context(IoSnafu)?;
        let footer = Arc::new(deserialize_stripe_footer(&footer, compression)?);

        //TODO(weny): add tz
        let columns = projected_data_type
            .children()
            .iter()
            .map(|col| Column::new(col.name(), col.data_type(), &footer, info.number_of_rows()))
            .collect();

        let mut stream_map = HashMap::new();
        let mut stream_offset = info.offset();
        for stream in &footer.streams {
            let length = stream.length();
            let column_id = stream.column();
            let kind = stream.kind();
            let data = Column::read_stream(reader, stream_offset, length)?;

            // TODO(weny): filter out unused streams.
            stream_map.insert((column_id, kind), data);

            stream_offset += length;
        }

        Ok(Self {
            footer,
            columns,
            stripe_offset: stripe,
            stream_map: Arc::new(StreamMap {
                inner: stream_map,
                compression,
            }),
            number_of_rows: info.number_of_rows() as usize,
        })
    }

    pub fn footer(&self) -> &Arc<StripeFooter> {
        &self.footer
    }

    pub fn stripe_offset(&self) -> usize {
        self.stripe_offset
    }
}

#[derive(Debug)]
pub struct StreamMap {
    pub inner: HashMap<(u32, Kind), Bytes>,
    pub compression: Option<Compression>,
}

impl StreamMap {
    pub fn get(&self, column: &Column, kind: Kind) -> Result<Decompressor> {
        self.get_opt(column, kind).context(InvalidColumnSnafu {
            name: column.name(),
        })
    }

    pub fn get_opt(&self, column: &Column, kind: Kind) -> Option<Decompressor> {
        let column_id = column.column_id();

        self.inner
            .get(&(column_id, kind))
            .cloned()
            .map(|data| Decompressor::new(data, self.compression, vec![]))
    }
}

pub(crate) fn deserialize_stripe_footer(
    bytes: &[u8],
    compression: Option<Compression>,
) -> Result<StripeFooter> {
    let mut buffer = vec![];
    // TODO: refactor to not need Bytes::copy_from_slice
    Decompressor::new(Bytes::copy_from_slice(bytes), compression, vec![])
        .read_to_end(&mut buffer)
        .context(error::IoSnafu)?;
    StripeFooter::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, FieldRef};
use snafu::ResultExt;

use crate::arrow_reader::column::{get_present_vec, Column};
use crate::arrow_reader::decoder::{
    array_decoder_factory, derive_present_vec, populate_lengths_with_nulls, ArrayBatchDecoder,
};
use crate::arrow_reader::Stripe;
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;

use crate::error::{ArrowSnafu, Result};

pub struct ListArrayDecoder {
    inner: Box<dyn ArrayBatchDecoder>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    field: FieldRef,
}

impl ListArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let child = &column.children()[0];
        let inner = array_decoder_factory(child, stripe)?;

        let reader = stripe.stream_map.get(column, Kind::Length)?;
        let lengths = get_rle_reader(column, reader)?;

        let field = Arc::new(Field::from(child));

        Ok(Self {
            inner,
            present,
            lengths,
            field,
        })
    }
}

impl ArrayBatchDecoder for ListArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

        // How many lengths we need to fetch
        let elements_to_fetch = if let Some(present) = &present {
            present.iter().filter(|&&is_present| is_present).count()
        } else {
            batch_size
        };
        let lengths = self
            .lengths
            .by_ref()
            .take(elements_to_fetch)
            .collect::<Result<Vec<_>>>()?;
        debug_assert_eq!(
            lengths.len(),
            elements_to_fetch,
            "less lengths than expected in ListArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch child array as one Array with total_length elements
        let child_array = self.inner.next_batch(total_length as usize, None)?;
        let lengths = populate_lengths_with_nulls(lengths, batch_size, &present);
        let offsets = OffsetBuffer::from_lengths(lengths);
        let null_buffer = present.map(NullBuffer::from);

        let array = ListArray::try_new(self.field.clone(), offsets, child_array, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}

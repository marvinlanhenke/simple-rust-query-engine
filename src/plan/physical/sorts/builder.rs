use arrow::compute::interleave;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::Result;

/// Represents a cursor that tracks the current position within a batch.
#[derive(Debug, Clone, Default)]
struct BatchCursor {
    /// Index of the current `RecordBatch` for a specific stream.
    batch_index: usize,
    /// Index of the current row within the `RecordBatch`.
    row_index: usize,
}

/// A builder for constructing `RecordBatch`'es incrementally.
#[derive(Debug)]
pub struct BatchBuilder {
    /// A reference-counted schema of the `RecordBatch`.
    schema: SchemaRef,
    /// A vector of tuples containing the stream index
    /// and the corresponding `RecordBatch`.
    batches: Vec<(usize, RecordBatch)>,
    /// A vector of `Cursors` tracking the current position in each stream.
    cursors: Vec<BatchCursor>,
    /// A vector of tuples containing the batch index
    /// and row index for each row to be included in the final output batch.
    indices: Vec<(usize, usize)>,
}

impl BatchBuilder {
    /// Creates a new [`BatchBuilder`] instance.
    pub fn new(schema: SchemaRef, stream_count: usize, batch_size: usize) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
        }
    }

    /// Adds a new `RecordBatch` to the builder for the specified stream index.
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) {
        let batch_index = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_index,
            row_index: 0,
        };
    }

    /// Adds a row from the specified stream to the builder.
    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_index = cursor.row_index;
        cursor.row_index += 1;
        self.indices.push((cursor.batch_index, row_index));
    }

    /// Returns the number of rows added to the builder.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Checks if the builder is empty,
    /// i.e. has no rows added to the final output.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of the `RecordBatch` instances being built.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Builds a `RecordBatch` from the added rows.
    ///
    /// This method constructs a `RecordBatch` using the rows that have been added to the builder.
    /// It first checks if there are any rows to process. If not, it returns `None`.
    /// Otherwise, it interleaves the columns from the retained batches based on the stored indices
    /// and constructs a new `RecordBatch`.
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let columns = (0..self.schema.fields().len())
            .map(|col_idx| {
                let arrays = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(col_idx).as_ref())
                    .collect::<Vec<_>>();
                Ok(interleave(&arrays, &self.indices)?)
            })
            .collect::<Result<Vec<_>>>()?;

        self.indices.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch.
        // We can therefore drop all but the last batch for each stream.
        let mut batch_index = 0;
        let mut retained = 0;
        self.batches.retain(|(stream_idx, _)| {
            let stream_cursor = &mut self.cursors[*stream_idx];
            let retain = stream_cursor.batch_index == batch_index;
            batch_index += 1;

            if retain {
                stream_cursor.batch_index = retained;
                retained += 1;
            };
            retain
        });

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
